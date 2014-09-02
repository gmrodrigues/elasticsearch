/*
 * Licensed to CRATE Technology GmbH ("Crate") under one or more contributor
 * license agreements.  See the NOTICE file distributed with this work for
 * additional information regarding copyright ownership.  Crate licenses
 * this file to you under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.  You may
 * obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.  See the
 * License for the specific language governing permissions and limitations
 * under the License.
 *
 * However, if you have executed another commercial license agreement
 * with Crate these terms will supersede the license and you may use the
 * software solely pursuant to the terms of the relevant commercial agreement.
 */

package org.elasticsearch.cluster.routing.allocation.deallocator;

import com.carrotsearch.hppc.cursors.ObjectObjectCursor;
import com.google.common.base.Joiner;
import com.google.common.base.Objects;
import com.google.common.base.Splitter;
import com.google.common.collect.Sets;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.SettableFuture;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.admin.indices.settings.put.TransportUpdateSettingsAction;
import org.elasticsearch.action.admin.indices.settings.put.UpdateSettingsRequest;
import org.elasticsearch.action.admin.indices.settings.put.UpdateSettingsResponse;
import org.elasticsearch.cluster.ClusterChangedEvent;
import org.elasticsearch.cluster.ClusterService;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.ClusterStateListener;
import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.elasticsearch.cluster.metadata.MetaData;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.routing.RoutingNode;
import org.elasticsearch.cluster.routing.ShardRoutingState;
import org.elasticsearch.common.component.AbstractComponent;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.settings.Settings;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

/**
 * This deallocator only deallocates primary shards that have no replica.
 * Other primary shards are not moved as their replicas can take over.
 *
 * Internally it excludes the local node for shard deallocation for every index with currently 0 replicas
 * that has shards on this node.
 * This will move the primary shards on this node to another but leaves everything else as is.
 */
public class PrimariesDeallocator extends AbstractComponent implements Deallocator, ClusterStateListener {

    static final String EXCLUDE_NODE_ID_FROM_INDEX = "index.routing.allocation.exclude._id";
    static final Joiner COMMA_JOINER = Joiner.on(',');
    static final Splitter COMMA_SPLITTER = Splitter.on(',');

    private final ClusterService clusterService;
    private final TransportUpdateSettingsAction indicesUpdateSettingsAction;
    private String localNodeId;

    private final Object localNodeFutureLock = new Object();
    private volatile SettableFuture<DeallocationResult> localNodeFuture;

    private final Object deallocatingIndicesLock = new Object();
    private volatile Map<String, Set<String>> deallocatingIndices;

    @Inject
    public PrimariesDeallocator(ClusterService clusterService,
                                TransportUpdateSettingsAction indicesUpdateSettingsAction) {
        super(ImmutableSettings.EMPTY);
        this.clusterService = clusterService;
        this.indicesUpdateSettingsAction = indicesUpdateSettingsAction;
        this.deallocatingIndices = new ConcurrentHashMap<>();
        this.clusterService.add(this);
    }

    public String localNodeId() {
        if (localNodeId == null) {
            localNodeId = clusterService.localNode().id();
        }
        return localNodeId;
    }

    private Set<String> localZeroReplicaIndices(RoutingNode routingNode, MetaData clusterMetaData) {
        final Set<String> zeroReplicaIndices = new HashSet<>();
        for (ObjectObjectCursor<String, IndexMetaData> entry : clusterMetaData.indices()) {
            if (entry.value.numberOfReplicas() == 0) {
                if (!routingNode.shardsWithState(entry.key, ShardRoutingState.INITIALIZING, ShardRoutingState.STARTED, ShardRoutingState.RELOCATING).isEmpty()) {
                    zeroReplicaIndices.add(entry.key);
                }
            }
        }
        return zeroReplicaIndices;
    }

    @Override
    public ListenableFuture<DeallocationResult> deallocate() {
        if (isDeallocating()) {
            throw new IllegalStateException("node already waiting for primary only deallocation");
        }
        ClusterState state = clusterService.state();
        final RoutingNode node = state.routingNodes().node(localNodeId());
        if (node.size() == 0) {
            return Futures.immediateFuture(DeallocationResult.SUCCESS_NOTHING_HAPPENED);
        }

        final Set<String> zeroReplicaIndices = localZeroReplicaIndices(node, state.metaData());
        if (zeroReplicaIndices.isEmpty()) {
            // no zero replica primaries on node
            return Futures.immediateFuture(DeallocationResult.SUCCESS_NOTHING_HAPPENED);
        }

        UpdateSettingsRequest[] settingsRequests = new UpdateSettingsRequest[zeroReplicaIndices.size()];
        synchronized (deallocatingIndicesLock) {
            int i = 0;
            for (String index : zeroReplicaIndices) {
                Set<String> excludeNodes = deallocatingIndices.get(index);
                if (excludeNodes == null) {
                    excludeNodes = new HashSet<>();
                    deallocatingIndices.put(index, excludeNodes);
                }
                excludeNodes.add(localNodeId());
                settingsRequests[i++] = new UpdateSettingsRequest(ImmutableSettings.builder().put(EXCLUDE_NODE_ID_FROM_INDEX, COMMA_JOINER.join(excludeNodes)).build(), index);
            }

        }
        synchronized (localNodeFutureLock) {
            localNodeFuture = SettableFuture.create();
        }
        for (final UpdateSettingsRequest request : settingsRequests) {
            indicesUpdateSettingsAction.execute(request, new ActionListener<UpdateSettingsResponse>() {
                @Override
                public void onResponse(UpdateSettingsResponse updateSettingsResponse) {
                    logger.trace("successfully updated index settings");
                    // do nothing
                }

                @Override
                public void onFailure(Throwable e) {
                    logger.error("error updating index settings", e);
                    cancelWithExceptionIfPresent(e);
                }
            });

        }

        return localNodeFuture;
    }

    private boolean cancelWithExceptionIfPresent(Throwable e) {
        boolean result = false;
        synchronized (localNodeFutureLock) {
            SettableFuture<DeallocationResult> future = localNodeFuture;
            if (future != null) {
                future.setException(e);
                localNodeFuture = null;
                result = true;
            }
        }
        return result;
    }

    @Override
    public boolean cancel() {
        boolean cancelled = removeExclusion(localNodeId());
        cancelled |= cancelWithExceptionIfPresent(new DeallocationCancelledException(localNodeId()));
        if (cancelled) {
            logger.debug("[{}] deallocation cancelled", localNodeId());
        } else {
            logger.debug("[{}] node not deallocating", localNodeId());
        }
        return cancelled;
    }

    private boolean removeExclusion(final String nodeId) {
        synchronized (deallocatingIndicesLock) {
            Set<String> changed = new HashSet<>();
            for (Map.Entry<String, Set<String>> entry : deallocatingIndices.entrySet()) {
                Set<String> excludeNodes = entry.getValue();
                if (excludeNodes.remove(nodeId)) {
                    changed.add(entry.getKey());
                }
                if (excludeNodes.isEmpty()) {
                    deallocatingIndices.remove(entry.getKey());
                }
            }
            if (!changed.isEmpty()) {
                for (final String index : changed) {
                    Settings settings = ImmutableSettings.builder().put(EXCLUDE_NODE_ID_FROM_INDEX,
                            COMMA_JOINER.join(Objects.firstNonNull(deallocatingIndices.get(index), Collections.EMPTY_SET))).build();
                    UpdateSettingsRequest request = new UpdateSettingsRequest(settings, index);
                    indicesUpdateSettingsAction.execute(request, new ActionListener<UpdateSettingsResponse>() {
                        @Override
                        public void onResponse(UpdateSettingsResponse updateSettingsResponse) {
                            logger.trace("[{}] updated settings for index '{}'", nodeId, index);
                        }

                        @Override
                        public void onFailure(Throwable e) {
                            logger.error("[{}] error removing exclusion for node {} on index '{}'", e, nodeId, nodeId, index);
                        }
                    });
                }
                return true;
            }
        }
        return false;
    }

    @Override
    public boolean isDeallocating() {
        return localNodeFuture != null || localNodeIsExcluded();
    }

    /**
     * This deallocator can always deallocate
     */
    @Override
    public boolean canDeallocate() {
        return true;
    }

    /**
     * @return true if this node has no primary shards with 0 replicas
     * or no shards at all
     */
    @Override
    public boolean isNoOp() {
        ClusterState state = clusterService.state();
        RoutingNode node = state.routingNodes().node(localNodeId());
        return node.size() == 0 || localZeroReplicaIndices(node, state.metaData()).isEmpty();
    }

    private boolean localNodeIsExcluded() {
        synchronized (deallocatingIndicesLock) {
            for (Map.Entry<String, Set<String>> entry : deallocatingIndices.entrySet()) {
                Set<String> excludeNodes = entry.getValue();
                if (excludeNodes != null && excludeNodes.contains(localNodeId())) {
                    return true;
                }
            }
        }
        return false;
    }

    @Override
    public void clusterChanged(ClusterChangedEvent event) {
        if (event.metaDataChanged()) {
            synchronized (deallocatingIndicesLock) {
                // update deallocating nodes from new cluster state
                for (ObjectObjectCursor<String, IndexMetaData> entry : event.state().metaData().indices()) {
                    String excludeNodesSetting = entry.value.settings().get(EXCLUDE_NODE_ID_FROM_INDEX);
                    if (excludeNodesSetting != null) {
                        List<String> excludeNodes = COMMA_SPLITTER.splitToList(excludeNodesSetting);
                        if (!excludeNodes.isEmpty()) {
                            deallocatingIndices.put(entry.key, Sets.newHashSet(excludeNodes));
                        }
                    }
                }
                if (logger.isTraceEnabled()) {
                    logger.trace("new deallocating indices: {}", COMMA_JOINER.withKeyValueSeparator(":").join(deallocatingIndices));
                }
            }
        }

        // remove removed nodes from deallocatingNodes list if we are master
        if (event.nodesRemoved() && event.state().nodes().localNodeMaster()) {
            for (DiscoveryNode node : event.nodesDelta().removedNodes()) {
                if (removeExclusion(node.id())) {
                    logger.trace("[{}] removed removed node {}", localNodeId(), node.id());
                }
            }
        }
        synchronized (localNodeFutureLock) {
            if (localNodeFuture != null) {
                RoutingNode node = event.state().getRoutingNodes().node(localNodeId());
                Set<String> localZeroReplicaIndices = localZeroReplicaIndices(
                        node,
                        event.state().metaData());
                if (localZeroReplicaIndices.isEmpty()) {
                    logger.trace("[{}] successfully moved zero replica indices", localNodeId());
                    localNodeFuture.set(DeallocationResult.SUCCESS);
                    localNodeFuture = null;
                } else {
                    logger.trace("[{}] zero replica primaries left for indices: {}", localNodeId(), COMMA_JOINER.join(localZeroReplicaIndices));
                }
            }
        }
    }
}
