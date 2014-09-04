/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.elasticsearch.cluster.routing.allocation.deallocator;

import com.carrotsearch.hppc.cursors.ObjectObjectCursor;
import com.google.common.base.Joiner;
import com.google.common.collect.Sets;
import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.SettableFuture;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.admin.cluster.settings.ClusterUpdateSettingsRequest;
import org.elasticsearch.action.admin.cluster.settings.ClusterUpdateSettingsResponse;
import org.elasticsearch.action.admin.cluster.settings.TransportClusterUpdateSettingsAction;
import org.elasticsearch.cluster.ClusterChangedEvent;
import org.elasticsearch.cluster.ClusterService;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.ClusterStateListener;
import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.routing.RoutingNode;
import org.elasticsearch.cluster.routing.ShardRoutingState;
import org.elasticsearch.cluster.routing.allocation.decider.EnableAllocationDecider;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.logging.ESLogger;
import org.elasticsearch.common.logging.Loggers;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.settings.Settings;

import java.util.Set;
import java.util.concurrent.atomic.AtomicReference;

public class AllShardsDeallocator implements Deallocator, ClusterStateListener {

    public static final String CLUSTER_ROUTING_EXCLUDE_BY_NODE_ID = "cluster.routing.allocation.exclude._id";

    static final Joiner COMMA_JOINER = Joiner.on(',');

    private final ESLogger logger = Loggers.getLogger(getClass());
    private final TransportClusterUpdateSettingsAction clusterUpdateSettingsAction;
    private final ClusterService clusterService;

    private final Object futureLock = new Object();
    private final Object excludeNodesLock = new Object();

    private String localNodeId;
    private volatile Set<String> deallocatingNodes;
    private volatile SettableFuture<DeallocationResult> waitForFullDeallocation = null;

    private AtomicReference<String> allocationEnableSetting = new AtomicReference<>(EnableAllocationDecider.Allocation.ALL.name());

    @Inject
    public AllShardsDeallocator(ClusterService clusterService, TransportClusterUpdateSettingsAction clusterUpdateSettingsAction) {
        this.clusterService = clusterService;
        this.clusterUpdateSettingsAction = clusterUpdateSettingsAction;
        this.deallocatingNodes = Sets.newConcurrentHashSet();
        this.clusterService.add(this);
    }

    public String localNodeId() {
        if (localNodeId == null) {
            localNodeId = clusterService.localNode().id();
        }
        return localNodeId;
    }

    private void setAllocationEnableSetting(final String value, boolean async) {
        ClusterUpdateSettingsRequest request = new ClusterUpdateSettingsRequest();
        request.transientSettings(ImmutableSettings.builder().put(
                EnableAllocationDecider.CLUSTER_ROUTING_ALLOCATION_ENABLE,
                value));

        if (async) {
            clusterUpdateSettingsAction.execute(request, new ActionListener<ClusterUpdateSettingsResponse>() {
                @Override
                public void onResponse(ClusterUpdateSettingsResponse response) {
                    logger.trace("[{}] setting '{}' successfully set to {}", localNodeId(), EnableAllocationDecider.CLUSTER_ROUTING_ALLOCATION_ENABLE, value);
                }

                @Override
                public void onFailure(Throwable e) {
                    logger.debug("[{}] error setting '{}'", e, localNodeId(), EnableAllocationDecider.CLUSTER_ROUTING_ALLOCATION_ENABLE);
                }
            });
        } else {
            clusterUpdateSettingsAction.execute(request).actionGet();
        }
    }

     private void resetAllocationEnableSetting() {
         setAllocationEnableSetting(allocationEnableSetting.get(), true); // avoid deadlock
     }

    /**
     * @see Deallocator
     *
     * @return a future that is set when the node is fully decommissioned
     */
    @Override
    public ListenableFuture<DeallocationResult> deallocate() {
        RoutingNode node = clusterService.state().routingNodes().node(localNodeId());
        if (isDeallocating()) {
            throw new IllegalStateException("node already waiting for complete deallocation");
        }
        logger.debug("[{}] deallocating node ...", localNodeId());

        if (node.size() == 0) {
            return Futures.immediateFuture(DeallocationResult.SUCCESS_NOTHING_HAPPENED);
        }

        // enable all allocation to make sure shards are moved, keep the old value
        allocationEnableSetting.set(
                clusterService.state().metaData().settings().get(
                        EnableAllocationDecider.CLUSTER_ROUTING_ALLOCATION_ENABLE,
                        EnableAllocationDecider.Allocation.ALL.name()));
        setAllocationEnableSetting(EnableAllocationDecider.Allocation.ALL.name(), false);

        final SettableFuture<DeallocationResult> future = waitForFullDeallocation = SettableFuture.create();
        Futures.addCallback(future, new FutureCallback<DeallocationResult>() {
            @Override
            public void onSuccess(DeallocationResult result) {
                resetAllocationEnableSetting();
            }

            @Override
            public void onFailure(Throwable throwable) {
                resetAllocationEnableSetting();
            }
        });
        ClusterUpdateSettingsRequest request = new ClusterUpdateSettingsRequest();
        synchronized (excludeNodesLock) {
            deallocatingNodes.add(localNodeId());
            request.transientSettings(ImmutableSettings.builder()
                    .put(CLUSTER_ROUTING_EXCLUDE_BY_NODE_ID, COMMA_JOINER.join(deallocatingNodes))
                    .build());
        }
        clusterUpdateSettingsAction.execute(request, new ActionListener<ClusterUpdateSettingsResponse>() {
            @Override
            public void onResponse(ClusterUpdateSettingsResponse response) {
                logExcludedNodes(response.getTransientSettings());
                // future will be set when node has no shards
            }

            @Override
            public void onFailure(Throwable e) {
                logger.error("[{}] error disabling allocation", e, localNodeId());
                cancelWithExceptionIfPresent(e);
            }
        });


        return future;
    }

    private void cancelWithExceptionIfPresent(Throwable e) {
        synchronized (futureLock) {
            SettableFuture<DeallocationResult> future = waitForFullDeallocation;
            if (future != null) {
                future.setException(e);
                waitForFullDeallocation = null;
            }
        }
    }

    @Override
    public boolean cancel() {
        boolean cancelled = removeExclusion(localNodeId());
        cancelWithExceptionIfPresent(new DeallocationCancelledException(localNodeId()));
        if (cancelled) {
            logger.debug("[{}] deallocation cancelled", localNodeId());
        } else {
            logger.debug("[{}] node not deallocating", localNodeId());
        }
        return cancelled;
    }

    @Override
    public boolean isDeallocating() {
        return waitForFullDeallocation != null || deallocatingNodes.contains(localNodeId());
    }

    /**
     * can deallocate if:
     * we have one spare node which does not contain a replica or primary
     * of any index which has shards on this node,
     * so we can move the shards on this node to it.
     *
     * More technically: number of data nodes > (maximum number_of_replicas of indices with shards on this node + 1)
     */
    @Override
    public boolean canDeallocate() {
        ClusterState clusterState = clusterService.state();
        int numNodes = clusterState.nodes().dataNodes().size();
        int maxReplicas = -1;
        RoutingNode localNode = clusterState.routingNodes().node(localNodeId());
        for (ObjectObjectCursor<String, IndexMetaData> entry : clusterState.metaData().indices()) {
            if (!localNode.shardsWithState(entry.key, ShardRoutingState.STARTED, ShardRoutingState.INITIALIZING, ShardRoutingState.RELOCATING).isEmpty()) {
                maxReplicas = Math.max(maxReplicas, entry.value.numberOfReplicas());
            }
        }
        return numNodes > maxReplicas+1;
    }

    /**
     * @return true if this node has no shards
     */
    @Override
    public boolean isNoOp() {
        ClusterState state = clusterService.state();
        RoutingNode node = state.routingNodes().node(localNodeId());
        return node.size() == 0;
    }

    /**
     * <ul>
     *     <li>remove exclusion for previously excluded nodeId that has been removed from the cluster
     *     <li>wait for excluded nodes to have all their shards moved
     * </ul>
     */
    @Override
    public void clusterChanged(ClusterChangedEvent event) {
        // apply new settings
        if (event.metaDataChanged()) {
            Settings settings = event.state().metaData().settings();
            synchronized (excludeNodesLock) {
                deallocatingNodes = Strings.splitStringByCommaToSet(settings.get(CLUSTER_ROUTING_EXCLUDE_BY_NODE_ID, ""));
            }
        }

        // remove removed nodes from deallocatingNodes list if we are master
        if (event.state().nodes().localNodeMaster()) {
            for (DiscoveryNode node : event.nodesDelta().removedNodes()) {
                if (removeExclusion(node.id())) {
                    logger.trace("[{}] removed removed node {}", localNodeId(), node.id());
                }
            }
        }

        // set future for fully deallocated local node
        synchronized (futureLock) {
            SettableFuture<DeallocationResult> future = waitForFullDeallocation;
            if (future != null) {
                RoutingNode node = event.state().routingNodes().node(localNodeId());
                if (node.numberOfShardsWithState(ShardRoutingState.STARTED, ShardRoutingState.INITIALIZING, ShardRoutingState.RELOCATING) == 0) {
                    logger.debug("[{}] deallocation successful.", localNodeId());
                    waitForFullDeallocation = null;
                    future.set(DeallocationResult.SUCCESS);
                } else if (logger.isTraceEnabled()) {
                    logger.trace("[{}] still {} started, {} initializing and {} relocating shards remaining",
                            localNodeId(),
                            node.numberOfShardsWithState(ShardRoutingState.STARTED),
                            node.numberOfShardsWithState(ShardRoutingState.INITIALIZING),
                            node.numberOfShardsWithState(ShardRoutingState.RELOCATING));
                }
            }
        }
    }

    /**
     * asynchronously remove exclusion for a node with id <code>nodeId</code> if it exists
     * @return true if the exclusion existed and will be removed
     */
    private boolean removeExclusion(final String nodeId) {
        synchronized (excludeNodesLock) {
            boolean removed = deallocatingNodes.remove(nodeId);
            if (removed) {
                ClusterUpdateSettingsRequest request = new ClusterUpdateSettingsRequest();
                request.transientSettings(ImmutableSettings.builder()
                        .put(CLUSTER_ROUTING_EXCLUDE_BY_NODE_ID, COMMA_JOINER.join(deallocatingNodes))
                        .build());
                clusterUpdateSettingsAction.execute(request, new ActionListener<ClusterUpdateSettingsResponse>() {
                    @Override
                    public void onResponse(ClusterUpdateSettingsResponse response) {
                        logExcludedNodes(response.getTransientSettings());
                    }

                    @Override
                    public void onFailure(Throwable e) {
                        logger.error("[{}] error removing node '{}' from exclusion list", localNodeId(), nodeId, e);
                    }
                });
            }
            return removed;
        }
    }

    private void logExcludedNodes(Settings transientSettings) {
        logger.debug("[{}] excluded nodes now set to: {}", localNodeId(), transientSettings.get(CLUSTER_ROUTING_EXCLUDE_BY_NODE_ID));
    }
}
