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

import com.google.common.base.Joiner;
import com.google.common.base.Splitter;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.action.admin.cluster.settings.ClusterUpdateSettingsRequest;
import org.elasticsearch.action.admin.cluster.settings.ClusterUpdateSettingsResponse;
import org.elasticsearch.action.admin.cluster.settings.TransportClusterUpdateSettingsAction;
import org.elasticsearch.action.admin.indices.settings.put.TransportUpdateSettingsAction;
import org.elasticsearch.action.support.TransportAction;
import org.elasticsearch.cluster.ClusterService;
import org.elasticsearch.cluster.routing.allocation.decider.EnableAllocationDecider;
import org.elasticsearch.common.component.AbstractComponent;
import org.elasticsearch.common.settings.ImmutableSettings;

import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

public abstract class AbstractDeallocator extends AbstractComponent implements Deallocator {

    static final String EXCLUDE_NODE_ID_FROM_INDEX = "index.routing.allocation.exclude._id";
    static final String CLUSTER_ROUTING_EXCLUDE_BY_NODE_ID = "cluster.routing.allocation.exclude._id";
    static final Joiner COMMA_JOINER = Joiner.on(',');
    static final Splitter COMMA_SPLITTER = Splitter.on(',');
    static final ActionListener<ClusterUpdateSettingsResponse> ALLOCATION_ENABLE_NOOP_LISTENER = new ActionListener<ClusterUpdateSettingsResponse>() {
        @Override
        public void onResponse(ClusterUpdateSettingsResponse response) {

        }

        @Override
        public void onFailure(Throwable e) {

        }
    };

    /**
     * executor with only 1 Thread, ensuring linearized execution of
     * requests changing cluster state
     */
    protected static class ClusterChangeExecutor {
        private final ThreadPoolExecutor executor;
        private final BlockingQueue<Runnable> workQueue;


        public ClusterChangeExecutor() {
            workQueue = new ArrayBlockingQueue<>(50);
            executor = new ThreadPoolExecutor(1, 1, 10, TimeUnit.SECONDS, workQueue);
            executor.prestartAllCoreThreads();
        }

        public <TRequest extends ActionRequest, TResponse extends ActionResponse> void enqueue(
                final TRequest request,
                final TransportAction<TRequest, TResponse> action,
                final ActionListener<TResponse> listener) {
            workQueue.add(new Runnable() {
                @Override
                public void run() {
                    // execute synchronously
                    try {
                        listener.onResponse(
                                action.execute(request).actionGet()
                        );
                    } catch (Exception e) {
                        listener.onFailure(e);
                    }
                }
            });
        }

        public <TRequest extends ActionRequest, TResponse extends ActionResponse> void enqueue(
                final TRequest requests[],
                final TransportAction<TRequest, TResponse> action,
                final ActionListener<TResponse> listener) {
            workQueue.add(new Runnable() {
                @Override
                public void run() {
                    for (final TRequest request : requests) {
                        // execute synchronously
                        try {
                            listener.onResponse(
                                    action.execute(request).actionGet()
                            );
                        } catch (Exception e) {
                            listener.onFailure(e);
                        }
                    }
                }
            });
        }
    }

    protected final ClusterChangeExecutor clusterChangeExecutor;
    protected final ClusterService clusterService;
    protected final TransportUpdateSettingsAction updateSettingsAction;
    protected final TransportClusterUpdateSettingsAction clusterUpdateSettingsAction;
    protected final AtomicReference<String> allocationEnableSetting = new AtomicReference<>(EnableAllocationDecider.Allocation.ALL.name());

    private String localNodeId;

    public AbstractDeallocator(ClusterService clusterService, TransportUpdateSettingsAction indicesUpdateSettingsAction, TransportClusterUpdateSettingsAction clusterUpdateSettingsAction) {
        super(ImmutableSettings.EMPTY);
        this.clusterService = clusterService;
        this.clusterChangeExecutor = new ClusterChangeExecutor();
        this.updateSettingsAction = indicesUpdateSettingsAction;
        this.clusterUpdateSettingsAction = clusterUpdateSettingsAction;
    }

    public String localNodeId() {
        if (localNodeId == null) {
            localNodeId = clusterService.localNode().id();
        }
        return localNodeId;
    }

    protected void setAllocationEnableSetting(final String value) {
        ClusterUpdateSettingsRequest request = new ClusterUpdateSettingsRequest();
        request.transientSettings(ImmutableSettings.builder().put(
                EnableAllocationDecider.CLUSTER_ROUTING_ALLOCATION_ENABLE,
                value));

        ActionListener<ClusterUpdateSettingsResponse> listener;
        if (logger.isDebugEnabled()) {
            listener = new ActionListener<ClusterUpdateSettingsResponse>() {
                @Override
                public void onResponse(ClusterUpdateSettingsResponse response) {
                    logger.trace("[{}] setting '{}' successfully set to {}", localNodeId(), EnableAllocationDecider.CLUSTER_ROUTING_ALLOCATION_ENABLE, value);
                }

                @Override
                public void onFailure(Throwable e) {
                    logger.debug("[{}] error setting '{}'", e, localNodeId(), EnableAllocationDecider.CLUSTER_ROUTING_ALLOCATION_ENABLE);
                }
            };
        } else {
            listener = ALLOCATION_ENABLE_NOOP_LISTENER;
        }
        clusterChangeExecutor.enqueue(request, clusterUpdateSettingsAction, listener);
    }
    protected void resetAllocationEnableSetting() {
        setAllocationEnableSetting(allocationEnableSetting.get());
    }
}
