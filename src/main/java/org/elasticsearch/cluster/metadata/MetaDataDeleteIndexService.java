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

package org.elasticsearch.cluster.metadata;

import org.elasticsearch.action.support.master.MasterNodeOperationRequest;
import org.elasticsearch.cluster.ClusterService;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.TimeoutClusterStateUpdateTask;
import org.elasticsearch.cluster.action.index.NodeIndexDeletedAction;
import org.elasticsearch.cluster.block.ClusterBlocks;
import org.elasticsearch.cluster.routing.RoutingTable;
import org.elasticsearch.cluster.routing.allocation.AllocationService;
import org.elasticsearch.cluster.routing.allocation.RoutingAllocation;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.Priority;
import org.elasticsearch.common.component.AbstractComponent;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.util.concurrent.FutureUtils;
import org.elasticsearch.index.Index;
import org.elasticsearch.indices.IndexMissingException;
import org.elasticsearch.threadpool.ThreadPool;

import java.util.*;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

/**
 *
 */
public class MetaDataDeleteIndexService extends AbstractComponent {

    private final ThreadPool threadPool;

    private final ClusterService clusterService;

    private final AllocationService allocationService;

    private final NodeIndexDeletedAction nodeIndexDeletedAction;

    private final MetaDataService metaDataService;

    @Inject
    public MetaDataDeleteIndexService(Settings settings, ThreadPool threadPool, ClusterService clusterService, AllocationService allocationService,
                                      NodeIndexDeletedAction nodeIndexDeletedAction, MetaDataService metaDataService) {
        super(settings);
        this.threadPool = threadPool;
        this.clusterService = clusterService;
        this.allocationService = allocationService;
        this.nodeIndexDeletedAction = nodeIndexDeletedAction;
        this.metaDataService = metaDataService;
    }

    public void deleteIndices(final Request request, final Listener<Response> userListener) {
        // we lock here, and not within the cluster service callback since we don't want to
        // block the whole cluster state handling

        final Map<Semaphore, Collection<String>> mdLocks = metaDataService.indexMetaDataLocks(Arrays.asList(request.indices));

        List<Semaphore> acquiredLocks = new ArrayList<>(request.indices.length);
        List<String> lockedIndices = new ArrayList<>(request.indices.length);

        final List<Semaphore> notAcquired = new ArrayList<>();
        final List<String> unlockedIndices = new ArrayList<>();

        for (Map.Entry<Semaphore, Collection<String>> lockEntry : mdLocks.entrySet()) {
            if (lockEntry.getKey().tryAcquire()) {
                acquiredLocks.add(lockEntry.getKey());
                lockedIndices.addAll(lockEntry.getValue());
            } else {
                notAcquired.add(lockEntry.getKey());
                unlockedIndices.addAll(lockEntry.getValue());
            }
        }

        final MetaDataDeleteResponseListener deleteResponseListener = new MetaDataDeleteResponseListener(userListener, notAcquired.isEmpty() ? null : 2);
        // call delete with the already acquired locks
        deleteIndices(request, lockedIndices, deleteResponseListener, acquiredLocks);
        if(!notAcquired.isEmpty()) {
            threadPool.executor(ThreadPool.Names.MANAGEMENT).execute(new Runnable() {
                @Override
                public void run() {
                    List<Semaphore> acquired = new ArrayList<>(notAcquired.size());
                    for(Semaphore mdLock : notAcquired) {
                        try {
                            if (mdLock.tryAcquire(request.masterTimeout.nanos(), TimeUnit.NANOSECONDS)) {
                                acquired.add(mdLock);
                            } else {
                                // release already acquired locks
                                for (Semaphore lock : acquired) {
                                    lock.release();
                                }
                                deleteResponseListener.onFailure(new ProcessClusterEventTimeoutException(request.masterTimeout, "acquire index lock"));
                                return;
                            }
                        } catch (InterruptedException e) {
                            // release already acquired locks
                            for (Semaphore lock : acquired) {
                                lock.release();
                            }
                            deleteResponseListener.onFailure(e);
                            return;
                        }
                    }
                    deleteIndices(request, unlockedIndices, deleteResponseListener, acquired);
                }
            });
        }
    }

    private void deleteIndices(final Request request, final Collection<String> indices, final Listener<Boolean> listener, Collection<Semaphore> mdLock) {
        final DeleteIndexListener deleteListener = new DeleteIndexListener(mdLock, listener);
        clusterService.submitStateUpdateTask("delete-index [" + indices + "]", Priority.URGENT, new TimeoutClusterStateUpdateTask() {

            @Override
            public TimeValue timeout() {
                return request.masterTimeout;
            }

            @Override
            public void onFailure(String source, Throwable t) {
                deleteListener.onFailure(t);
            }

            @Override
            public ClusterState execute(final ClusterState currentState) {

                RoutingTable.Builder routingTableBuilder = RoutingTable.builder(currentState.routingTable());
                MetaData.Builder metaDataBuilder = MetaData.builder(currentState.metaData());
                ClusterBlocks.Builder clusterBlocksBuilder = ClusterBlocks.builder().blocks(currentState.blocks());

                for (final String index: indices) {
                    if (!currentState.metaData().hasConcreteIndex(index)) {
                        throw new IndexMissingException(new Index(index));
                    }

                    logger.info("[{}] deleting index", index);

                    routingTableBuilder.remove(index);
                    clusterBlocksBuilder.removeIndexBlocks(index);
                    metaDataBuilder.remove(index);
                }
                // wait for events from all nodes that it has been removed from their respective metadata...
                int count = currentState.nodes().size();
                // add the notifications that the store was deleted from *data* nodes
                count += currentState.nodes().dataNodes().size();
                final AtomicInteger counter = new AtomicInteger(count * indices.size());

                // this listener will be notified once we get back a notification based on the cluster state change below.
                final NodeIndexDeletedAction.Listener nodeIndexDeleteListener = new NodeIndexDeletedAction.Listener() {
                    @Override
                    public void onNodeIndexDeleted(String deleted, String nodeId) {
                        if (indices.contains(deleted)) {
                            if (counter.decrementAndGet() == 0) {
                                deleteListener.onResponse(true);
                                nodeIndexDeletedAction.remove(this);
                            }
                        }
                    }

                    @Override
                    public void onNodeIndexStoreDeleted(String deleted, String nodeId) {
                        if (indices.contains(deleted)) {
                            if (counter.decrementAndGet() == 0) {
                                deleteListener.onResponse(true);
                                nodeIndexDeletedAction.remove(this);
                            }
                        }
                    }
                };
                nodeIndexDeletedAction.add(nodeIndexDeleteListener);
                deleteListener.future = threadPool.schedule(request.timeout, ThreadPool.Names.SAME, new Runnable() {
                    @Override
                    public void run() {
                        deleteListener.onResponse(false);
                        nodeIndexDeletedAction.remove(nodeIndexDeleteListener);
                    }
                });

                MetaData newMetaData = metaDataBuilder.build();
                ClusterBlocks blocks = clusterBlocksBuilder.build();
                RoutingAllocation.Result routingResult = allocationService.reroute(
                        ClusterState.builder(currentState).routingTable(routingTableBuilder).metaData(newMetaData).build());
                return ClusterState.builder(currentState).routingResult(routingResult).metaData(newMetaData).blocks(blocks).build();
            }

            @Override
            public void clusterStateProcessed(String source, ClusterState oldState, ClusterState newState) {
            }
        });
    }

    class DeleteIndexListener implements Listener<Boolean> {

        private final AtomicBoolean notified = new AtomicBoolean();
        private final Collection<Semaphore> mdLocks;
        private final Listener<Boolean> resultListener;
        volatile ScheduledFuture<?> future;

        private DeleteIndexListener(Collection<Semaphore> mdLocks, Listener<Boolean> resultListener) {
            this.mdLocks = mdLocks;
            this.resultListener = resultListener;
        }

        @Override
        public void onResponse(final Boolean ack) {
            if (notified.compareAndSet(false, true)) {
                for(Semaphore mdLock : mdLocks) {
                    mdLock.release();
                }
                FutureUtils.cancel(future);
                resultListener.onResponse(ack);
            }
        }

        @Override
        public void onFailure(Throwable t) {
            if (notified.compareAndSet(false, true)) {
                for(Semaphore mdLock : mdLocks) {
                    mdLock.release();
                }
                FutureUtils.cancel(future);
                resultListener.onFailure(t);
            }
        }
    }

    private static class MetaDataDeleteResponseListener implements Listener<Boolean> {

        private @Nullable Throwable t;
        private boolean ack = true;
        private @Nullable AtomicInteger counter;
        final Listener<Response> userListener;

        public MetaDataDeleteResponseListener(Listener<Response> userListener, @Nullable Integer counter) {
            this.userListener = userListener;
            if (counter != null) {
                this.counter = new AtomicInteger(counter);
            }
        }

        @Override
        public void onResponse(Boolean ack) {
            if (!ack) {
                this.ack = false;
            }
            respondIfFinished();
        }

        @Override
        public void onFailure(Throwable t) {
            this.t = t;
            respondIfFinished();
        }

        private void respondIfFinished() {
            if(counter == null || counter.decrementAndGet() == 0) {
                if ( t != null) {
                    userListener.onFailure(t);
                } else {
                    userListener.onResponse(new Response(ack));
                }
            }
        }
    }

    public static interface Listener<T> {

        void onResponse(T response);

        void onFailure(Throwable t);
    }

    public static class Request {

        final String[] indices;

        TimeValue timeout = TimeValue.timeValueSeconds(10);
        TimeValue masterTimeout = MasterNodeOperationRequest.DEFAULT_MASTER_NODE_TIMEOUT;

        public Request(String[] indices) {
            this.indices = indices;
        }

        public Request timeout(TimeValue timeout) {
            this.timeout = timeout;
            return this;
        }

        public Request masterTimeout(TimeValue masterTimeout) {
            this.masterTimeout = masterTimeout;
            return this;
        }
    }

    public static class Response {
        private final boolean acknowledged;

        public Response(boolean acknowledged) {
            this.acknowledged = acknowledged;
        }

        public boolean acknowledged() {
            return acknowledged;
        }
    }
}
