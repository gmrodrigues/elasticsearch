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

import com.google.common.collect.ImmutableMap;
import com.google.common.util.concurrent.ListenableFuture;
import org.elasticsearch.action.admin.cluster.health.ClusterHealthStatus;
import org.elasticsearch.cluster.routing.ShardRoutingState;
import org.elasticsearch.common.logging.Loggers;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.test.ElasticsearchIntegrationTest;
import org.elasticsearch.test.InternalTestCluster;
import org.junit.Test;

import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

import static org.hamcrest.Matchers.*;

@ElasticsearchIntegrationTest.ClusterScope(scope = ElasticsearchIntegrationTest.Scope.TEST, numDataNodes = 2)
public class PrimariesDeallocatorTest extends DeallocatorTest {

    static {
        ClassLoader.getSystemClassLoader().setDefaultAssertionStatus(true);
        Loggers.getLogger(PrimariesDeallocator.class).setLevel("TRACE");
        System.setProperty(TESTS_CLUSTER, ""); // ensure InternalTestCluster
    }

    @Test
    public void testDeallocate() throws Exception {
        createIndices();

        PrimariesDeallocator deallocator = ((InternalTestCluster)cluster()).getInstance(PrimariesDeallocator.class, takeDownNode.name());
        ListenableFuture<Deallocator.DeallocationResult> future = deallocator.deallocate();
        Deallocator.DeallocationResult result = future.get(1, TimeUnit.MINUTES);
        assertThat(result.success(), is(true));
        assertThat(result.didDeallocate(), is(true));

        ensureGreen("t0");

        assertThat(clusterService().state().routingNodes().node(takeDownNode.id()).shardsWithState("t0", ShardRoutingState.STARTED, ShardRoutingState.INITIALIZING, ShardRoutingState.RELOCATING).size(), is(0));
        ClusterHealthStatus status = client().admin().cluster().prepareHealth().setWaitForYellowStatus().setTimeout("2s").execute().actionGet().getStatus();
        assertThat(status, isOneOf(ClusterHealthStatus.GREEN, ClusterHealthStatus.YELLOW));
    }

    @Test
    public void testDeallocateMultipleZeroReplicaIndices() throws Exception {
        createIndices();
        client().admin().indices()
                .prepareCreate("t2")
                .addMapping("default", mappingSource)
                .setSettings(ImmutableSettings.builder().put("number_of_shards", 3).put("number_of_replicas", 0))
                .execute().actionGet();
        for (int i = 0; i<1000; i++) {
            client().prepareIndex("t2", "default")
                    .setId(String.valueOf(randomInt()))
                    .setSource(ImmutableMap.<String, Object>of("name", randomAsciiOfLength(10))).execute().actionGet();
        }
        refresh();

        PrimariesDeallocator deallocator = ((InternalTestCluster)cluster()).getInstance(PrimariesDeallocator.class, takeDownNode.name());
        ListenableFuture<Deallocator.DeallocationResult> future = deallocator.deallocate();
        Deallocator.DeallocationResult result = future.get(1, TimeUnit.MINUTES);
        assertThat(result.success(), is(true));
        assertThat(result.didDeallocate(), is(true));

        ensureGreen("t0");

        assertThat(clusterService().state().routingNodes().node(takeDownNode.id()).shardsWithState("t0", ShardRoutingState.STARTED, ShardRoutingState.INITIALIZING, ShardRoutingState.RELOCATING).size(), is(0));
        ClusterHealthStatus status = client().admin().cluster().prepareHealth().setWaitForYellowStatus().setTimeout("2s").execute().actionGet().getStatus();
        assertThat(status, is(isOneOf(ClusterHealthStatus.GREEN, ClusterHealthStatus.YELLOW)));
    }

    @Test
    public void testCancel() throws Exception {
        createIndices();

        PrimariesDeallocator deallocator = ((InternalTestCluster)cluster()).getInstance(PrimariesDeallocator.class, takeDownNode.name());
        assertThat(deallocator.cancel(), is(false));
        ListenableFuture<Deallocator.DeallocationResult> future = deallocator.deallocate();
        assertThat(deallocator.isDeallocating(), is(true));
        assertThat(deallocator.cancel(), is(true));
        assertThat(deallocator.isDeallocating(), is(false));

        expectedException.expect(DeallocationCancelledException.class);
        expectedException.expectMessage("Deallocation cancelled for node '" + takeDownNode.id() + "'");

        try {
            future.get(1, TimeUnit.SECONDS);
        } catch (ExecutionException e) {
            throw (Exception)e.getCause();
        }
    }

    @Test
    public void testDeallocateNoOps() throws Exception {
        PrimariesDeallocator deallocator = ((InternalTestCluster)cluster()).getInstance(PrimariesDeallocator.class, takeDownNode.name());

        // NOOP
        Deallocator.DeallocationResult result = deallocator.deallocate().get(1, TimeUnit.SECONDS);
        assertThat(result.success(), is(true));
        assertThat(result.didDeallocate(), is(false));

        createIndices();

        // DOUBLE deallocation
        deallocator.deallocate();

        expectedException.expect(IllegalStateException.class);
        expectedException.expectMessage("node already waiting for primary only deallocation");
        deallocator.deallocate();
    }

    @Test
    public void testNotOverrideExistingSettings() throws Exception {
        createIndices();
        client().admin().indices().prepareUpdateSettings("t0").setSettings(
                ImmutableSettings.builder().put(PrimariesDeallocator.EXCLUDE_NODE_ID_FROM_INDEX, "abc")
        ).execute().actionGet();

        PrimariesDeallocator deallocator = ((InternalTestCluster)cluster()).getInstance(PrimariesDeallocator.class, takeDownNode.name());
        ListenableFuture<Deallocator.DeallocationResult> future = deallocator.deallocate();
        Deallocator.DeallocationResult result = future.get(1, TimeUnit.MINUTES);
        assertThat(result.success(), is(true));
        assertThat(result.didDeallocate(), is(true));

        assertThat(deallocator.cancel(), is(true));
        ensureGreen();

        assertThat(clusterService().state().metaData().index("t0").settings().get(PrimariesDeallocator.EXCLUDE_NODE_ID_FROM_INDEX), is("abc"));

    }

}
