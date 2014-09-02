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
import org.elasticsearch.cluster.ClusterService;
import org.elasticsearch.common.logging.Loggers;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.test.ElasticsearchIntegrationTest;
import org.elasticsearch.test.InternalTestCluster;
import org.junit.Test;

import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import static org.hamcrest.Matchers.is;

@ElasticsearchIntegrationTest.ClusterScope(scope = ElasticsearchIntegrationTest.Scope.TEST, numDataNodes = 3)
public class AllShardsDeallocatorTest extends DeallocatorTest {

    static {
        ClassLoader.getSystemClassLoader().setDefaultAssertionStatus(true);
        Loggers.getLogger(AllShardsDeallocator.class).setLevel("TRACE");
        System.setProperty(TESTS_CLUSTER, ""); // ensure InternalTestCluster
    }

    @Test
    public void testDeallocate() throws Exception {
        createIndices();

        AllShardsDeallocator allShardsDeallocator = ((InternalTestCluster)cluster()).getInstance(AllShardsDeallocator.class, takeDownNode.name());
        ListenableFuture<Deallocator.DeallocationResult> future = allShardsDeallocator.deallocate();
        Deallocator.DeallocationResult result = future.get(1, TimeUnit.MINUTES);
        assertThat(result.success(), is(true));
        assertThat(result.didDeallocate(), is(true));


        assertThat(
                ((InternalTestCluster)cluster()).getInstance(ClusterService.class, takeDownNode.name()).state().routingNodes().node(takeDownNode.id()).size(),
                is(0));
        ClusterHealthStatus status = client().admin().cluster().prepareHealth().setWaitForGreenStatus().setTimeout("2s").execute().actionGet().getStatus();
        assertThat(status, is(ClusterHealthStatus.GREEN));
    }

    @Test
    public void testDeallocateFailCannotMoveShards() throws Exception {
        client().admin().indices()
                .prepareCreate("t2")
                .addMapping("default", mappingSource)
                .setSettings(ImmutableSettings.builder().put("number_of_shards", 2).put("number_of_replicas", 2))
                .execute().actionGet();
        ensureGreen();

        for (int i = 0; i < 4; i++) {
            client().prepareIndex("t2", "default")
                    .setId(String.valueOf(randomInt()))
                    .setSource(ImmutableMap.<String, Object>of("name", randomAsciiOfLength(10))).execute().actionGet();

        }
        refresh();

        AllShardsDeallocator allShardsDeallocator = ((InternalTestCluster)cluster()).getInstance(AllShardsDeallocator.class, takeDownNode.name());
        ListenableFuture<Deallocator.DeallocationResult> future = allShardsDeallocator.deallocate();
        try {
            future.get(2, TimeUnit.SECONDS);
            fail("no TimeoutException occurred");
        } catch (TimeoutException e) {
            assertThat(clusterService().state().routingNodes().node(takeDownNode.id()).size(), is(2));
        }
    }

    @Test
    public void testCancel() throws Exception {
        createIndices();

        AllShardsDeallocator allShardsDeallocator = ((InternalTestCluster)cluster()).getInstance(AllShardsDeallocator.class, takeDownNode.name());
        assertThat(allShardsDeallocator.cancel(), is(false));
        ListenableFuture<Deallocator.DeallocationResult> future = allShardsDeallocator.deallocate();
        assertThat(allShardsDeallocator.isDeallocating(), is(true));
        assertThat(allShardsDeallocator.cancel(), is(true));
        assertThat(allShardsDeallocator.isDeallocating(), is(false));

        expectedException.expect(DeallocationCancelledException.class);
        expectedException.expectMessage("Deallocation cancelled for node '" + takeDownNode.id() + "'");

        try {
            future.get(1, TimeUnit.SECONDS);
        } catch (ExecutionException e) {
            throw (Exception)e.getCause();
        }
    }

    @Test
    public void testDeallocationNoOps() throws Exception {
        AllShardsDeallocator allShardsDeallocator = ((InternalTestCluster)cluster()).getInstance(AllShardsDeallocator.class, takeDownNode.name());

        // NOOP
        Deallocator.DeallocationResult result = allShardsDeallocator.deallocate().get(1, TimeUnit.SECONDS);
        assertThat(result.success(), is(true));
        assertThat(result.didDeallocate(), is(false));

        createIndices();

        // DOUBLE deallocation
        allShardsDeallocator.deallocate();

        expectedException.expect(IllegalStateException.class);
        expectedException.expectMessage("node already waiting for complete deallocation");
        allShardsDeallocator.deallocate();
    }
}
