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

import com.google.common.base.Predicate;
import com.google.common.collect.ImmutableMap;
import com.google.common.util.concurrent.ListenableFuture;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.test.ElasticsearchIntegrationTest;
import org.elasticsearch.test.InternalTestCluster;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import java.util.concurrent.CancellationException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

import static org.hamcrest.Matchers.is;

@ElasticsearchIntegrationTest.ClusterScope(scope = ElasticsearchIntegrationTest.Scope.TEST, numDataNodes = 2)
public class DeallocatorsTest extends ElasticsearchIntegrationTest{

    static {
        ClassLoader.getSystemClassLoader().setDefaultAssertionStatus(true);
        System.setProperty(TESTS_CLUSTER, ""); // ensure InternalTestCluster
    }

    @Rule
    public ExpectedException expectedException = ExpectedException.none();

    private Deallocators deallocators;

    @Before
    public void prepare() {
        deallocators = ((InternalTestCluster)cluster()).getInstance(Deallocators.class);
        // change setting
        client().admin().cluster().prepareUpdateSettings().setTransientSettings(
                ImmutableSettings.builder().put(Deallocators.GRACEFUL_STOP_MIN_AVAILABILITY, "full")
        ).execute().actionGet();
    }

    @Test
    public void testDeallocatorsDeallocate() throws Exception {

        client().admin().indices()
                .prepareCreate("t0")
                .addMapping("default", XContentFactory.jsonBuilder().startObject().startObject("properties")
                        .startObject("_id")
                        .field("type", "integer")
                        .endObject()
                        .startObject("name")
                        .field("type", "string")
                        .endObject()
                        .endObject()
                        .endObject().string())
                .setSettings(ImmutableSettings.builder().put("number_of_shards", 2).put("number_of_replicas", 0))
                .execute().actionGet();
        ensureGreen();
        client().prepareIndex("t0", "default")
                .setId(String.valueOf(randomInt()))
                .setSource(ImmutableMap.<String, Object>of("name", randomAsciiOfLength(10))).execute().actionGet();
        client().prepareIndex("t0", "default")
                .setId(String.valueOf(randomInt()))
                .setSource(ImmutableMap.<String, Object>of("name", randomAsciiOfLength(10))).execute().actionGet();
        refresh();

        ListenableFuture<Deallocator.DeallocationResult> future = deallocators.deallocate();
        Deallocator.DeallocationResult result = future.get(10, TimeUnit.SECONDS);
        assertThat(result.success(), is(true));
        assertThat(result.didDeallocate(), is(true));
        ensureGreen(); // wait for clusterstate to propagate
        assertThat(deallocators.isDeallocating(), is(true)); // node not shut down yet, still seen as deallocating
    }

    @Test
    public void testDeallocatorsDeallocateOngoing() throws Exception {

        client().admin().indices()
                .prepareCreate("t0")
                .addMapping("default", XContentFactory.jsonBuilder().startObject().startObject("properties")
                        .startObject("_id")
                        .field("type", "integer")
                        .endObject()
                        .startObject("name")
                        .field("type", "string")
                        .endObject()
                        .endObject()
                        .endObject().string())
                .setSettings(ImmutableSettings.builder().put("number_of_shards", 2).put("number_of_replicas", 1))
                .execute().actionGet();
        ensureGreen();
        client().prepareIndex("t0", "default")
                .setId(String.valueOf(randomInt()))
                .setSource(ImmutableMap.<String, Object>of("name", randomAsciiOfLength(10))).execute().actionGet();
        client().prepareIndex("t0", "default")
                .setId(String.valueOf(randomInt()))
                .setSource(ImmutableMap.<String, Object>of("name", randomAsciiOfLength(10))).execute().actionGet();
        refresh();

        ListenableFuture<Deallocator.DeallocationResult> future = deallocators.deallocate();
        assertThat(future.isDone(), is(false));
        awaitBusy(new Predicate<Object>() {
            @Override
            public boolean apply(Object input) {
                return deallocators.isDeallocating();
            }
        });

        // change setting
        client().admin().cluster().prepareUpdateSettings().setTransientSettings(
                ImmutableSettings.builder().put(Deallocators.GRACEFUL_STOP_MIN_AVAILABILITY, "none")
        ).execute().actionGet();

        // still deallocating
        assertThat(deallocators.isDeallocating(), is(true));
        assertThat(future.isDone(), is(false));

        deallocators.cancel();

        // change setting
        client().admin().cluster().prepareUpdateSettings().setTransientSettings(
                ImmutableSettings.builder().put(Deallocators.GRACEFUL_STOP_MIN_AVAILABILITY, "primaries")
        ).execute().actionGet();
        assertThat(deallocators.isDeallocating(), is(false));

        expectedException.expect(CancellationException.class);
        try {
            future.get(1, TimeUnit.SECONDS);
        } catch (ExecutionException e) {
            throw (Exception)e.getCause();
        }
    }

}
