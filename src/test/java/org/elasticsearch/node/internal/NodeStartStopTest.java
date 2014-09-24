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


package org.elasticsearch.node.internal;

import com.carrotsearch.randomizedtesting.annotations.Repeat;
import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.util.concurrent.EsExecutors;
import org.elasticsearch.node.NodeBuilder;
import org.elasticsearch.test.ElasticsearchTestCase;
import org.junit.Test;

public class NodeStartStopTest extends ElasticsearchTestCase {

    static {
        ClassLoader.getSystemClassLoader().setDefaultAssertionStatus(true);
    }

    @Repeat(iterations = 10)
    @Test
    public void testFastStartAndStop() throws Exception {
        // assert that no exception is thrown when starting and stopping the
        // internal node in parallel etc.
        final InternalNode node = (InternalNode) NodeBuilder.nodeBuilder().local(true).data(true).settings(
                ImmutableSettings.builder()
                        .put(ClusterName.SETTING, getClass().getName())
                        .put("node.name", getClass().getName())
                        .put(IndexMetaData.SETTING_NUMBER_OF_SHARDS, 1)
                        .put(IndexMetaData.SETTING_NUMBER_OF_SHARDS, 0)
                        .put(EsExecutors.PROCESSORS, 1)
                        .put("http.enabled", true)
                        .put("index.store.type", "ram")
                        .put("config.ignore_system_properties", true)
                        .put("gateway.type", "none")).build();
        new Thread(new Runnable() {
            @Override
            public void run() {
                node.start();
            }
        }).start();
        new Thread(new Runnable() {
            @Override
            public void run() {
                node.stop();
            }
        }).start();
    }
}
