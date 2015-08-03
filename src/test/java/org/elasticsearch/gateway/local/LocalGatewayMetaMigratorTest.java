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

package org.elasticsearch.gateway.local;

import com.google.common.collect.ImmutableMap;
import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.elasticsearch.cluster.metadata.MetaData;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.gateway.local.state.meta.LocalGatewayMetaMigrator;
import org.elasticsearch.test.ElasticsearchTestCase;
import org.elasticsearch.test.junit.annotations.TestLogging;
import org.junit.Test;

import static org.hamcrest.Matchers.*;

@TestLogging("gateway.local.state.meta:TRACE")
public class LocalGatewayMetaMigratorTest extends ElasticsearchTestCase {

    private static LocalGatewayMetaMigrator.LocalGatewayMetaDataMigration testMigration = new LocalGatewayMetaMigrator.LocalGatewayMetaDataMigration() {
        @Override
        public MetaData migrateMetaData(MetaData globalMetaData) {
            IndexMetaData indexMetaData = IndexMetaData.builder(globalMetaData.indices().get("test")).numberOfReplicas(1).build();
            return MetaData.builder(globalMetaData).version(42L).put(indexMetaData, true).build();
        }
    };

    @Test
    public void testMigration() throws Exception {
        LocalGatewayMetaMigrator migrator = new LocalGatewayMetaMigrator(ImmutableSettings.EMPTY, ImmutableMap.of("test", testMigration));

        IndexMetaData.Builder builder = IndexMetaData.builder("test").numberOfShards(1).numberOfReplicas(0).version(42L);
        MetaData globalMeta = MetaData.builder().version(1L).put(builder).build();
        MetaData migrated = migrator.migrateMetaData(globalMeta);
        assertThat(globalMeta, is(not(migrated)));
        assertThat(migrated.version(), is(42L));
        assertThat(migrated.index("test").version(), is(greaterThan(42L)));
    }

    @Test
    public void testNoMigrations() throws Exception {
        LocalGatewayMetaMigrator migrator = new LocalGatewayMetaMigrator(
                ImmutableSettings.EMPTY,
                ImmutableMap.<String, LocalGatewayMetaMigrator.LocalGatewayMetaDataMigration>of());

        IndexMetaData.Builder builder = IndexMetaData.builder("test").numberOfShards(1).numberOfReplicas(0).version(42L);
        MetaData globalMeta = MetaData.builder().version(1L).put(builder.build(), false).build();
        MetaData migrated = migrator.migrateMetaData(globalMeta);
        assertThat(globalMeta, is(migrated));
        assertThat(migrated.version(), is(1L));
        assertThat(migrated.index("test").version(), is(42L));
    }
}
