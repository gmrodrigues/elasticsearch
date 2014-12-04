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

package org.elasticsearch.gateway.local.state.meta;

import org.elasticsearch.cluster.metadata.MetaData;
import org.elasticsearch.common.component.AbstractComponent;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.Settings;

import java.util.Map;

public class LocalGatewayMetaMigrator extends AbstractComponent {

    /**
     * Migration to be applied when the cluster metadata is loaded from file
     */
    public static interface LocalGatewayMetaDataMigration {

        /**
         * Migrate the given metadata.
         * Return the unchanged one if no changes were applied.
         */
        public MetaData migrateMetaData(MetaData globalMetaData);
    }

    private final Map<String, LocalGatewayMetaDataMigration> migrations;

    @Inject
    public LocalGatewayMetaMigrator(Settings settings, Map<String, LocalGatewayMetaDataMigration> migrations) {
        super(settings);
        this.migrations = migrations;
    }

    public MetaData migrateMetaData(MetaData globalMetaData) {
        MetaData migrated = globalMetaData;
        logger.trace("migrating cluster metadata...");
        for (Map.Entry<String, LocalGatewayMetaDataMigration> entry : migrations.entrySet()) {
            logger.trace("executing migration '{}'", entry.getKey());
            try {
                migrated = entry.getValue().migrateMetaData(globalMetaData);
            } catch (Exception e) {
                logger.error("error during migration '{}'", e, entry.getKey());
            }
        }
        logger.trace("migration of cluster metadata done.");
        if (logger.isTraceEnabled() && globalMetaData != migrated) {
            logger.trace("cluster metadata changed during migrations.");
        }
        return migrated;
    }
}
