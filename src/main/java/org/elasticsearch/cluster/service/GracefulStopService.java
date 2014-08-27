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

package org.elasticsearch.cluster.service;

import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.common.component.AbstractLifecycleComponent;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.node.settings.NodeSettingsService;

import java.util.concurrent.atomic.AtomicBoolean;

public class GracefulStopService extends AbstractLifecycleComponent<GracefulStopService> {

    private AtomicBoolean gracefulStopDefaultDefault = new AtomicBoolean(false);
    private AtomicBoolean gracefulStop = new AtomicBoolean(false);

    @Inject
    public GracefulStopService(Settings settings, NodeSettingsService nodeSettingsService) {
        super(settings);
        nodeSettingsService.addListener(new NodeSettingsService.Listener() {
            @Override
            public void onRefreshSettings(Settings settings) {
                gracefulStop.set(settings.getAsBoolean("cluster.graceful_stop.default",
                        gracefulStopDefaultDefault.get()));
            }
        });
    }

    public boolean gracefulStop() {
        return gracefulStop.get();
    }

    public void gracefulStop(boolean value) {
        gracefulStopDefaultDefault.set(value);
        gracefulStop.set(value);
    }

    @Override
    protected void doStart() throws ElasticsearchException {

    }

    @Override
    protected void doStop() throws ElasticsearchException {

    }

    @Override
    protected void doClose() throws ElasticsearchException {

    }
}
