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

import com.google.common.util.concurrent.ListenableFuture;
import org.elasticsearch.cluster.routing.allocation.deallocator.Deallocator;
import org.elasticsearch.cluster.routing.allocation.deallocator.Deallocators;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.logging.ESLogger;
import org.elasticsearch.common.logging.Loggers;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.node.settings.NodeSettingsService;

import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;

public class GracefulStop {

    private final Deallocators deallocators;
    private AtomicBoolean gracefulStop = new AtomicBoolean(false);
    private final ESLogger logger = Loggers.getLogger(getClass());
    private ListenableFuture<Deallocator.DeallocationResult> deallocateFuture;

    private static class SettingNames {
        private static final String IS_DEFAULT = "cluster.graceful_stop.is_default";
    }

    @Inject
    public GracefulStop(Settings settings,
                        NodeSettingsService nodeSettingsService,
                        Deallocators deallocators) {
        this.deallocators = deallocators;
        gracefulStop.set(settings.getAsBoolean(SettingNames.IS_DEFAULT, false));

        nodeSettingsService.addListener(new NodeSettingsService.Listener() {
            @Override
            public void onRefreshSettings(Settings settings) {
                gracefulStop.set(settings.getAsBoolean(SettingNames.IS_DEFAULT, false));
            }
        });
    }

    public boolean isDefault() {
        return gracefulStop.get();
    }

    public void deallocate() {
        deallocateFuture = deallocators.deallocate();
        try {
            deallocateFuture.get(10, TimeUnit.MINUTES);
        } catch (InterruptedException | ExecutionException | TimeoutException e) {
            logger.error("error while de-allocating node", e);
        }
    }

    public void cancelDeAllocationIfRunning() {
        if (deallocators.isDeallocating()) {
            deallocators.cancel();
        }
        if (deallocateFuture != null) {
            deallocateFuture.cancel(true);
            deallocateFuture = null;
        }
    }
}
