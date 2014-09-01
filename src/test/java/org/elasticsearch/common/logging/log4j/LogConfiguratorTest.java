/*
 * Licensed to CRATE Technology GmbH ("Crate") under one or more contributor
 * license agreements.  See the NOTICE file distributed with this work for
 * additional information regarding copyright ownership.  Crate licenses
 * this file to you under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.  You may
 * obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.  See the
 * License for the specific language governing permissions and limitations
 * under the License.
 *
 * However, if you have executed another commercial license agreement
 * with Crate these terms will supersede the license and you may use the
 * software solely pursuant to the terms of the relevant commercial agreement.
 */

package org.elasticsearch.common.logging.log4j;

import com.google.common.io.Files;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.env.Environment;
import org.hamcrest.Matchers;
import org.junit.Test;

import java.io.File;
import java.io.FileOutputStream;
import java.io.OutputStreamWriter;
import java.nio.charset.StandardCharsets;

import static org.hamcrest.core.Is.is;
import static org.junit.Assert.assertThat;

public class LogConfiguratorTest {


    @Test
    public void testResolveConfigValidFilename() throws Exception {
        File tempDir = Files.createTempDir();
        File tempFileYml = File.createTempFile("logging.", ".yml", tempDir);
        File tempFileYaml = File.createTempFile("logging.", ".yaml", tempDir);

        OutputStreamWriter out = new OutputStreamWriter(new FileOutputStream(tempFileYml), StandardCharsets.UTF_8);
        out.write("yml: bar");
        out.close();

        out = new OutputStreamWriter(new FileOutputStream(tempFileYaml), StandardCharsets.UTF_8);
        out.write("yaml: bar");
        out.close();

        Environment environment = new Environment(
                ImmutableSettings.builder().put("path.conf", tempDir.getAbsolutePath()).build());

        ImmutableSettings.Builder builder = ImmutableSettings.builder();
        LogConfigurator.resolveConfig(environment, builder);

        Settings logSettings = builder.build();
        assertThat(logSettings.get("yml"), is("bar"));
        assertThat(logSettings.get("yaml"), is("bar"));
    }

    @Test
    public void testResolveConfigInvalidFilename() throws Exception {
        File tempDir = Files.createTempDir();
        File tempFile = File.createTempFile("logging.", ".yml.bak", tempDir);

        OutputStreamWriter out = new OutputStreamWriter(new FileOutputStream(tempFile), StandardCharsets.UTF_8);
        out.write("yml: bar");
        out.close();

        Environment environment = new Environment(
                ImmutableSettings.builder().put("path.conf", tempDir.getAbsolutePath()).build());

        ImmutableSettings.Builder builder = ImmutableSettings.builder();
        LogConfigurator.resolveConfig(environment, builder);

        Settings logSettings = builder.build();
        assertThat(logSettings.get("yml"), Matchers.nullValue());
    }
}