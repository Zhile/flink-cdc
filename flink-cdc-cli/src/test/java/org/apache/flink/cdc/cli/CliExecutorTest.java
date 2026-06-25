/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.cdc.cli;

import org.apache.flink.cdc.cli.parser.YamlPipelineDefinitionParser;
import org.apache.flink.cdc.common.configuration.Configuration;
import org.apache.flink.cdc.composer.definition.PipelineDef;
import org.apache.flink.core.fs.Path;

import org.apache.flink.shaded.guava31.com.google.common.io.Resources;

import org.junit.jupiter.api.Test;

import java.net.URL;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Tests covering how {@link CliExecutor#main(String[])} (the application-mode entrypoint) loads the
 * pipeline definition.
 *
 * <p>In application mode the deployment executors set {@code
 * ApplicationConfiguration.APPLICATION_ARGS = commandLine.getArgList()}, so {@code args[0]} is the
 * pipeline definition FILE PATH. It must be parsed via the {@link
 * YamlPipelineDefinitionParser#parse(Path, Configuration)} overload (read the file). Using the
 * {@link YamlPipelineDefinitionParser#parse(String, Configuration)} overload would treat the path
 * string itself as YAML content and fail with {@code Missing required field "source"}.
 */
class CliExecutorTest {

    /** The fixed behavior: parse the pipeline definition from a file path (Path overload). */
    @Test
    void testParsePipelineDefinitionFromFilePath() throws Exception {
        URL resource = Resources.getResource("definitions/pipeline-definition-minimized.yaml");
        Path pipelineDefPath = new Path(resource.toURI());
        PipelineDef pipelineDef =
                new YamlPipelineDefinitionParser().parse(pipelineDefPath, new Configuration());
        assertThat(pipelineDef).isNotNull();
        assertThat(pipelineDef.getSource()).isNotNull();
    }

    /**
     * Reproduces the application-mode bug: passing the path STRING to the content overload makes
     * the parser treat the path as YAML content, yielding a scalar node without a {@code source}.
     */
    @Test
    void testParsingFilePathAsYamlContentFails() {
        String pipelineDefPath = "/opt/flink/config/pipeline.yaml";
        assertThatThrownBy(
                        () ->
                                new YamlPipelineDefinitionParser()
                                        .parse(pipelineDefPath, new Configuration()))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("Missing required field \"source\"");
    }
}
