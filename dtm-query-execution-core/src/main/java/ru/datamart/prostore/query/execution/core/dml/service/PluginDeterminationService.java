/*
 * Copyright © 2022 DATAMART LLC
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package ru.datamart.prostore.query.execution.core.dml.service;

import ru.datamart.prostore.common.reader.SourceType;
import ru.datamart.prostore.query.execution.core.dml.dto.PluginDeterminationRequest;
import ru.datamart.prostore.query.execution.core.dml.dto.PluginDeterminationResult;
import ru.datamart.prostore.query.execution.core.plugin.configuration.properties.ActivePluginsProperties;
import ru.datamart.prostore.query.execution.core.query.exception.QueriedEntityIsMissingException;
import io.vertx.core.Future;
import lombok.extern.slf4j.Slf4j;
import lombok.val;
import lombok.var;
import org.springframework.stereotype.Service;

import java.util.Set;

@Slf4j
@Service
public class PluginDeterminationService {
    private final ActivePluginsProperties activePluginsProperties;
    private final SuitablePluginSelector suitablePluginSelector;
    private final SelectCategoryQualifier selectCategoryQualifier;
    private final ShardingCategoryQualifier shardingCategoryQualifier;
    private final AcceptableSourceTypesDefinitionService acceptableSourceTypesDefinitionService;

    public PluginDeterminationService(ActivePluginsProperties activePluginsProperties,
                                      SuitablePluginSelector suitablePluginSelector,
                                      SelectCategoryQualifier selectCategoryQualifier,
                                      ShardingCategoryQualifier shardingCategoryQualifier,
                                      AcceptableSourceTypesDefinitionService acceptableSourceTypesDefinitionService) {
        this.activePluginsProperties = activePluginsProperties;
        this.suitablePluginSelector = suitablePluginSelector;
        this.selectCategoryQualifier = selectCategoryQualifier;
        this.shardingCategoryQualifier = shardingCategoryQualifier;
        this.acceptableSourceTypesDefinitionService = acceptableSourceTypesDefinitionService;
    }

    public Future<PluginDeterminationResult> determine(PluginDeterminationRequest request) {
        if (activePluginsProperties.getActive().size() == 1) {
            val plugin = activePluginsProperties.getActive().iterator().next();
            if (request.getPreferredSourceType() != null && !plugin.equals(request.getPreferredSourceType())) {
                return Future.failedFuture(new QueriedEntityIsMissingException(request.getPreferredSourceType()));
            }
            return Future.succeededFuture(new PluginDeterminationResult(activePluginsProperties.getActive(), plugin, plugin));
        }

        return getAcceptablePlugins(request)
                .map(acceptablePlugins -> {

                    var mostSuitablePlugin = request.getCachedMostSuitablePlugin();
                    if (mostSuitablePlugin == null) {
                        val category = selectCategoryQualifier.qualify(request.getSchema(), request.getSqlNode());
                        log.debug("Defined category [{}] for sql query [{}]", category, request.getQuery());

                        val shardingCategory = shardingCategoryQualifier.qualify(request.getSchema(), request.getSqlNode());
                        log.debug("Defined sharding category [{}] for sql query [{}]", shardingCategory, request.getQuery());

                        mostSuitablePlugin = suitablePluginSelector.selectByCategory(category, shardingCategory, acceptablePlugins);
                    }

                    var executionPlugin = request.getPreferredSourceType();
                    if (executionPlugin != null && !acceptablePlugins.contains(executionPlugin)) {
                        throw new QueriedEntityIsMissingException(executionPlugin);
                    } else if (executionPlugin == null) {
                        executionPlugin = mostSuitablePlugin;
                    }

                    return new PluginDeterminationResult(acceptablePlugins, mostSuitablePlugin, executionPlugin);
                });
    }

    private Future<Set<SourceType>> getAcceptablePlugins(PluginDeterminationRequest request) {
        if (request.getCachedAcceptablePlugins() != null && !request.getCachedAcceptablePlugins().isEmpty()) {
            return Future.succeededFuture(request.getCachedAcceptablePlugins());
        }

        return acceptableSourceTypesDefinitionService.define(request.getSchema());
    }

}
