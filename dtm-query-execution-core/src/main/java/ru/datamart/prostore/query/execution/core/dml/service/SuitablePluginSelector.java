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

import ru.datamart.prostore.common.dml.SelectCategory;
import ru.datamart.prostore.common.dml.ShardingCategory;
import ru.datamart.prostore.common.exception.DtmException;
import ru.datamart.prostore.common.reader.SourceType;
import ru.datamart.prostore.query.execution.core.plugin.configuration.properties.PluginSelectCategoryProperties;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.util.List;
import java.util.Set;

@Component
@Slf4j
public class SuitablePluginSelector {

    private final PluginSelectCategoryProperties pluginSelectCategoryProperties;

    @Autowired
    public SuitablePluginSelector(PluginSelectCategoryProperties pluginSelectCategoryProperties) {
        this.pluginSelectCategoryProperties = pluginSelectCategoryProperties;
    }

    public SourceType selectByCategory(SelectCategory category, ShardingCategory shardingCategory, Set<SourceType> acceptablePlugins) {
        List<SourceType> prioritySourceTypes;
        if (pluginSelectCategoryProperties.getAutoSelect() != null && pluginSelectCategoryProperties.getAutoSelect().get(category) != null) {
            prioritySourceTypes = pluginSelectCategoryProperties.getAutoSelect().get(category).get(shardingCategory);
        } else {
            prioritySourceTypes = pluginSelectCategoryProperties.getMapping().get(category);
        }

        if (prioritySourceTypes != null) {
            for (SourceType sourceType : prioritySourceTypes) {
                if (acceptablePlugins.contains(sourceType)) {
                    log.info("Most suitable plugin for category [{}], sharding category [{}]: {}", category, shardingCategory, sourceType);
                    return sourceType;
                }
            }
        }
        log.error("Can't defined suitable plugin for category [{}], sharding category [{}]", category, shardingCategory);
        throw new DtmException("Suitable plugin for the query does not exist.");
    }
}
