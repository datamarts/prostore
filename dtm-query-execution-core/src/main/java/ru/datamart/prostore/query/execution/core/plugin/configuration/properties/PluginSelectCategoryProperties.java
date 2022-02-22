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
package ru.datamart.prostore.query.execution.core.plugin.configuration.properties;

import ru.datamart.prostore.common.dml.SelectCategory;
import ru.datamart.prostore.common.dml.ShardingCategory;
import ru.datamart.prostore.common.reader.SourceType;
import lombok.Data;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.stereotype.Component;

import java.util.List;
import java.util.Map;

@Component
@Data
@ConfigurationProperties("core.plugins.category")
public class PluginSelectCategoryProperties {
    private Map<SelectCategory, List<SourceType>> mapping;
    private Map<SelectCategory, Map<ShardingCategory, List<SourceType>>> autoSelect;
}
