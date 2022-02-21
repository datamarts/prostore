/*
 * Copyright © 2021 ProStore
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
package ru.datamart.prostore.query.execution.core.dml.dto;

import ru.datamart.prostore.common.reader.SourceType;
import ru.datamart.prostore.query.execution.model.metadata.Datamart;
import lombok.Builder;
import lombok.Data;
import org.apache.calcite.sql.SqlNode;

import java.util.List;
import java.util.Set;

@Data
@Builder
public class PluginDeterminationRequest {
    private final SqlNode sqlNode;
    private final String query;
    private final List<Datamart> schema;
    private final SourceType preferredSourceType;
    private final Set<SourceType> cachedAcceptablePlugins;
    private final SourceType cachedMostSuitablePlugin;
}
