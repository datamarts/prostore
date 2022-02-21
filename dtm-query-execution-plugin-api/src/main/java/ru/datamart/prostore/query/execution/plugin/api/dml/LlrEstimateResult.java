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
package ru.datamart.prostore.query.execution.plugin.api.dml;

import com.fasterxml.jackson.databind.JsonNode;
import ru.datamart.prostore.common.reader.SourceType;
import lombok.Data;

@Data
public class LlrEstimateResult {
    private final SourceType plugin;
    private final JsonNode estimation;
    private final String query;
}
