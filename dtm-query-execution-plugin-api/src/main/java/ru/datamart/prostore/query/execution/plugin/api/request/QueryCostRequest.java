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
package ru.datamart.prostore.query.execution.plugin.api.request;

import ru.datamart.prostore.query.execution.model.metadata.Datamart;
import ru.datamart.prostore.query.execution.plugin.api.dto.PluginRequest;

import java.util.List;
import java.util.UUID;

public class QueryCostRequest extends PluginRequest {
    private final List<Datamart> schema;

    public QueryCostRequest(UUID requestId,
                            String envName,
                            String datamart,
                            List<Datamart> schema) {
        super(requestId, envName, datamart);
        this.schema = schema;
    }

    public List<Datamart> getSchema() {
        return schema;
    }

}
