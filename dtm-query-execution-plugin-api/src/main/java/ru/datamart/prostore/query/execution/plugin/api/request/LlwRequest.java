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
package ru.datamart.prostore.query.execution.plugin.api.request;

import ru.datamart.prostore.common.model.ddl.Entity;
import ru.datamart.prostore.common.reader.QueryParameters;
import ru.datamart.prostore.query.execution.plugin.api.dto.PluginRequest;
import lombok.Getter;
import org.apache.calcite.sql.SqlNode;

import java.util.UUID;

@Getter
public class LlwRequest<N extends SqlNode> extends PluginRequest {

    private final Long sysCn;
    private final Entity entity;
    private final N query;
    private final QueryParameters parameters;

    public LlwRequest(UUID requestId,
                      String envName,
                      String datamartMnemonic,
                      Long sysCn,
                      Entity entity,
                      N query,
                      QueryParameters parameters) {
        super(requestId, envName, datamartMnemonic);
        this.sysCn = sysCn;
        this.entity = entity;
        this.query = query;
        this.parameters = parameters;
    }
}
