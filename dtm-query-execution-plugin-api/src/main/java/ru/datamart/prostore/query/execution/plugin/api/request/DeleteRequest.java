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

import ru.datamart.prostore.common.model.ddl.Entity;
import ru.datamart.prostore.common.reader.QueryParameters;
import ru.datamart.prostore.query.execution.model.metadata.Datamart;
import lombok.Getter;
import org.apache.calcite.sql.SqlDelete;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.type.SqlTypeName;

import java.util.List;
import java.util.UUID;

@Getter
public class DeleteRequest extends LlwRequest<SqlDelete> {

    private final Long deltaOkSysCn;
    private final List<Datamart> datamarts;
    private final List<SqlNode> extractedParams;
    private final List<SqlTypeName> parametersTypes;

    public DeleteRequest(UUID requestId,
                         String envName,
                         String datamartMnemonic,
                         Entity entity,
                         SqlDelete query,
                         Long sysCn,
                         Long deltaOkSysCn,
                         List<Datamart> datamarts,
                         QueryParameters parameters,
                         List<SqlNode> extractedParams, List<SqlTypeName> parameterTypes) {
        super(requestId, envName, datamartMnemonic, sysCn, entity, query, parameters);
        this.deltaOkSysCn = deltaOkSysCn;
        this.datamarts = datamarts;
        this.extractedParams = extractedParams;
        this.parametersTypes = parameterTypes;
    }
}
