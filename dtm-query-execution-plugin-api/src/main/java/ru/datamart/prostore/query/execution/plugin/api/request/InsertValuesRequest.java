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

import lombok.Getter;
import lombok.ToString;
import org.apache.calcite.sql.SqlInsert;
import ru.datamart.prostore.common.model.ddl.Entity;
import ru.datamart.prostore.common.reader.QueryParameters;

import java.util.UUID;

@Getter
@ToString
public class InsertValuesRequest extends LlwRequest<SqlInsert> {

    public InsertValuesRequest(UUID requestId,
                               String envName,
                               String datamartMnemonic,
                               Long sysCn,
                               Entity entity,
                               SqlInsert query,
                               QueryParameters parameters) {
        super(requestId, envName, datamartMnemonic, sysCn, entity, query, parameters);
    }

}
