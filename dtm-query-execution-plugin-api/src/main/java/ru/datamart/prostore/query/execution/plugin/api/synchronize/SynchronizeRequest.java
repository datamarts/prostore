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
package ru.datamart.prostore.query.execution.plugin.api.synchronize;

import ru.datamart.prostore.common.delta.DeltaData;
import ru.datamart.prostore.common.model.ddl.Entity;
import ru.datamart.prostore.query.execution.model.metadata.Datamart;
import ru.datamart.prostore.query.execution.plugin.api.dto.PluginRequest;
import lombok.Getter;
import lombok.ToString;
import org.apache.calcite.sql.SqlNode;

import java.util.List;
import java.util.UUID;

@Getter
@ToString
public class SynchronizeRequest extends PluginRequest {
    private final List<Datamart> datamarts;
    private final Entity entity;
    private final SqlNode viewQuery;
    private final DeltaData deltaToBe;
    private final Long beforeDeltaCnTo;

    public SynchronizeRequest(UUID requestId,
                              String envName,
                              String datamartMnemonic,
                              List<Datamart> datamarts,
                              Entity entity,
                              SqlNode viewQuery,
                              DeltaData deltaToBe,
                              Long beforeDeltaCnTo) {
        super(requestId, envName, datamartMnemonic);
        this.datamarts = datamarts;
        this.viewQuery = viewQuery;
        this.entity = entity;
        this.deltaToBe = deltaToBe;
        this.beforeDeltaCnTo = beforeDeltaCnTo;
    }
}
