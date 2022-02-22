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
package ru.datamart.prostore.query.execution.plugin.api.dto;

import ru.datamart.prostore.common.model.ddl.Entity;
import lombok.Builder;
import lombok.Getter;
import org.apache.calcite.sql.SqlNode;

import java.util.UUID;

@Getter
public class TruncateHistoryRequest extends PluginRequest {
    private final Long sysCn;
    private final Entity entity;
    private final SqlNode conditions;

    @Builder
    public TruncateHistoryRequest(UUID requestId,
                                  String envName,
                                  String datamartMnemonic,
                                  Long sysCn,
                                  Entity entity,
                                  SqlNode conditions) {
        super(requestId, envName, datamartMnemonic);
        this.sysCn = sysCn;
        this.entity = entity;
        this.conditions = conditions;
    }
}
