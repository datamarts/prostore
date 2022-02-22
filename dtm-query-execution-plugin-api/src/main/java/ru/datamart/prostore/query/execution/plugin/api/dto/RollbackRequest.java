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

import java.util.UUID;

@Getter
public class RollbackRequest extends PluginRequest {

    private final String destinationTable;
    private final long sysCn;
    private final Entity entity;

    @Builder
    public RollbackRequest(UUID requestId,
                           String envName,
                           String datamartMnemonic,
                           String destinationTable,
                           long sysCn,
                           Entity entity) {
        super(requestId, envName, datamartMnemonic);
        this.destinationTable = destinationTable;
        this.sysCn = sysCn;
        this.entity = entity;
    }
}
