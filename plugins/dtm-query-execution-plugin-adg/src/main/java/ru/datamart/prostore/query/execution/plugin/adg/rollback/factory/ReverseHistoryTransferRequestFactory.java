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
package ru.datamart.prostore.query.execution.plugin.adg.rollback.factory;

import ru.datamart.prostore.query.execution.plugin.adg.base.factory.AdgHelperTableNamesFactory;
import ru.datamart.prostore.query.execution.plugin.adg.rollback.configuration.properties.AdgRollbackProperties;
import ru.datamart.prostore.query.execution.plugin.adg.rollback.dto.ReverseHistoryTransferRequest;
import ru.datamart.prostore.query.execution.plugin.api.dto.RollbackRequest;
import lombok.RequiredArgsConstructor;
import lombok.val;
import org.springframework.stereotype.Component;

@Component
@RequiredArgsConstructor
public class ReverseHistoryTransferRequestFactory {
    private final AdgHelperTableNamesFactory helperTableNamesFactory;
    private final AdgRollbackProperties rollbackProperties;

    public ReverseHistoryTransferRequest create(RollbackRequest request) {
        val envName = request.getEnvName();
        val tableName = request.getDestinationTable();
        val datamart = request.getDatamartMnemonic();
        val helperTableNames = helperTableNamesFactory.create(envName, datamart, tableName);
        return ReverseHistoryTransferRequest.builder()
            .eraseOperationBatchSize(rollbackProperties.getEraseOperationBatchSize())
            .stagingTableName(helperTableNames.getStaging())
            .historyTableName(helperTableNames.getHistory())
            .actualTableName(helperTableNames.getActual())
            .sysCn(request.getSysCn())
            .build();
    }
}
