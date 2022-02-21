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
import lombok.extern.slf4j.Slf4j;
import lombok.val;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;

@Slf4j
class ReverseHistoryTransferRequestFactoryTest {
    private static final ReverseHistoryTransferRequest EXPECTED_RQ = ReverseHistoryTransferRequest.builder()
            .historyTableName("env1__dtm1__tbl1_history")
            .stagingTableName("env1__dtm1__tbl1_staging")
            .actualTableName("env1__dtm1__tbl1_actual")
            .eraseOperationBatchSize(300)
            .sysCn(11)
            .build();

    @Test
    void create() {
        val factory = new ReverseHistoryTransferRequestFactory(
                new AdgHelperTableNamesFactory(),
                new AdgRollbackProperties()
        );
        val request = factory.create(
                RollbackRequest.builder()
                        .sysCn(11)
                        .destinationTable("tbl1")
                        .datamartMnemonic("dtm1")
                        .envName("env1")
                        .build());
        log.info(request.toString());
        assertEquals(EXPECTED_RQ, request);
    }
}
