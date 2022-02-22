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
package ru.datamart.prostore.query.execution.plugin.adqm.dml.service;

import ru.datamart.prostore.common.dto.QueryParserResponse;
import ru.datamart.prostore.query.execution.plugin.adqm.query.service.AdqmQueryJoinConditionsCheckService;
import ru.datamart.prostore.query.execution.plugin.api.exception.DataSourceException;
import ru.datamart.prostore.query.execution.plugin.api.service.LlrValidationService;
import org.apache.calcite.rel.RelRoot;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class AdqmValidationServiceTest {

    private final AdqmQueryJoinConditionsCheckService adqmQueryJoinConditionsCheckService = mock(AdqmQueryJoinConditionsCheckService.class);
    private final QueryParserResponse parserResponse = mock(QueryParserResponse.class);
    private final RelRoot relNode = mock(RelRoot.class);
    private final LlrValidationService validationService = new AdqmValidationService(adqmQueryJoinConditionsCheckService);

    @BeforeEach
    void setUp() {
        when(parserResponse.getRelNode()).thenReturn(relNode);
        when(parserResponse.getSchema()).thenReturn(null);
    }

    @Test
    void validateSuccess() {
        when(adqmQueryJoinConditionsCheckService.isJoinConditionsCorrect(any())).thenReturn(true);
        validationService.validate(parserResponse);
    }

    @Test
    void validateFail() {
        when(adqmQueryJoinConditionsCheckService.isJoinConditionsCorrect(any())).thenReturn(false);
        try {
            validationService.validate(parserResponse);
        } catch (Exception e) {
            assertTrue(e instanceof DataSourceException);
            assertTrue(e.getMessage().contains("Clickhouse’s global join is restricted"));
        }
    }
}
