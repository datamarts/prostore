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
package ru.datamart.prostore.query.execution.plugin.adb.ddl.factory.impl;

import ru.datamart.prostore.common.model.ddl.Entity;
import ru.datamart.prostore.query.execution.plugin.adb.calcite.configuration.CalciteConfiguration;
import ru.datamart.prostore.query.execution.plugin.api.dto.TruncateHistoryRequest;
import lombok.val;
import org.apache.calcite.sql.SqlDialect;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.Collections;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;

class TruncateQueryWithoutHistoryFactoryTest {
    private final SqlDialect sqlDialect = new CalciteConfiguration().adbSqlDialect();
    private final TruncateQueryWithoutHistoryFactory truncateQueryWithoutHistoryFactory = new TruncateQueryWithoutHistoryFactory(sqlDialect);

    private static final String SCHEMA = "datamart";
    private static final String ENTITY_NAME = "test_entity";

    private static final List<String> EXPECTED = Collections.singletonList("DELETE FROM datamart.test_entity_actual");
    private static final String EXPECTED_SYS_CN = "DELETE FROM datamart.test_entity_actual WHERE sys_to < 2";
    private TruncateHistoryRequest truncateHistoryRequest;

    @BeforeEach
    void setUp() {
        val entity = Entity.builder()
                .schema(SCHEMA)
                .name(ENTITY_NAME)
                .build();
        truncateHistoryRequest = TruncateHistoryRequest.builder()
                .sysCn(2L)
                .conditions(null)
                .entity(entity)
                .build();
    }

    @Test
    void shouldGenerateDeleteAllRecordsQuery() {
        assertThat(EXPECTED).isEqualTo(truncateQueryWithoutHistoryFactory.create(truncateHistoryRequest));
    }

    @Test
    void shouldGenerateDeleteAllRecordsWithSysCnQuery() {
        assertEquals(EXPECTED_SYS_CN, truncateQueryWithoutHistoryFactory.createWithSysCn(truncateHistoryRequest));
    }

}