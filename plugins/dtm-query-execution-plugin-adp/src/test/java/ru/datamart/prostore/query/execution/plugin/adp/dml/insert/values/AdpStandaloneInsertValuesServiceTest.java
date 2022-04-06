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
package ru.datamart.prostore.query.execution.plugin.adp.dml.insert.values;

import io.vertx.core.Future;
import lombok.val;
import org.apache.calcite.sql.SqlDialect;
import org.apache.calcite.sql.SqlInsert;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import ru.datamart.prostore.common.model.ddl.ColumnType;
import ru.datamart.prostore.common.model.ddl.Entity;
import ru.datamart.prostore.common.model.ddl.EntityField;
import ru.datamart.prostore.common.model.ddl.EntityType;
import ru.datamart.prostore.query.execution.plugin.adp.calcite.configuration.CalciteConfiguration;
import ru.datamart.prostore.query.execution.plugin.adp.db.service.DatabaseExecutor;
import ru.datamart.prostore.query.execution.plugin.adp.util.TestUtils;
import ru.datamart.prostore.query.execution.plugin.api.request.InsertValuesRequest;

import java.util.Arrays;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class AdpStandaloneInsertValuesServiceTest {
    private final CalciteConfiguration calciteConfiguration = new CalciteConfiguration();
    private final SqlDialect sqlDialect = calciteConfiguration.adpSqlDialect();

    @Mock
    private DatabaseExecutor executor;

    @Captor
    private ArgumentCaptor<String> executorArgCaptor;

    private AdpStandaloneInsertValuesService service;

    @BeforeEach
    void setUp() {
        service = new AdpStandaloneInsertValuesService(sqlDialect, executor);
    }

    @Test
    void shouldSuccessWhenInsertWithColumns() {
        // arrange
        when(executor.executeWithParams(anyString(), any(), any())).thenReturn(Future.succeededFuture());
        val request = getInsertRequest("INSERT INTO db.tablename(id,col1,col2) VALUES (1,2,3), (4,5,6), (7,8,9)");

        // act
        val result = service.execute(request);

        // assert
        assertTrue(result.succeeded());

        verify(executor).executeWithParams(executorArgCaptor.capture(), any(), any());
        val executorParam = executorArgCaptor.getValue();
        val expectedQuery = "INSERT INTO db.tablename (id, col1, col2)\n" +
                "VALUES  (1, 2, 3),\n" +
                " (4, 5, 6),\n" +
                " (7, 8, 9)";
        assertThat(executorParam).isEqualToIgnoringNewLines(expectedQuery);
    }

    @Test
    void shouldFailWhenColumnDoesNotExist() {
        // arrange
        when(executor.executeWithParams(anyString(), any(), any())).thenReturn(Future.failedFuture("error"));
        val request = getInsertRequest("INSERT INTO db.tablename(not_exist_col) VALUES (1)");

        // act
        val result = service.execute(request);

        // assert
        assertTrue(result.failed());

    }

    private InsertValuesRequest getInsertRequest(String sql) {
        val sqlNode = (SqlInsert) TestUtils.DEFINITION_SERVICE.processingQuery(sql);
        val entity = getEntity();
        return new InsertValuesRequest(UUID.randomUUID(), "dev", "datamart",
                1L, entity, sqlNode, null);

    }

    private Entity getEntity() {
        return Entity.builder()
                .fields(Arrays.asList(
                        EntityField.builder()
                                .name("id")
                                .primaryOrder(1)
                                .ordinalPosition(0)
                                .type(ColumnType.DOUBLE)
                                .nullable(false)
                                .build(),
                        EntityField.builder()
                                .name("col1")
                                .ordinalPosition(1)
                                .type(ColumnType.CHAR)
                                .nullable(false)
                                .build(),
                        EntityField.builder()
                                .name("col2")
                                .ordinalPosition(2)
                                .type(ColumnType.DOUBLE)
                                .nullable(true)
                                .build()
                ))
                .externalTableLocationPath("db.tablename")
                .entityType(EntityType.WRITEABLE_EXTERNAL_TABLE)
                .build();
    }
}