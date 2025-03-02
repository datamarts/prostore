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
package ru.datamart.prostore.query.execution.plugin.adg.dml.service.insert.values;

import io.vertx.core.Future;
import lombok.val;
import org.apache.calcite.sql.SqlDialect;
import org.apache.calcite.sql.SqlInsert;
import org.assertj.core.api.Assertions;
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
import ru.datamart.prostore.query.execution.plugin.adg.base.factory.AdgHelperTableNamesFactory;
import ru.datamart.prostore.query.execution.plugin.adg.base.model.cartridge.request.AdgTransferDataEtlRequest;
import ru.datamart.prostore.query.execution.plugin.adg.base.service.client.AdgCartridgeClient;
import ru.datamart.prostore.query.execution.plugin.adg.base.service.converter.AdgPluginSpecificLiteralConverter;
import ru.datamart.prostore.query.execution.plugin.adg.calcite.configuration.AdgCalciteConfiguration;
import ru.datamart.prostore.query.execution.plugin.adg.dml.service.insert.values.AdgLogicalInsertValuesService;
import ru.datamart.prostore.query.execution.plugin.adg.query.service.AdgQueryExecutorService;
import ru.datamart.prostore.query.execution.plugin.adg.utils.TestUtils;
import ru.datamart.prostore.query.execution.plugin.api.request.InsertValuesRequest;

import java.util.Arrays;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

@ExtendWith(MockitoExtension.class)
class AdgLogicalInsertValuesServiceTest {

    private final AdgCalciteConfiguration calciteConfiguration = new AdgCalciteConfiguration();
    private final SqlDialect sqlDialect = calciteConfiguration.adgSqlDialect();
    private final AdgPluginSpecificLiteralConverter parameterConverter = new AdgPluginSpecificLiteralConverter();

    @Mock
    private AdgQueryExecutorService executor;

    @Mock
    private AdgCartridgeClient cartridgeClient;

    @Captor
    private ArgumentCaptor<String> executorArgCaptor;

    @Captor
    private ArgumentCaptor<AdgTransferDataEtlRequest> transferRequestCaptor;

    private AdgLogicalInsertValuesService service;

    @BeforeEach
    void setUp() {
        service = new AdgLogicalInsertValuesService(sqlDialect, executor, cartridgeClient, new AdgHelperTableNamesFactory(), parameterConverter);

        lenient().when(executor.executeUpdate(anyString(), any())).thenReturn(Future.succeededFuture());
        lenient().when(cartridgeClient.transferDataToScdTable(any())).thenReturn(Future.succeededFuture());
    }

    @Test
    void shouldSuccessWhenInsertWithColumns() {
        // arrange
        val request = getInsertRequest("INSERT INTO a.abc(id,col1,col2) VALUES (1,2,'2021-08-21'), (1,2,'2021-08-22'), (1,3,'2021-08-23')", ColumnType.DATE);

        // act
        val result = service.execute(request);

        // assert
        if (result.failed()) {
            fail(result.cause());
        }
        assertTrue(result.succeeded());

        verify(executor).executeUpdate(executorArgCaptor.capture(), any());
        val executedSql = executorArgCaptor.getValue();
        Assertions.assertThat(executedSql).isEqualToIgnoringNewLines("INSERT INTO \"dev__datamart__abc_staging\" (\"id\", \"col1\", \"col2\", \"sys_op\")\n" +
                "VALUES  (1, 2, 18860, 0),\n" +
                " (1, 2, 18861, 0),\n" +
                " (1, 3, 18862, 0)");

        verify(cartridgeClient).transferDataToScdTable(transferRequestCaptor.capture());
        val transferDataEtlRequest = transferRequestCaptor.getValue();
        assertEquals("dev__datamart__abc_actual", transferDataEtlRequest.getHelperTableNames().getActual());
        assertEquals("dev__datamart__abc_history", transferDataEtlRequest.getHelperTableNames().getHistory());
        assertEquals("dev__datamart__abc_staging", transferDataEtlRequest.getHelperTableNames().getStaging());
        assertEquals(request.getSysCn(), transferDataEtlRequest.getDeltaNumber());
    }

    @Test
    void shouldSuccessWhenInsertWithoutColumns() {
        // arrange
        val request = getInsertRequest("INSERT INTO a.abc VALUES (1,2,3), (1,2,3), (1,3,3)", ColumnType.INT);

        // act
        val result = service.execute(request);

        // assert
        if (result.failed()) {
            fail(result.cause());
        }
        assertTrue(result.succeeded());

        verify(executor).executeUpdate(executorArgCaptor.capture(), any());
        val executedSql = executorArgCaptor.getValue();
        Assertions.assertThat(executedSql).isEqualToIgnoringNewLines("INSERT INTO \"dev__datamart__abc_staging\" (\"id\", \"col1\", \"col2\", \"sys_op\")\n" +
                "VALUES  (1, 2, 3, 0),\n" +
                " (1, 2, 3, 0),\n" +
                " (1, 3, 3, 0)");

        verify(cartridgeClient).transferDataToScdTable(transferRequestCaptor.capture());
        val transferDataEtlRequest = transferRequestCaptor.getValue();
        assertEquals("dev__datamart__abc_actual", transferDataEtlRequest.getHelperTableNames().getActual());
        assertEquals("dev__datamart__abc_history", transferDataEtlRequest.getHelperTableNames().getHistory());
        assertEquals("dev__datamart__abc_staging", transferDataEtlRequest.getHelperTableNames().getStaging());
        assertEquals(request.getSysCn(), transferDataEtlRequest.getDeltaNumber());
    }

    @Test
    void shouldFailWhenUnknownColumn() {
        // arrange
        val request = getInsertRequest("INSERT INTO a.abc(unknown_col) VALUES (1)", ColumnType.INT);

        // act
        val result = service.execute(request);

        // assert
        if (result.succeeded()) {
            fail("UnexpectedSuccess");
        }
        assertEquals("Column [unknown_col] not exists", result.cause().getMessage());
        assertTrue(result.failed());
    }

    @Test
    void shouldFailWhenExecutorThrows() {
        // arrange
        reset(executor);
        when(executor.executeUpdate(any(), any())).thenThrow(new RuntimeException("Exception"));
        val request = getInsertRequest("INSERT INTO a.abc(id,col1,col2) VALUES (1,2,3), (1,2,3), (1,3,3)", ColumnType.INT);

        // act
        val result = service.execute(request);

        // assert
        if (result.succeeded()) {
            fail("Unexpected success");
        }
        assertTrue(result.failed());
    }

    @Test
    void shouldFailWhenExecutorFails() {
        // arrange
        reset(executor);
        when(executor.executeUpdate(any(), any())).thenReturn(Future.failedFuture("Failed"));
        val request = getInsertRequest("INSERT INTO a.abc(id,col1,col2) VALUES (1,2,3), (1,2,3), (1,3,3)", ColumnType.INT);

        // act
        val result = service.execute(request);

        // assert
        if (result.succeeded()) {
            fail("Unexpected success");
        }
        assertTrue(result.failed());
    }

    @Test
    void shouldFailWhenTransferThrows() {
        // arrange
        reset(cartridgeClient);
        when(cartridgeClient.transferDataToScdTable(any())).thenThrow(new RuntimeException("Exception"));
        val request = getInsertRequest("INSERT INTO a.abc(id,col1,col2) VALUES (1,2,3), (1,2,3), (1,3,3)", ColumnType.INT);

        // act
        val result = service.execute(request);

        // assert
        if (result.succeeded()) {
            fail("Unexpected success");
        }
        assertTrue(result.failed());
    }

    @Test
    void shouldFailWhenTransferFails() {
        // arrange
        reset(cartridgeClient);
        when(cartridgeClient.transferDataToScdTable(any())).thenReturn(Future.failedFuture("Failed"));
        val request = getInsertRequest("INSERT INTO a.abc(id,col1,col2) VALUES (1,2,3), (1,2,3), (1,3,3)", ColumnType.INT);

        // act
        val result = service.execute(request);

        // assert
        if (result.succeeded()) {
            fail("Unexpected success");
        }
        assertTrue(result.failed());
    }

    @Test
    void shouldFailWhenNotValuesSource() {
        // arrange
        val request = getInsertRequest("INSERT INTO a.abc(id,col1,col2) SELECT * FROM TBL", ColumnType.INT);
        // act
        Future<Void> result = service.execute(request);

        // assert
        if (result.succeeded()) {
            fail("Unexpected success");
        }
        assertTrue(result.failed());
    }

    private InsertValuesRequest getInsertRequest(String sql, ColumnType type) {
        val sqlNode = (SqlInsert) TestUtils.DEFINITION_SERVICE.processingQuery(sql);
        Entity entity = Entity.builder()
                .name("abc")
                .fields(Arrays.asList(
                        EntityField.builder()
                                .name("id")
                                .type(ColumnType.INT)
                                .primaryOrder(1)
                                .ordinalPosition(0)
                                .build(),
                        EntityField.builder()
                                .name("col1")
                                .type(ColumnType.INT)
                                .ordinalPosition(1)
                                .build(),
                        EntityField.builder()
                                .name("col2")
                                .type(type)
                                .ordinalPosition(2)
                                .build()
                ))
                .build();

        return new InsertValuesRequest(UUID.randomUUID(), "dev", "datamart", 1L, entity, sqlNode, null);
    }

}