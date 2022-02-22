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

import ru.datamart.prostore.common.model.ddl.ColumnType;
import ru.datamart.prostore.common.model.ddl.Entity;
import ru.datamart.prostore.common.model.ddl.EntityField;
import ru.datamart.prostore.query.execution.model.metadata.Datamart;
import ru.datamart.prostore.query.execution.plugin.adqm.base.service.converter.AdqmPluginSpecificLiteralConverter;
import ru.datamart.prostore.query.execution.plugin.adqm.calcite.configuration.CalciteConfiguration;
import ru.datamart.prostore.query.execution.plugin.adqm.ddl.configuration.properties.DdlProperties;
import ru.datamart.prostore.query.execution.plugin.adqm.factory.AdqmProcessingSqlFactory;
import ru.datamart.prostore.query.execution.plugin.adqm.query.service.AdqmQueryTemplateExtractor;
import ru.datamart.prostore.query.execution.plugin.adqm.query.service.DatabaseExecutor;
import ru.datamart.prostore.query.execution.plugin.api.request.DeleteRequest;
import io.vertx.core.Future;
import lombok.val;
import org.apache.calcite.sql.SqlDelete;
import org.apache.calcite.sql.SqlLiteral;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.sql.type.SqlTypeName;
import org.hamcrest.Matchers;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.util.Arrays;
import java.util.List;
import java.util.UUID;

import static ru.datamart.prostore.query.execution.plugin.adqm.utils.TestUtils.DEFINITION_SERVICE;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.*;

@ExtendWith(MockitoExtension.class)
class AdqmDeleteServiceTest {
    private static final String CLUSTER_NAME = "cluster";

    @Mock
    private DatabaseExecutor databaseExecutor;
    @Mock
    private DdlProperties ddlProperties;
    @Captor
    private ArgumentCaptor<String> sqlCaptor;

    private AdqmDeleteService adqmDeleteService;

    @BeforeEach
    void setUp() {
        val calciteConfiguration = new CalciteConfiguration();
        val queryTemplateExtractor = new AdqmQueryTemplateExtractor(DEFINITION_SERVICE, calciteConfiguration.adqmSqlDialect());
        val adqmCommonSqlFactory = new AdqmProcessingSqlFactory(ddlProperties, calciteConfiguration.adqmSqlDialect());
        adqmDeleteService = new AdqmDeleteService(new AdqmPluginSpecificLiteralConverter(), adqmCommonSqlFactory, databaseExecutor, queryTemplateExtractor);

        lenient().when(ddlProperties.getCluster()).thenReturn(CLUSTER_NAME);
        lenient().when(databaseExecutor.executeWithParams(anyString(), any(), any())).thenReturn(Future.succeededFuture());
        lenient().when(databaseExecutor.executeUpdate(anyString())).thenReturn(Future.succeededFuture())
                .thenReturn(Future.succeededFuture());
    }

    @Test
    void shouldSuccess() {
        // arrange
        val request = getDeleteRequest("DELETE FROM datamart.abc WHERE id > ? OR col1 = ?");

        // act
        val result = adqmDeleteService.execute(request);

        // assert
        if (result.failed()) {
            fail(result.cause());
        }

        verify(databaseExecutor).executeWithParams(sqlCaptor.capture(), any(), any());
        verify(databaseExecutor, times(2)).executeUpdate(sqlCaptor.capture());
        List<String> sqlCalls = sqlCaptor.getAllValues();
        assertThat(sqlCalls, Matchers.contains(
                Matchers.matchesPattern("INSERT INTO dev__datamart.abc_actual \\(id, col1, col2, col3, sys_from, sys_to, sys_op, sys_close_date, sign\\)  SELECT __a.id, __a.col1, __a.col2, __a.col3, __a.sys_from, 0, 1, TIMESTAMP '\\d+-\\d+-\\d+ \\d+:\\d+:\\d+', arrayJoin\\(\\[-1, 1]\\) FROM dev__datamart.abc_actual AS __a FINAL WHERE \\(__a.id > 10 OR __a.col1 = 17532\\) AND __a.sys_from <= 0 AND __a.sys_to >= 0"),
                Matchers.is("SYSTEM FLUSH DISTRIBUTED dev__datamart.abc_actual"),
                Matchers.is("OPTIMIZE TABLE dev__datamart.abc_actual_shard ON CLUSTER cluster FINAL")
        ));
        assertTrue(result.succeeded());
    }

    @Test
    void shouldSuccessWithBetween() {
        // arrange
        val request = getDeleteRequest("DELETE FROM datamart.abc WHERE id BETWEEN ? AND ? OR col1 = ?",
                Arrays.asList(SqlLiteral.createExactNumeric("9", SqlParserPos.ZERO),
                        SqlLiteral.createExactNumeric("11", SqlParserPos.ZERO),
                        SqlLiteral.createCharString("2018-01-01", SqlParserPos.ZERO)),
                Arrays.asList(SqlTypeName.INTEGER, SqlTypeName.INTEGER, SqlTypeName.DATE));

        // act
        val result = adqmDeleteService.execute(request);

        // assert
        if (result.failed()) {
            fail(result.cause());
        }

        verify(databaseExecutor).executeWithParams(sqlCaptor.capture(), any(), any());
        verify(databaseExecutor, times(2)).executeUpdate(sqlCaptor.capture());
        List<String> sqlCalls = sqlCaptor.getAllValues();
        assertThat(sqlCalls, Matchers.contains(
                Matchers.matchesPattern("INSERT INTO dev__datamart.abc_actual \\(id, col1, col2, col3, sys_from, sys_to, sys_op, sys_close_date, sign\\)  SELECT __a.id, __a.col1, __a.col2, __a.col3, __a.sys_from, 0, 1, TIMESTAMP '\\d+-\\d+-\\d+ \\d+:\\d+:\\d+', arrayJoin\\(\\[-1, 1]\\) FROM dev__datamart.abc_actual AS __a FINAL WHERE \\(__a.id >= 9 AND __a.id <= 11 OR __a.col1 = 17532\\) AND __a.sys_from <= 0 AND __a.sys_to >= 0"),
                Matchers.is("SYSTEM FLUSH DISTRIBUTED dev__datamart.abc_actual"),
                Matchers.is("OPTIMIZE TABLE dev__datamart.abc_actual_shard ON CLUSTER cluster FINAL")
        ));
        assertTrue(result.succeeded());
    }


    @Test
    void shouldSuccessWithNotBetween() {
        // arrange
        val request = getDeleteRequest("DELETE FROM datamart.abc WHERE id NOT BETWEEN ? AND ? OR col1 = ?",
                Arrays.asList(SqlLiteral.createExactNumeric("9", SqlParserPos.ZERO),
                        SqlLiteral.createExactNumeric("11", SqlParserPos.ZERO),
                        SqlLiteral.createCharString("2018-01-01", SqlParserPos.ZERO)),
                Arrays.asList(SqlTypeName.INTEGER, SqlTypeName.INTEGER, SqlTypeName.DATE));

        // act
        val result = adqmDeleteService.execute(request);

        // assert
        if (result.failed()) {
            fail(result.cause());
        }

        verify(databaseExecutor).executeWithParams(sqlCaptor.capture(), any(), any());
        verify(databaseExecutor, times(2)).executeUpdate(sqlCaptor.capture());
        List<String> sqlCalls = sqlCaptor.getAllValues();
        assertThat(sqlCalls, Matchers.contains(
                Matchers.matchesPattern("INSERT INTO dev__datamart.abc_actual \\(id, col1, col2, col3, sys_from, sys_to, sys_op, sys_close_date, sign\\)  SELECT __a.id, __a.col1, __a.col2, __a.col3, __a.sys_from, 0, 1, TIMESTAMP '\\d+-\\d+-\\d+ \\d+:\\d+:\\d+', arrayJoin\\(\\[-1, 1]\\) FROM dev__datamart.abc_actual AS __a FINAL WHERE \\(__a.id < 9 OR __a.id > 11 OR __a.col1 = 17532\\) AND __a.sys_from <= 0 AND __a.sys_to >= 0"),
                Matchers.is("SYSTEM FLUSH DISTRIBUTED dev__datamart.abc_actual"),
                Matchers.is("OPTIMIZE TABLE dev__datamart.abc_actual_shard ON CLUSTER cluster FINAL")
        ));
        assertTrue(result.succeeded());
    }

    @Test
    void shouldSuccessWhenNotNullableField() {
        // arrange

        val request = getDeleteRequest("DELETE FROM datamart.abc WHERE id > ? OR col1 = ?");
        // act
        Future<Void> result = adqmDeleteService.execute(request);

        // assert
        if (result.failed()) {
            fail(result.cause());
        }

        verify(databaseExecutor).executeWithParams(sqlCaptor.capture(), any(), any());
        verify(databaseExecutor, times(2)).executeUpdate(sqlCaptor.capture());
        List<String> sqlCalls = sqlCaptor.getAllValues();
        assertThat(sqlCalls, Matchers.contains(
                Matchers.matchesPattern("INSERT INTO dev__datamart.abc_actual \\(id, col1, col2, col3, sys_from, sys_to, sys_op, sys_close_date, sign\\)  SELECT __a.id, __a.col1, __a.col2, __a.col3, __a.sys_from, 0, 1, TIMESTAMP '\\d+-\\d+-\\d+ \\d+:\\d+:\\d+', arrayJoin\\(\\[-1, 1]\\) FROM dev__datamart.abc_actual AS __a FINAL WHERE \\(__a.id > 10 OR __a.col1 = 17532\\) AND __a.sys_from <= 0 AND __a.sys_to >= 0"),
                Matchers.is("SYSTEM FLUSH DISTRIBUTED dev__datamart.abc_actual"),
                Matchers.is("OPTIMIZE TABLE dev__datamart.abc_actual_shard ON CLUSTER cluster FINAL")
        ));
        assertTrue(result.succeeded());
    }

    @Test
    void shouldSuccessWhenNoCondition() {
        // arrange
        val request = getDeleteRequest("DELETE FROM datamart.abc");

        // act
        val result = adqmDeleteService.execute(request);

        // assert
        if (result.failed()) {
            fail(result.cause());
        }

        verify(databaseExecutor).executeWithParams(sqlCaptor.capture(), any(), any());
        verify(databaseExecutor, times(2)).executeUpdate(sqlCaptor.capture());
        List<String> sqlCalls = sqlCaptor.getAllValues();
        assertThat(sqlCalls, Matchers.contains(
                Matchers.matchesPattern("INSERT INTO dev__datamart.abc_actual \\(id, col1, col2, col3, sys_from, sys_to, sys_op, sys_close_date, sign\\)  SELECT __a.id, __a.col1, __a.col2, __a.col3, __a.sys_from, 0, 1, TIMESTAMP '\\d+-\\d+-\\d+ \\d+:\\d+:\\d+', arrayJoin\\(\\[-1, 1]\\) FROM dev__datamart.abc_actual AS __a FINAL WHERE __a.sys_from <= 0 AND __a.sys_to >= 0"),
                Matchers.is("SYSTEM FLUSH DISTRIBUTED dev__datamart.abc_actual"),
                Matchers.is("OPTIMIZE TABLE dev__datamart.abc_actual_shard ON CLUSTER cluster FINAL")
        ));
        assertTrue(result.succeeded());
    }

    @Test
    void shouldSuccessWithAlias() {
        // arrange
        val request = getDeleteRequest("DELETE FROM datamart.abc as a WHERE a.id > ? OR a.col1 = ?");

        // act
        val result = adqmDeleteService.execute(request);

        // assert
        if (result.failed()) {
            fail(result.cause());
        }
        verify(databaseExecutor).executeWithParams(sqlCaptor.capture(), any(), any());
        verify(databaseExecutor, times(2)).executeUpdate(sqlCaptor.capture());
        List<String> sqlCalls = sqlCaptor.getAllValues();
        assertThat(sqlCalls, Matchers.contains(
                Matchers.matchesPattern("INSERT INTO dev__datamart.abc_actual \\(id, col1, col2, col3, sys_from, sys_to, sys_op, sys_close_date, sign\\)  SELECT __a.id, __a.col1, __a.col2, __a.col3, __a.sys_from, 0, 1, TIMESTAMP '\\d+-\\d+-\\d+ \\d+:\\d+:\\d+', arrayJoin\\(\\[-1, 1]\\) FROM dev__datamart.abc_actual AS __a FINAL WHERE \\(__a.id > 10 OR __a.col1 = 17532\\) AND __a.sys_from <= 0 AND __a.sys_to >= 0"),
                Matchers.is("SYSTEM FLUSH DISTRIBUTED dev__datamart.abc_actual"),
                Matchers.is("OPTIMIZE TABLE dev__datamart.abc_actual_shard ON CLUSTER cluster FINAL")
        ));
        assertTrue(result.succeeded());
    }

    @Test
    void shouldFailFirstExecuteFailed() {
        // arrange
        reset(databaseExecutor);
        when(databaseExecutor.executeWithParams(anyString(), any(), any()))
                .thenReturn(Future.failedFuture(new RuntimeException("Exception")));

        val request = getDeleteRequest("DELETE FROM datamart.abc WHERE id > ? OR col1 = ?");

        // act
        val result = adqmDeleteService.execute(request);

        // assert
        if (result.succeeded()) {
            fail("Unexpected success");
        }
        assertEquals("Exception", result.cause().getMessage());
        assertTrue(result.failed());
    }

    @Test
    void shouldFailSecondExecuteFailed() {
        // arrange
        reset(databaseExecutor);
        lenient().when(databaseExecutor.executeWithParams(anyString(), any(), any())).thenReturn(Future.succeededFuture());
        when(databaseExecutor.executeUpdate(anyString()))
                .thenReturn(Future.failedFuture(new RuntimeException("Exception")));

        val request = getDeleteRequest("DELETE FROM datamart.abc WHERE id > ? OR col1 = ?");

        // act
        val result = adqmDeleteService.execute(request);

        // assert
        if (result.succeeded()) {
            fail("Unexpected success");
        }
        assertEquals("Exception", result.cause().getMessage());
        assertTrue(result.failed());
    }

    @Test
    void shouldFailThirdExecuteFailed() {
        // arrange
        reset(databaseExecutor);
        lenient().when(databaseExecutor.executeWithParams(anyString(), any(), any())).thenReturn(Future.succeededFuture());
        when(databaseExecutor.executeUpdate(anyString()))
                .thenReturn(Future.succeededFuture())
                .thenReturn(Future.failedFuture(new RuntimeException("Exception")));

        val request = getDeleteRequest("DELETE FROM datamart.abc WHERE id > ? OR col1 = ?");

        // act
        val result = adqmDeleteService.execute(request);

        // assert
        if (result.succeeded()) {
            fail("Unexpected success");
        }
        assertEquals("Exception", result.cause().getMessage());
        assertTrue(result.failed());
    }

    private DeleteRequest getDeleteRequest(String sql) {
        return getDeleteRequest(sql, getExtractedParams(), Arrays.asList(SqlTypeName.INTEGER, SqlTypeName.DATE));
    }

    private DeleteRequest getDeleteRequest(String sql, List<SqlNode> extractedParams, List<SqlTypeName> paramsTypes) {
        SqlDelete sqlNode = (SqlDelete) DEFINITION_SERVICE.processingQuery(sql);
        Entity entity = getEntity();
        Datamart datamart = Datamart.builder()
                .isDefault(true)
                .entities(Arrays.asList(entity))
                .build();

        return new DeleteRequest(UUID.randomUUID(), "dev", "datamart", entity, sqlNode, 1L, 0L, Arrays.asList(datamart), null, extractedParams, paramsTypes);
    }

    private List<SqlNode> getExtractedParams() {
        return Arrays.asList(SqlLiteral.createExactNumeric("10", SqlParserPos.ZERO),
                SqlLiteral.createCharString("2018-01-01", SqlParserPos.ZERO));
    }

    private Entity getEntity() {
        return Entity.builder()
                .name("abc")
                .fields(Arrays.asList(
                        EntityField.builder()
                                .name("id")
                                .primaryOrder(1)
                                .ordinalPosition(0)
                                .type(ColumnType.BIGINT)
                                .nullable(false)
                                .build(),
                        EntityField.builder()
                                .name("col1")
                                .ordinalPosition(1)
                                .type(ColumnType.DATE)
                                .nullable(true)
                                .build(),
                        EntityField.builder()
                                .name("col2")
                                .ordinalPosition(2)
                                .type(ColumnType.TIME)
                                .nullable(true)
                                .build(),
                        EntityField.builder()
                                .name("col3")
                                .ordinalPosition(3)
                                .type(ColumnType.TIMESTAMP)
                                .nullable(false)
                                .build()
                ))
                .build();
    }
}