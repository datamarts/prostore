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
package ru.datamart.prostore.query.execution.plugin.adqm.dml.service.insertvalues;

import io.vertx.core.Future;
import lombok.val;
import org.apache.calcite.sql.SqlInsert;
import org.hamcrest.Matchers;
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
import ru.datamart.prostore.query.execution.plugin.adqm.base.service.converter.AdqmPluginSpecificLiteralConverter;
import ru.datamart.prostore.query.execution.plugin.adqm.calcite.configuration.CalciteConfiguration;
import ru.datamart.prostore.query.execution.plugin.adqm.ddl.configuration.properties.DdlProperties;
import ru.datamart.prostore.query.execution.plugin.adqm.factory.AdqmProcessingSqlFactory;
import ru.datamart.prostore.query.execution.plugin.adqm.query.service.DatabaseExecutor;
import ru.datamart.prostore.query.execution.plugin.adqm.utils.TestUtils;
import ru.datamart.prostore.query.execution.plugin.api.request.InsertValuesRequest;

import java.util.Arrays;
import java.util.UUID;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.*;

@ExtendWith(MockitoExtension.class)
class AdqmLogicalInsertValuesServiceTest {
    private static final String CLOSE_VERSIONS_PATTERN = "(?s)INSERT INTO dev__datamart.abc_actual \\(id, col1, col2, col3, col4, sys_from, sys_to, sys_op, sys_close_date, sign\\).*" +
            "SELECT id, col1, col2, col3, col4, sys_from, 0, 0, '\\d+-\\d+-\\d+ \\d+:\\d+:\\d+', arrayJoin\\(\\[-1, 1]\\).*" +
            "ROM dev__datamart.abc_actual.*" +
            "WHERE sys_from < 1 AND sys_to > 1 AND id IN \\(.*" +
            "SELECT id.*" +
            "FROM dev__datamart.abc_actual_shard.*" +
            "WHERE sys_from = 1.*" +
            "\\)";

    private static final String CLUSTER_NAME = "cluster";

    @Mock
    private DdlProperties ddlProperties;

    @Mock
    private DatabaseExecutor databaseExecutor;

    private AdqmLogicalInsertValuesService service;

    @Captor
    private ArgumentCaptor<String> sqlCaptor;

    @BeforeEach
    void setUp() {
        val calciteConfiguration = new CalciteConfiguration();
        val adqmDmlSqlFactory = new AdqmProcessingSqlFactory(ddlProperties, calciteConfiguration.adqmSqlDialect());
        service = new AdqmLogicalInsertValuesService(new AdqmPluginSpecificLiteralConverter(), adqmDmlSqlFactory, databaseExecutor);

        lenient().when(ddlProperties.getCluster()).thenReturn(CLUSTER_NAME);
        lenient().when(databaseExecutor.executeWithParams(anyString(), any(), any())).thenReturn(Future.succeededFuture());
        lenient().when(databaseExecutor.executeUpdate(anyString())).thenReturn(Future.succeededFuture());
    }

    @Test
    void shouldSuccessWhenInsertWithoutColumns() {
        // arrange
        val request = getInsertRequest("INSERT INTO datamart.abc VALUES (1,'2001-01-01','01:01:01', '2001-01-01 01:01:01', true), (2,'2002-02-02','02:02:02', '2002-02-02 02:02:02', false), (3,'2003-03-03','03:03:03', '2003-03-03 03:03:03', true)");

        // act
        val result = service.execute(request);

        // assert
        if (result.failed()) {
            fail(result.cause());
        }

        verify(databaseExecutor).executeWithParams(sqlCaptor.capture(), any(), any());
        verify(databaseExecutor, times(5)).executeUpdate(sqlCaptor.capture());
        val sqlCalls = sqlCaptor.getAllValues();
        assertThat(sqlCalls, Matchers.contains(
                Matchers.is("INSERT INTO dev__datamart.abc_actual (id, col1, col2, col3, col4, sys_from, sys_to, sys_op, sys_close_date, sign) VALUES  (1, 11323, 3661000000, 978310861000000, 1, 1, 9223372036854775807, 0, 9223372036854775807, 1),  (2, 11720, 7322000000, 1012615322000000, 0, 1, 9223372036854775807, 0, 9223372036854775807, 1),  (3, 12114, 10983000000, 1046660583000000, 1, 1, 9223372036854775807, 0, 9223372036854775807, 1)"),
                Matchers.is("SYSTEM FLUSH DISTRIBUTED dev__datamart.abc_actual"),
                Matchers.is("OPTIMIZE TABLE dev__datamart.abc_actual_shard ON CLUSTER cluster FINAL"),
                Matchers.matchesPattern(CLOSE_VERSIONS_PATTERN),
                Matchers.is("SYSTEM FLUSH DISTRIBUTED dev__datamart.abc_actual"),
                Matchers.is("OPTIMIZE TABLE dev__datamart.abc_actual_shard ON CLUSTER cluster FINAL")
        ));
        assertTrue(result.succeeded());
    }

    @Test
    void shouldSuccessWhenInsertWithColumns() {
        // arrange
        val request = getInsertRequest("INSERT INTO datamart.abc (id, col1, col2) VALUES (1,'2001-01-01','01:01:01'), (2,'2002-02-02','02:02:02'), (3,'2003-03-03','03:03:03')");

        // act
        val result = service.execute(request);

        // assert
        if (result.failed()) {
            fail(result.cause());
        }

        verify(databaseExecutor).executeWithParams(sqlCaptor.capture(), any(), any());
        verify(databaseExecutor, times(5)).executeUpdate(sqlCaptor.capture());
        val sqlCalls = sqlCaptor.getAllValues();
        assertThat(sqlCalls, Matchers.contains(
                Matchers.is("INSERT INTO dev__datamart.abc_actual (id, col1, col2, sys_from, sys_to, sys_op, sys_close_date, sign) VALUES  (1, 11323, 3661000000, 1, 9223372036854775807, 0, 9223372036854775807, 1),  (2, 11720, 7322000000, 1, 9223372036854775807, 0, 9223372036854775807, 1),  (3, 12114, 10983000000, 1, 9223372036854775807, 0, 9223372036854775807, 1)"),
                Matchers.is("SYSTEM FLUSH DISTRIBUTED dev__datamart.abc_actual"),
                Matchers.is("OPTIMIZE TABLE dev__datamart.abc_actual_shard ON CLUSTER cluster FINAL"),
                Matchers.matchesPattern(CLOSE_VERSIONS_PATTERN),
                Matchers.is("SYSTEM FLUSH DISTRIBUTED dev__datamart.abc_actual"),
                Matchers.is("OPTIMIZE TABLE dev__datamart.abc_actual_shard ON CLUSTER cluster FINAL")
        ));
        assertTrue(result.succeeded());
    }

    @Test
    void shouldFailWhenValuesSizeNotEqualToColumnsSize() {
        // arrange
        val request = getInsertRequest("INSERT INTO datamart.abc (id) VALUES (1,'2001-01-01','01:01:01', true)");

        // act
        val result = service.execute(request);

        // assert
        if (result.succeeded()) {
            fail("Unexpected success");
        }

        assertEquals("Values size: [4] not equal to columns size: [1]", result.cause().getMessage());
        assertTrue(result.failed());
    }

    @Test
    void shouldFailWhenUnknownField() {
        // arrange
        val sqlNode = (SqlInsert) TestUtils.DEFINITION_SERVICE.processingQuery("INSERT INTO datamart.abc (col_unknown) VALUES ((1))");
        val entity = Entity.builder()
                .name("abc")
                .fields(Arrays.asList(
                        EntityField.builder()
                                .name("id")
                                .primaryOrder(1)
                                .ordinalPosition(0)
                                .type(ColumnType.BIGINT)
                                .build(),
                        EntityField.builder()
                                .name("col1")
                                .ordinalPosition(1)
                                .type(ColumnType.DATE)
                                .build(),
                        EntityField.builder()
                                .name("col2")
                                .ordinalPosition(2)
                                .type(ColumnType.TIME)
                                .build(),
                        EntityField.builder()
                                .name("col3")
                                .ordinalPosition(3)
                                .type(ColumnType.TIMESTAMP)
                                .build()
                ))
                .build();

        val request = new InsertValuesRequest(UUID.randomUUID(), "dev", "datamart", 1L, entity, sqlNode, null);

        // act
        val result = service.execute(request);

        // assert
        if (result.succeeded()) {
            fail("Unexpected success");
        }
        assertEquals("Column [col_unknown] not exists", result.cause().getMessage());
        assertTrue(result.failed());
    }

    @Test
    void shouldFailWhenNoPkFieldInColumns() {
        // arrange
        val sqlNode = (SqlInsert) TestUtils.DEFINITION_SERVICE.processingQuery("INSERT INTO datamart.abc (col1) VALUES ('2001-01-01','01:01:01')");
        val entity = Entity.builder()
                .name("abc")
                .fields(Arrays.asList(
                        EntityField.builder()
                                .name("id")
                                .primaryOrder(1)
                                .ordinalPosition(0)
                                .type(ColumnType.BIGINT)
                                .build(),
                        EntityField.builder()
                                .name("col1")
                                .ordinalPosition(1)
                                .type(ColumnType.DATE)
                                .build(),
                        EntityField.builder()
                                .name("col2")
                                .ordinalPosition(2)
                                .type(ColumnType.TIME)
                                .build(),
                        EntityField.builder()
                                .name("col3")
                                .ordinalPosition(3)
                                .type(ColumnType.TIMESTAMP)
                                .build()
                ))
                .build();

        InsertValuesRequest request = new InsertValuesRequest(UUID.randomUUID(), "dev", "datamart", 1L, entity, sqlNode, null);

        // act
        val result = service.execute(request);

        // assert
        if (result.succeeded()) {
            fail("Unexpected success");
        }
        assertEquals("Inserted values must contain primary keys: [id]", result.cause().getMessage());
        assertTrue(result.failed());
    }

    @Test
    void shouldFailWhenFirstDatabaseExecuteFailed() {
        // arrange
        reset(databaseExecutor);
        when(databaseExecutor.executeWithParams(anyString(), any(), any())).thenReturn(Future.failedFuture(new RuntimeException("Exception")));

        val sqlNode = (SqlInsert) TestUtils.DEFINITION_SERVICE.processingQuery("INSERT INTO datamart.abc VALUES (1,'2001-01-01','01:01:01', '2001-01-01 01:01:01')");
        val entity = Entity.builder()
                .name("abc")
                .fields(Arrays.asList(
                        EntityField.builder()
                                .name("id")
                                .primaryOrder(1)
                                .ordinalPosition(0)
                                .type(ColumnType.BIGINT)
                                .build(),
                        EntityField.builder()
                                .name("col1")
                                .ordinalPosition(1)
                                .type(ColumnType.DATE)
                                .build(),
                        EntityField.builder()
                                .name("col2")
                                .ordinalPosition(2)
                                .type(ColumnType.TIME)
                                .build(),
                        EntityField.builder()
                                .name("col3")
                                .ordinalPosition(3)
                                .type(ColumnType.TIMESTAMP)
                                .build()
                ))
                .build();

        val request = new InsertValuesRequest(UUID.randomUUID(), "dev", "datamart", 1L, entity, sqlNode, null);

        // act
        val result = service.execute(request);

        // assert
        if (result.succeeded()) {
            fail("Unexpected success");
        }

        assertEquals("Exception", result.cause().getMessage());
        assertTrue(result.failed());
    }

    @Test
    void shouldFailWhenSecondDatabaseExecuteFailed() {
        // arrange
        reset(databaseExecutor);
        when(databaseExecutor.executeWithParams(anyString(), any(), any())).thenReturn(Future.succeededFuture());
        when(databaseExecutor.executeUpdate(anyString())).thenReturn(Future.failedFuture(new RuntimeException("Exception")));

        val sqlNode = (SqlInsert) TestUtils.DEFINITION_SERVICE.processingQuery("INSERT INTO datamart.abc VALUES (1,'2001-01-01','01:01:01', '2001-01-01 01:01:01')");
        val entity = Entity.builder()
                .name("abc")
                .fields(Arrays.asList(
                        EntityField.builder()
                                .name("id")
                                .primaryOrder(1)
                                .ordinalPosition(0)
                                .type(ColumnType.BIGINT)
                                .build(),
                        EntityField.builder()
                                .name("col1")
                                .ordinalPosition(1)
                                .type(ColumnType.DATE)
                                .build(),
                        EntityField.builder()
                                .name("col2")
                                .ordinalPosition(2)
                                .type(ColumnType.TIME)
                                .build(),
                        EntityField.builder()
                                .name("col3")
                                .ordinalPosition(3)
                                .type(ColumnType.TIMESTAMP)
                                .build()
                ))
                .build();

        val request = new InsertValuesRequest(UUID.randomUUID(), "dev", "datamart", 1L, entity, sqlNode, null);

        // act
        val result = service.execute(request);

        // assert
        if (result.succeeded()) {
            fail("Unexpected success");
        }

        assertEquals("Exception", result.cause().getMessage());
        assertTrue(result.failed());
    }

    @Test
    void shouldFailWhenThirdDatabaseExecuteFailed() {
        // arrange
        reset(databaseExecutor);
        when(databaseExecutor.executeWithParams(anyString(), any(), any())).thenReturn(Future.succeededFuture());
        when(databaseExecutor.executeUpdate(anyString()))
                .thenReturn(Future.succeededFuture())
                .thenReturn(Future.failedFuture(new RuntimeException("Exception")));

        val sqlNode = (SqlInsert) TestUtils.DEFINITION_SERVICE.processingQuery("INSERT INTO datamart.abc VALUES (1,'2001-01-01','01:01:01', '2001-01-01 01:01:01')");
        val entity = Entity.builder()
                .name("abc")
                .fields(Arrays.asList(
                        EntityField.builder()
                                .name("id")
                                .primaryOrder(1)
                                .ordinalPosition(0)
                                .type(ColumnType.BIGINT)
                                .build(),
                        EntityField.builder()
                                .name("col1")
                                .ordinalPosition(1)
                                .type(ColumnType.DATE)
                                .build(),
                        EntityField.builder()
                                .name("col2")
                                .ordinalPosition(2)
                                .type(ColumnType.TIME)
                                .build(),
                        EntityField.builder()
                                .name("col3")
                                .ordinalPosition(3)
                                .type(ColumnType.TIMESTAMP)
                                .build()
                ))
                .build();

        val request = new InsertValuesRequest(UUID.randomUUID(), "dev", "datamart", 1L, entity, sqlNode, null);

        // act
        val result = service.execute(request);

        // assert
        if (result.succeeded()) {
            fail("Unexpected success");
        }

        assertEquals("Exception", result.cause().getMessage());
        assertTrue(result.failed());
    }

    private InsertValuesRequest getInsertRequest(String sql) {
        val sqlNode = (SqlInsert) TestUtils.DEFINITION_SERVICE.processingQuery(sql);
        val entity = prepareEntity();

        return new InsertValuesRequest(UUID.randomUUID(), "dev", "datamart", 1L, entity, sqlNode, null);
    }

    private Entity prepareEntity() {
        return Entity.builder()
                .name("abc")
                .fields(Arrays.asList(
                        EntityField.builder()
                                .name("id")
                                .primaryOrder(1)
                                .ordinalPosition(0)
                                .type(ColumnType.BIGINT)
                                .build(),
                        EntityField.builder()
                                .name("col1")
                                .ordinalPosition(1)
                                .type(ColumnType.DATE)
                                .build(),
                        EntityField.builder()
                                .name("col2")
                                .ordinalPosition(2)
                                .type(ColumnType.TIME)
                                .build(),
                        EntityField.builder()
                                .name("col3")
                                .ordinalPosition(3)
                                .type(ColumnType.TIMESTAMP)
                                .build(),
                        EntityField.builder()
                                .name("col4")
                                .ordinalPosition(4)
                                .type(ColumnType.BOOLEAN)
                                .build()
                ))
                .build();
    }
}
