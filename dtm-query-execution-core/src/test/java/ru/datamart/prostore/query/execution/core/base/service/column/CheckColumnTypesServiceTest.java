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
package ru.datamart.prostore.query.execution.core.base.service.column;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import io.vertx.core.Vertx;
import io.vertx.junit5.VertxTestContext;
import lombok.val;
import org.apache.calcite.sql.parser.SqlParser;
import org.junit.jupiter.api.Test;
import ru.datamart.prostore.common.dto.QueryParserRequest;
import ru.datamart.prostore.common.model.ddl.ColumnType;
import ru.datamart.prostore.common.model.ddl.EntityField;
import ru.datamart.prostore.query.calcite.core.service.QueryParserService;
import ru.datamart.prostore.query.execution.core.calcite.configuration.CalciteConfiguration;
import ru.datamart.prostore.query.execution.core.calcite.factory.CoreCalciteSchemaFactory;
import ru.datamart.prostore.query.execution.core.calcite.factory.CoreSchemaFactory;
import ru.datamart.prostore.query.execution.core.calcite.service.CoreCalciteContextProvider;
import ru.datamart.prostore.query.execution.core.calcite.service.CoreCalciteDMLQueryParserService;
import ru.datamart.prostore.query.execution.core.utils.TestUtils;
import ru.datamart.prostore.query.execution.model.metadata.Datamart;
import ru.datamart.prostore.serialization.CoreSerialization;

import java.util.Arrays;
import java.util.List;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.AssertionsForClassTypes.assertThat;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static ru.datamart.prostore.query.execution.core.utils.TestUtils.loadTextFromFile;

class CheckColumnTypesServiceTest {
    private final CalciteConfiguration calciteConfiguration = new CalciteConfiguration();
    private final SqlParser.Config configParser = calciteConfiguration.configEddlParser(calciteConfiguration.getSqlParserFactory());
    private final CoreCalciteSchemaFactory calciteSchemaFactory = new CoreCalciteSchemaFactory(new CoreSchemaFactory());
    private final CoreCalciteContextProvider calciteContextProvider = new CoreCalciteContextProvider(configParser, calciteSchemaFactory);
    private final QueryParserService parserService = new CoreCalciteDMLQueryParserService(calciteContextProvider, Vertx.vertx());
    private final CheckColumnTypesService service = new CheckColumnTypesService();

    @Test
    void checkColumnTypesSuccess() throws JsonProcessingException, InterruptedException {
        val testContext = new VertxTestContext();
        val sql = "select * from dml.accounts";
        val sqlNode = TestUtils.DEFINITION_SERVICE.processingQuery(sql);
        val datamarts = CoreSerialization.mapper()
                .readValue(loadTextFromFile("schema/dml_all_types.json"), new TypeReference<List<Datamart>>() {
                });
        val destColumns = Arrays.asList(
                createEntityField(0, "id", ColumnType.INT, null, 1, 1),
                createEntityField(1, "double_col", ColumnType.DOUBLE, null, null, null),
                createEntityField(2, "float_col", ColumnType.FLOAT, null, null, null),
                createEntityField(3, "varchar_col", ColumnType.VARCHAR, 36, null, null),
                createEntityField(4, "boolean_col", ColumnType.BOOLEAN, null, null, null),
                createEntityField(5, "int_col", ColumnType.INT, null, null, null),
                createEntityField(6, "bigint_col", ColumnType.BIGINT, null, null, null),
                createEntityField(9, "date_col", ColumnType.DATE, null, null, null),
                createEntityField(7, "timestamp_col", ColumnType.TIMESTAMP, 6, null, null),
                createEntityField(8, "time_col", ColumnType.TIME, 5, null, null),
                createEntityField(9, "uuid_col", ColumnType.UUID, 36, null, null),
                createEntityField(9, "char_col", ColumnType.CHAR, 10, null, null),
                createEntityField(9, "int32_col", ColumnType.INT32, null, null, null),
                createEntityField(9, "link_col", ColumnType.LINK, null, null, null)
        );

        parserService.parse(new QueryParserRequest(sqlNode, datamarts))
                .map(parserResponse -> service.check(destColumns, parserResponse.getRelNode()))
                .onComplete(ar -> {
                    if (ar.succeeded()) {
                        assertTrue(ar.result());
                        testContext.completeNow();
                    } else {
                        testContext.failNow(ar.cause());
                    }
                });
        assertThat(testContext.awaitCompletion(10, TimeUnit.SECONDS)).isTrue();
    }

    @Test
    void checkColumnTypesWithConstSuccess() throws JsonProcessingException, InterruptedException {
        val testContext = new VertxTestContext();
        val sql = "select *, 0 as sys_op from dml.accounts";
        val sqlNode = TestUtils.DEFINITION_SERVICE.processingQuery(sql);
        val datamarts = CoreSerialization.mapper()
                .readValue(loadTextFromFile("schema/dml_all_types.json"), new TypeReference<List<Datamart>>() {
                });
        val destColumns = Arrays.asList(
                createEntityField(0, "id", ColumnType.INT, null, 1, 1),
                createEntityField(1, "double_col", ColumnType.DOUBLE, null, null, null),
                createEntityField(2, "float_col", ColumnType.FLOAT, null, null, null),
                createEntityField(3, "varchar_col", ColumnType.VARCHAR, 36, null, null),
                createEntityField(4, "boolean_col", ColumnType.BOOLEAN, null, null, null),
                createEntityField(5, "int_col", ColumnType.INT, null, null, null),
                createEntityField(6, "bigint_col", ColumnType.BIGINT, null, null, null),
                createEntityField(9, "date_col", ColumnType.DATE, null, null, null),
                createEntityField(7, "timestamp_col", ColumnType.TIMESTAMP, 6, null, null),
                createEntityField(8, "time_col", ColumnType.TIME, 5, null, null),
                createEntityField(9, "uuid_col", ColumnType.UUID, 36, null, null),
                createEntityField(9, "char_col", ColumnType.CHAR, 10, null, null),
                createEntityField(9, "int32_col", ColumnType.INT32, null, null, null),
                createEntityField(9, "link_col", ColumnType.LINK, null, null, null),
                createEntityField(9, "sys_op", ColumnType.INT32, null, null, null)
        );

        parserService.parse(new QueryParserRequest(sqlNode, datamarts))
                .map(parserResponse -> service.check(destColumns, parserResponse.getRelNode()))
                .onComplete(ar -> {
                    if (ar.succeeded()) {
                        assertTrue(ar.result());
                        testContext.completeNow();
                    } else {
                        testContext.failNow(ar.cause());
                    }
                });
        assertThat(testContext.awaitCompletion(10, TimeUnit.SECONDS)).isTrue();
    }

    @Test
    void checkColumnTypesFail() throws JsonProcessingException, InterruptedException {
        val testContext = new VertxTestContext();
        val sql = "select * from dml.accounts";
        val sqlNode = TestUtils.DEFINITION_SERVICE.processingQuery(sql);
        val datamarts = CoreSerialization.mapper()
                .readValue(loadTextFromFile("schema/dml_all_types.json"), new TypeReference<List<Datamart>>() {
                });
        val destColumns = Arrays.asList(
                createEntityField(0, "id", ColumnType.INT, null, 1, 1),
                createEntityField(1, "double_col", ColumnType.DOUBLE, null, null, null),
                createEntityField(2, "float_col", ColumnType.FLOAT, null, null, null),
                createEntityField(3, "varchar_col", ColumnType.VARCHAR, 36, null, null),
                createEntityField(4, "boolean_col", ColumnType.BOOLEAN, null, null, null),
                createEntityField(5, "int_col", ColumnType.INT, null, null, null),
                createEntityField(6, "bigint_col", ColumnType.BIGINT, null, null, null),
                createEntityField(9, "date_col", ColumnType.DATE, null, null, null),
                createEntityField(7, "timestamp_col", ColumnType.TIMESTAMP, 6, null, null),
                createEntityField(8, "time_col", ColumnType.TIME, 5, null, null),
                createEntityField(9, "uuid_col", ColumnType.UUID, 36, null, null),
                createEntityField(9, "char_col", ColumnType.CHAR, 10, null, null),
                createEntityField(9, "int32_col", ColumnType.INT32, null, null, null)
        );

        parserService.parse(new QueryParserRequest(sqlNode, datamarts))
                .map(parserResponse -> service.check(destColumns, parserResponse.getRelNode()))
                .onComplete(ar -> {
                    if (ar.succeeded()) {
                        assertFalse(ar.result());
                        testContext.completeNow();
                    } else {
                        testContext.failNow(ar.cause());
                    }
                });
        assertThat(testContext.awaitCompletion(10, TimeUnit.SECONDS)).isTrue();
    }

    EntityField createEntityField(int ordinalPosition, String name, ColumnType type, Integer size,
                                  Integer primaryOrder, Integer shardingOrder) {
        return EntityField.builder()
                .ordinalPosition(ordinalPosition)
                .name(name)
                .type(type)
                .size(size)
                .accuracy(null)
                .nullable(true)
                .primaryOrder(primaryOrder)
                .shardingOrder(shardingOrder)
                .defaultValue(null)
                .build();
    }
}
