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
package ru.datamart.prostore.query.execution.plugin.adb.ddl.service;

import ru.datamart.prostore.common.model.ddl.Entity;
import ru.datamart.prostore.common.plugin.sql.PreparedStatementRequest;
import ru.datamart.prostore.query.calcite.core.configuration.CalciteCoreConfiguration;
import ru.datamart.prostore.query.calcite.core.framework.DtmCalciteFramework;
import ru.datamart.prostore.query.execution.plugin.adb.calcite.configuration.CalciteConfiguration;
import ru.datamart.prostore.query.execution.plugin.adb.ddl.service.AdbTruncateHistoryService;
import ru.datamart.prostore.query.execution.plugin.adb.ddl.factory.TruncateQueryFactory;
import ru.datamart.prostore.query.execution.plugin.adb.ddl.factory.impl.TruncateQueryWithHistoryFactory;
import ru.datamart.prostore.query.execution.plugin.adb.query.service.DatabaseExecutor;
import ru.datamart.prostore.query.execution.plugin.adb.query.service.impl.AdbQueryExecutor;
import ru.datamart.prostore.query.execution.plugin.api.dto.TruncateHistoryRequest;
import ru.datamart.prostore.query.execution.plugin.api.exception.DataSourceException;
import ru.datamart.prostore.query.execution.plugin.api.service.ddl.TruncateHistoryService;
import io.vertx.core.Future;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlSelect;
import org.apache.calcite.sql.parser.SqlParseException;
import org.apache.calcite.sql.parser.SqlParser;
import org.apache.calcite.tools.FrameworkConfig;
import org.apache.calcite.tools.Planner;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.*;

public class AdbTruncateHistoryServiceTest {
    private static final String SCHEMA = "schema";
    private static final String TABLE = "table";

    private final CalciteConfiguration calciteConfiguration = new CalciteConfiguration();
    private final CalciteCoreConfiguration calciteCoreConfiguration = new CalciteCoreConfiguration();
    private final SqlParser.Config parserConfig = calciteConfiguration
            .configDdlParser(calciteCoreConfiguration.eddlParserImplFactory());
    private final DtmCalciteFramework.ConfigBuilder configBuilder = DtmCalciteFramework.newConfigBuilder();
    private final FrameworkConfig frameworkConfig = configBuilder.parserConfig(parserConfig).build();
    private final Planner planner = DtmCalciteFramework.getPlanner(frameworkConfig);
    private final TruncateQueryFactory queriesFactory = new TruncateQueryWithHistoryFactory(calciteConfiguration.adbSqlDialect());
    private final DatabaseExecutor adbQueryExecutor = mock(AdbQueryExecutor.class);
    private final TruncateHistoryService adbTruncateHistoryService = new AdbTruncateHistoryService(adbQueryExecutor, queriesFactory);

    @BeforeEach
    void setUp() {
        when(adbQueryExecutor.execute(anyString())).thenReturn(Future.succeededFuture());
        when(adbQueryExecutor.executeInTransaction(any())).thenReturn(Future.succeededFuture());
    }

    @Test
    void test() {
        List<String> expectedList = Arrays.asList(
                "DELETE FROM schema.table_actual",
                "DELETE FROM schema.table_history"
        );
        test(null, expectedList);
    }

    @Test
    void testWithConditions() {
        String conditions = "id > 2";
        List<String> expectedList = Arrays.asList(
                String.format("DELETE FROM schema.table_actual WHERE %s", conditions),
                String.format("DELETE FROM schema.table_history WHERE %s", conditions));
        test(conditions, expectedList);
    }

    @Test
    void testWithSysCn() {
        Long sysCn = 1L;
        String expected = String.format("DELETE FROM schema.table_history WHERE sys_to < %s", sysCn);
        test(sysCn, null, expected);
    }


    @Test
    void testWithConditionsAndSysCn() {
        String conditions = "id > 2";
        Long sysCn = 1L;
        String expected = String.format("DELETE FROM schema.table_history WHERE %s AND sys_to < %s", conditions, sysCn);
        test(sysCn, conditions, expected);
    }

    private void test(String conditions, List<String> list) {
        adbTruncateHistoryService.truncateHistory(createRequest(null, conditions))
                .onComplete(ar -> assertTrue(ar.succeeded()));

        verify(adbQueryExecutor).executeInTransaction(argThat(input -> input.stream()
                .map(PreparedStatementRequest::getSql)
                .collect(Collectors.toList())
                .equals(list)));
    }

    private void test(Long sysCn, String conditions, String expected) {
        adbTruncateHistoryService.truncateHistory(createRequest(sysCn, conditions));
        verify(adbQueryExecutor, times(1)).execute(expected);
    }

    private TruncateHistoryRequest createRequest(Long sysCn, String conditions) {
        Entity entity = new Entity();
        entity.setSchema(SCHEMA);
        entity.setName(TABLE);
        SqlNode sqlNode = Optional.ofNullable(conditions)
                .map(val -> {
                    try {
                        return ((SqlSelect) planner.parse(String.format("SELECT * from t WHERE %s", conditions)))
                                .getWhere();
                    } catch (SqlParseException e) {
                        throw new DataSourceException("Error", e);
                    }
                })
                .orElse(null);
        return TruncateHistoryRequest.builder()
                .sysCn(sysCn)
                .entity(entity)
                .conditions(sqlNode)
                .build();
    }
}
