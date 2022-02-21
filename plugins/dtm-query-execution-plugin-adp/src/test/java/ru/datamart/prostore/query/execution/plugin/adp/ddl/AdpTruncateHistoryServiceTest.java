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
package ru.datamart.prostore.query.execution.plugin.adp.ddl;

import ru.datamart.prostore.common.model.ddl.Entity;
import ru.datamart.prostore.common.plugin.sql.PreparedStatementRequest;
import ru.datamart.prostore.query.calcite.core.configuration.CalciteCoreConfiguration;
import ru.datamart.prostore.query.calcite.core.framework.DtmCalciteFramework;
import ru.datamart.prostore.query.execution.plugin.adp.calcite.configuration.CalciteConfiguration;
import ru.datamart.prostore.query.execution.plugin.adp.db.service.DatabaseExecutor;
import ru.datamart.prostore.query.execution.plugin.adp.ddl.factory.TruncateHistoryFactory;
import ru.datamart.prostore.query.execution.plugin.adp.ddl.service.AdpTruncateHistoryService;
import ru.datamart.prostore.query.execution.plugin.api.dto.TruncateHistoryRequest;
import ru.datamart.prostore.query.execution.plugin.api.exception.DataSourceException;
import io.vertx.core.Future;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlSelect;
import org.apache.calcite.sql.parser.SqlParseException;
import org.apache.calcite.sql.parser.SqlParser;
import org.apache.calcite.tools.FrameworkConfig;
import org.apache.calcite.tools.Planner;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

import static ru.datamart.prostore.query.execution.plugin.adp.util.TestUtils.SCHEMA;
import static ru.datamart.prostore.query.execution.plugin.adp.util.TestUtils.TABLE;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

class AdpTruncateHistoryServiceTest {

    private final CalciteConfiguration calciteConfiguration = new CalciteConfiguration();
    private final CalciteCoreConfiguration calciteCoreConfiguration = new CalciteCoreConfiguration();
    private final SqlParser.Config parserConfig = calciteConfiguration
            .configDdlParser(calciteCoreConfiguration.eddlParserImplFactory());
    private final DtmCalciteFramework.ConfigBuilder configBuilder = DtmCalciteFramework.newConfigBuilder();
    private final FrameworkConfig frameworkConfig = configBuilder.parserConfig(parserConfig).build();
    private final Planner planner = DtmCalciteFramework.getPlanner(frameworkConfig);
    private final TruncateHistoryFactory truncateHistoryFactory = new TruncateHistoryFactory(calciteConfiguration.adpSqlDialect());
    private final DatabaseExecutor databaseExecutor = mock(DatabaseExecutor.class);
    private final AdpTruncateHistoryService truncateHistoryService = new AdpTruncateHistoryService(databaseExecutor, truncateHistoryFactory);

    @BeforeEach
    void setUp() {
        when(databaseExecutor.execute(anyString())).thenReturn(Future.succeededFuture());
        when(databaseExecutor.executeInTransaction(any())).thenReturn(Future.succeededFuture());
    }

    @Test
    void testWithoutConditions() {
        List<String> expectedSqlList = Collections.singletonList(
                String.format("DELETE FROM %s.%s_actual", SCHEMA, TABLE)
        );
        test(null, expectedSqlList);
    }

    @Test
    void testWithConditions() {
        String conditions = "id > 2";
        List<String> expectedSqlList = Collections.singletonList(
                String.format("DELETE FROM %s.%s_actual WHERE %s", SCHEMA, TABLE, conditions));
        test(conditions, expectedSqlList);
    }

    @Test
    void testWithSysCn() {
        Long sysCn = 1L;
        String expectedSql = String.format("DELETE FROM %s.%s_actual WHERE sys_to < %s", SCHEMA, TABLE, sysCn);
        test(sysCn, null, expectedSql);
    }

    @Test
    void testWithConditionsAndSysCn() {
        String conditions = "id > 2";
        Long sysCn = 1L;
        String expectedSql = String.format("DELETE FROM %s.%s_actual WHERE %s AND sys_to < %s", SCHEMA, TABLE, conditions, sysCn);
        test(sysCn, conditions, expectedSql);
    }

    private void test(String conditions, List<String> expectedSqlList) {
        truncateHistoryService.truncateHistory(createRequest(null, conditions))
                .onComplete(ar -> assertTrue(ar.succeeded()));

        verify(databaseExecutor).executeInTransaction(argThat(input -> input.stream()
                .map(PreparedStatementRequest::getSql)
                .collect(Collectors.toList())
                .equals(expectedSqlList)));
    }

    private void test(Long sysCn, String conditions, String expectedSql) {
        truncateHistoryService.truncateHistory(createRequest(sysCn, conditions))
                .onComplete(ar -> assertTrue(ar.succeeded()));
        verify(databaseExecutor, times(1)).execute(expectedSql);
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
