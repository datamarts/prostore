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
package io.arenadata.dtm.query.execution.core.base.schema;

import io.arenadata.dtm.common.dto.schema.DatamartSchemaKey;
import io.arenadata.dtm.common.exception.DtmException;
import io.arenadata.dtm.common.model.ddl.ColumnType;
import io.arenadata.dtm.common.model.ddl.Entity;
import io.arenadata.dtm.common.model.ddl.EntityField;
import io.arenadata.dtm.query.calcite.core.configuration.CalciteCoreConfiguration;
import io.arenadata.dtm.query.calcite.core.framework.DtmCalciteFramework;
import io.arenadata.dtm.query.execution.core.calcite.configuration.CalciteConfiguration;
import io.arenadata.dtm.query.execution.core.base.service.metadata.LogicalSchemaProvider;
import io.arenadata.dtm.query.execution.core.base.service.metadata.LogicalSchemaService;
import io.arenadata.dtm.query.execution.model.metadata.Datamart;
import io.vertx.core.Future;
import io.vertx.core.Promise;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.parser.SqlParseException;
import org.apache.calcite.sql.parser.SqlParser;
import org.apache.calcite.tools.FrameworkConfig;
import org.apache.calcite.tools.Planner;
import org.junit.jupiter.api.Test;

import java.util.*;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.mockito.Mockito.*;

class LogicalSchemaProviderTest {

    private static final String DATAMART = "test_datamart";

    private final CalciteConfiguration config = new CalciteConfiguration();
    private final CalciteCoreConfiguration calciteCoreConfiguration = new CalciteCoreConfiguration();
    private final SqlParser.Config parserConfig = config.configEddlParser(calciteCoreConfiguration.eddlParserImplFactory());
    private final DtmCalciteFramework.ConfigBuilder configBuilder = DtmCalciteFramework.newConfigBuilder();
    private final FrameworkConfig frameworkConfig = configBuilder.parserConfig(parserConfig).build();
    private final Planner planner = DtmCalciteFramework.getPlanner(frameworkConfig);
    private final LogicalSchemaService logicalSchemaService = mock(LogicalSchemaService.class);
    private final LogicalSchemaProvider logicalSchemaProvider = new LogicalSchemaProvider(logicalSchemaService);
    private SqlNode query;

    @Test
    void createSchemaSuccess() throws SqlParseException {
        query = planner.parse("CREATE SCHEMA " + DATAMART);
        Promise<List<Datamart>> promise = Promise.promise();
        final Map<DatamartSchemaKey, Entity> datamartTableMap = new HashMap<>();
        Entity table1 = Entity.builder()
                .schema(DATAMART)
                .name("doc")
                .build();

        EntityField attr = EntityField.builder()
                .name("id")
                .type(ColumnType.VARCHAR)
                .primaryOrder(1)
                .shardingOrder(1)
                .build();

        table1.setFields(Collections.singletonList(attr));


        datamartTableMap.put(new DatamartSchemaKey(DATAMART, "doc"), table1);

        List<Datamart> datamarts = new ArrayList<>();
        Datamart dm = new Datamart();
        dm.setMnemonic(DATAMART);
        dm.setEntities(Collections.singletonList(table1));
        datamarts.add(dm);
        when(logicalSchemaService.createSchemaFromQuery(any(), any()))
                .thenReturn(Future.succeededFuture(datamartTableMap));

        logicalSchemaProvider.getSchemaFromQuery(query, DATAMART)
                .onComplete(promise);
        assertEquals(datamarts.get(0).getMnemonic(), promise.future().result().get(0).getMnemonic());
        assertEquals(datamarts.get(0).getEntities(), promise.future().result().get(0).getEntities());
    }

    @Test
    void createSchemaWithServiceError() {
        Promise<List<Datamart>> promise = Promise.promise();
        when(logicalSchemaService.createSchemaFromQuery(any(), any()))
                .thenReturn(Future.failedFuture(new DtmException("Schema creation error")));

        logicalSchemaProvider.getSchemaFromQuery(query, DATAMART)
                .onComplete(promise);
        assertNotNull(promise.future().cause());
    }
}
