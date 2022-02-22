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
package ru.datamart.prostore.query.execution.core.base.schema;

import ru.datamart.prostore.common.dto.schema.DatamartSchemaKey;
import ru.datamart.prostore.common.exception.DtmException;
import ru.datamart.prostore.common.model.ddl.ColumnType;
import ru.datamart.prostore.common.model.ddl.Entity;
import ru.datamart.prostore.common.model.ddl.EntityField;
import ru.datamart.prostore.common.model.ddl.EntityType;
import ru.datamart.prostore.query.calcite.core.configuration.CalciteCoreConfiguration;
import ru.datamart.prostore.query.calcite.core.framework.DtmCalciteFramework;
import ru.datamart.prostore.query.execution.core.base.repository.ServiceDbFacade;
import ru.datamart.prostore.query.execution.core.base.repository.ServiceDbFacadeImpl;
import ru.datamart.prostore.query.execution.core.base.repository.zookeeper.EntityDao;
import ru.datamart.prostore.query.execution.core.base.repository.zookeeper.ServiceDbDao;
import ru.datamart.prostore.query.execution.core.base.service.delta.DeltaInformationExtractor;
import ru.datamart.prostore.query.execution.core.base.service.metadata.LogicalSchemaService;
import ru.datamart.prostore.query.execution.core.calcite.configuration.CalciteConfiguration;
import io.vertx.core.Future;
import io.vertx.core.Promise;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.parser.SqlParseException;
import org.apache.calcite.sql.parser.SqlParser;
import org.apache.calcite.tools.FrameworkConfig;
import org.apache.calcite.tools.Planner;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

class LogicalSchemaServiceTest {

    public static final String DATAMART = "test_datamart";
    public static final String TABLE_PSO = "pso";
    public static final String TABLE_DOC = "doc";
    private final CalciteConfiguration config = new CalciteConfiguration();
    private final DeltaInformationExtractor deltaInformationExtractor = new DeltaInformationExtractor();
    private final CalciteCoreConfiguration calciteCoreConfiguration = new CalciteCoreConfiguration();
    private final SqlParser.Config parserConfig = config.configEddlParser(calciteCoreConfiguration.eddlParserImplFactory());
    private final DtmCalciteFramework.ConfigBuilder configBuilder = DtmCalciteFramework.newConfigBuilder();
    private final FrameworkConfig frameworkConfig = configBuilder.parserConfig(parserConfig).build();
    private final Planner planner = DtmCalciteFramework.getPlanner(frameworkConfig);
    private final ServiceDbFacade serviceDbFacade = mock(ServiceDbFacadeImpl.class);
    private final ServiceDbDao serviceDbDao = mock(ServiceDbDao.class);
    private final EntityDao entityDao = mock(EntityDao.class);
    private LogicalSchemaService logicalSchemaService;
    private SqlNode query;

    @BeforeEach
    void setUp() {
        when(serviceDbFacade.getServiceDbDao()).thenReturn(serviceDbDao);
        when(serviceDbDao.getEntityDao()).thenReturn(entityDao);
        logicalSchemaService = new LogicalSchemaService(serviceDbFacade, deltaInformationExtractor);
    }

    @Test
    void createSchemaSuccess() throws SqlParseException {
        Promise<Map<DatamartSchemaKey, Entity>> promise = Promise.promise();
        final Map<DatamartSchemaKey, Entity> resultSchemaMap = new HashMap<>();
        query = planner.parse("select t1.id, cast(t2.id as varchar(10)) as tt from test_datamart.pso t1 \n" +
                " join test_datamart.doc t2 on t1.id = t2.id");
        Entity pso = Entity.builder()
                .schema(DATAMART)
                .name(TABLE_PSO)
                .build();

        EntityField entityField = EntityField.builder()
                .name("id")
                .type(ColumnType.INT)
                .ordinalPosition(0)
                .shardingOrder(1)
                .nullable(false)
                .primaryOrder(1)
                .accuracy(0)
                .size(0)
                .build();
        List<EntityField> psoAttrs = Collections.singletonList(entityField);
        pso.setFields(psoAttrs);

        Entity doc = Entity.builder()
                .schema(DATAMART)
                .name(TABLE_DOC)
                .build();
        List<EntityField> docAttrs = Collections.singletonList(entityField);
        doc.setFields(docAttrs);

        resultSchemaMap.put(new DatamartSchemaKey(DATAMART, TABLE_PSO), pso);
        resultSchemaMap.put(new DatamartSchemaKey(DATAMART, TABLE_DOC), doc);

        Entity.EntityBuilder builder = Entity.builder()
                .schema(DATAMART)
                .entityType(EntityType.TABLE)
                .fields(Collections.singletonList(
                        EntityField.builder()
                                .name("id")
                                .accuracy(0)
                                .size(0)
                                .ordinalPosition(0)
                                .nullable(false)
                                .shardingOrder(1)
                                .primaryOrder(1)
                                .type(ColumnType.INT)
                                .build()
                ));

        when(entityDao.getEntity(any(), any()))
                .thenReturn(
                        Future.succeededFuture(
                                builder
                                        .name(TABLE_PSO)
                                        .build()),
                        Future.succeededFuture(
                                builder
                                        .name(TABLE_DOC)
                                        .build())
                );
        logicalSchemaService.createSchemaFromQuery(query, DATAMART)
                .onComplete(promise);
        Map<DatamartSchemaKey, Entity> schemaMap = promise.future().result();
        assertNotNull(schemaMap);
        schemaMap.forEach((k, v) -> {
            assertEquals(resultSchemaMap.get(k).getName(), v.getName());
            assertEquals(resultSchemaMap.get(k).getName(), v.getName());
            assertEquals(resultSchemaMap.get(k).getFields().get(0).getName(), v.getFields().get(0).getName());
            assertEquals(resultSchemaMap.get(k).getFields().get(0).getType(), v.getFields().get(0).getType());
            assertEquals(resultSchemaMap.get(k).getFields().get(0).getPrimaryOrder(), v.getFields().get(0).getPrimaryOrder());
            assertEquals(resultSchemaMap.get(k).getFields().get(0).getShardingOrder(), v.getFields().get(0).getShardingOrder());
        });
    }

    @Test
    void createSchemaWithDefaultDatmart() throws SqlParseException {
        Promise<Map<DatamartSchemaKey, Entity>> promise = Promise.promise();
        final Map<DatamartSchemaKey, Entity> resultSchemaMap = new HashMap<>();
        query = planner.parse("select t1.id, cast(t2.id as varchar(10)) as tt from pso t1 \n" +
                " join doc t2 on t1.id = t2.id");
        Entity pso = Entity.builder()
                .schema(DATAMART)
                .name(TABLE_PSO)
                .build();

        EntityField entityField = EntityField.builder()
                .name("id")
                .type(ColumnType.INT)
                .ordinalPosition(0)
                .shardingOrder(1)
                .nullable(false)
                .primaryOrder(1)
                .accuracy(0)
                .size(0)
                .build();
        List<EntityField> psoAttrs = Collections.singletonList(entityField);
        pso.setFields(psoAttrs);

        Entity doc = Entity.builder()
                .schema(DATAMART)
                .name(TABLE_DOC)
                .build();
        List<EntityField> docAttrs = Collections.singletonList(entityField);
        doc.setFields(docAttrs);

        resultSchemaMap.put(new DatamartSchemaKey(DATAMART, TABLE_PSO), pso);
        resultSchemaMap.put(new DatamartSchemaKey(DATAMART, TABLE_DOC), doc);

        Entity.EntityBuilder builder = Entity.builder()
                .schema(DATAMART)
                .entityType(EntityType.TABLE)
                .fields(Collections.singletonList(
                        EntityField.builder()
                                .name("id")
                                .accuracy(0)
                                .size(0)
                                .ordinalPosition(0)
                                .nullable(false)
                                .shardingOrder(1)
                                .primaryOrder(1)
                                .type(ColumnType.INT)
                                .build()
                ));

        when(entityDao.getEntity(DATAMART, TABLE_PSO))
                .thenReturn(Future.succeededFuture(builder.name(TABLE_PSO).build()));

        when(entityDao.getEntity(DATAMART, TABLE_DOC))
                .thenReturn(Future.succeededFuture(builder.name(TABLE_DOC).build()));

        logicalSchemaService.createSchemaFromQuery(query, DATAMART)
                .onComplete(promise);

        Map<DatamartSchemaKey, Entity> schemaMap = promise.future().result();
        assertNotNull(schemaMap);
        schemaMap.forEach((k, v) -> {
            assertEquals(resultSchemaMap.get(k).getName(), v.getName());
            assertEquals(resultSchemaMap.get(k).getName(), v.getName());
            assertEquals(resultSchemaMap.get(k).getFields().get(0).getName(), v.getFields().get(0).getName());
            assertEquals(resultSchemaMap.get(k).getFields().get(0).getType(), v.getFields().get(0).getType());
            assertEquals(resultSchemaMap.get(k).getFields().get(0).getPrimaryOrder(), v.getFields().get(0).getPrimaryOrder());
            assertEquals(resultSchemaMap.get(k).getFields().get(0).getShardingOrder(), v.getFields().get(0).getShardingOrder());
        });
    }

    @Test
    void createSchemaWithDatamartEntityError() throws SqlParseException {
        Promise<Map<DatamartSchemaKey, Entity>> promise = Promise.promise();
        query = planner.parse("select t1.id, cast(t2.id as varchar(10)) as tt from test_datamart.pso t1 \n" +
                " join test_datamart.doc t2 on t1.id = t2.id");

        Mockito.when(entityDao.getEntity(any(), any()))
                .thenReturn(Future.failedFuture(new DtmException("Error getting entities!")));

        logicalSchemaService.createSchemaFromQuery(query, DATAMART)
                .onComplete(promise);
        assertNotNull(promise.future().cause());
    }

}
