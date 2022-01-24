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
package io.arenadata.dtm.query.execution.core.check;

import com.google.common.collect.Lists;
import io.arenadata.dtm.common.exception.DtmException;
import io.arenadata.dtm.common.metrics.RequestMetrics;
import io.arenadata.dtm.common.model.ddl.ColumnType;
import io.arenadata.dtm.common.model.ddl.Entity;
import io.arenadata.dtm.common.model.ddl.EntityField;
import io.arenadata.dtm.common.model.ddl.EntityType;
import io.arenadata.dtm.common.reader.QueryRequest;
import io.arenadata.dtm.common.reader.SourceType;
import io.arenadata.dtm.common.request.DatamartRequest;
import io.arenadata.dtm.query.calcite.core.extension.check.CheckType;
import io.arenadata.dtm.query.calcite.core.extension.check.SqlGetEntityDdl;
import io.arenadata.dtm.query.execution.core.base.repository.zookeeper.EntityDao;
import io.arenadata.dtm.query.execution.core.base.service.metadata.query.DdlQueryGenerator;
import io.arenadata.dtm.query.execution.core.base.service.metadata.query.CoreDdlQueryGenerator;
import io.arenadata.dtm.query.execution.core.check.dto.CheckContext;
import io.arenadata.dtm.query.execution.core.check.factory.CheckEntityDdlResultFactory;
import io.arenadata.dtm.query.execution.core.check.service.impl.GetEntityDdlExecutor;
import io.vertx.core.Future;
import io.vertx.junit5.VertxExtension;
import io.vertx.junit5.VertxTestContext;
import lombok.val;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

import java.util.EnumSet;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

@ExtendWith(VertxExtension.class)
class GetEntityDdlExecutorTest {
    private final static String SCHEMA = "TEST";
    private final static String ENTITY_NAME_TABLE = "TEST_TABLE";
    private final static String ENTITY_NAME_VIEW = "TEST_VIEW";
    private final static String ENTITY_NAME_MATVIEW = "TEST_MATVIEW";
    private final static String INVALID_ENTITY_NAME = "INVALID_NAME";

    private final static String CREATE_TABLE_SCRIPT_EXPECTED =
            "CREATE TABLE " + SCHEMA + "." + ENTITY_NAME_TABLE + " (" +
                    "ID INT NOT NULL, " +
                    "DESCRIPTION VARCHAR(200) NOT NULL, " +
                    "FOREIGN_KEY INT NOT NULL, " +
                    "TIME_COL TIME(5) NULL, " +
                    "PRIMARY KEY (ID)" +
                    ") DISTRIBUTED BY (ID) DATASOURCE_TYPE (ADB, ADG, ADQM, ADP)";

    private final static String CREATE_VIEW_SCRIPT_EXPECTED =
            "CREATE VIEW " + SCHEMA + "." + ENTITY_NAME_VIEW +
                    " AS SELECT TEST_TABLE2.NAME, " +
                    "TEST_TABLE.DESCRIPTION " +
                    "FROM TEST.TEST_TABLE " +
                    "LEFT JOIN TEST.TEST_TABLE2 ON TEST_TABLE2.ID = TEST_TABLE.FOREIGN_KEY " +
                    "ORDER BY TEST_TABLE2.NAME";

    private final static String CREATE_MATERIALIZED_VIEW_SCRIPT_EXPECTED =
            "CREATE MATERIALIZED VIEW " + SCHEMA + "." + ENTITY_NAME_MATVIEW +
                    " (ID INT NOT NULL, " +
                    "DESCRIPTION VARCHAR(200) NOT NULL, " +
                    "FOREIGN_KEY INT NOT NULL, " +
                    "TIME_COL TIME(5) NULL, " +
                    "PRIMARY KEY (ID)" +
                    ") DISTRIBUTED BY (ID) " +
                    "DATASOURCE_TYPE (ADB, ADG, ADQM, ADP) " +
                    "AS SELECT ID AS ID, " +
                    "DESCRIPTION AS DESCRIPTION, " +
                    "FOREIGN_KEY AS FOREIGN_KEY " +
                    "FROM TEST.TEST_TABLE " +
                    "DATASOURCE_TYPE = 'ADQM'";

    private final SqlGetEntityDdl sqlGetEntityDdl = mock(SqlGetEntityDdl.class);
    private final EntityDao entityDao = mock(EntityDao.class);
    private final DdlQueryGenerator ddlQueryGenerator = new CoreDdlQueryGenerator();
    private final CheckEntityDdlResultFactory resultFactory = new CheckEntityDdlResultFactory();

    private CheckContext checkContext;
    private GetEntityDdlExecutor entityDDLExecutor;
    private Entity entity;
    private List<EntityField> tableFields;

    @BeforeEach
    void setUp() {
        QueryRequest queryRequest = new QueryRequest();
        queryRequest.setDatamartMnemonic(SCHEMA);
        entityDDLExecutor = new GetEntityDdlExecutor(entityDao, resultFactory, ddlQueryGenerator);
        val idCol = EntityField.builder()
                .ordinalPosition(0)
                .name("ID")
                .type(ColumnType.INT)
                .nullable(false)
                .primaryOrder(1)
                .shardingOrder(1)
                .build();
        val descriptionCol = EntityField.builder()
                .ordinalPosition(1)
                .name("DESCRIPTION")
                .type(ColumnType.VARCHAR)
                .size(200)
                .nullable(false)
                .build();
        val foreignKeyCol = EntityField.builder()
                .ordinalPosition(2)
                .name("FOREIGN_KEY")
                .type(ColumnType.INT)
                .nullable(false)
                .build();
        val timeCol = EntityField.builder()
                .ordinalPosition(3)
                .name("TIME_COL")
                .type(ColumnType.TIME)
                .nullable(true)
                .accuracy(5)
                .build();
        tableFields = Lists.newArrayList(idCol, descriptionCol, foreignKeyCol, timeCol);
        checkContext = new CheckContext(new RequestMetrics(), "env",
                new DatamartRequest(queryRequest),
                CheckType.ENTITY_DDL,
                sqlGetEntityDdl);
    }

    @Test
    void executeForTableEntity(VertxTestContext testContext) {
        entity = getEntityByType(EntityType.TABLE);
        when(entityDao.getEntity(SCHEMA, entity.getName())).thenReturn(Future.succeededFuture(entity));
        when(sqlGetEntityDdl.getEntity()).thenReturn(entity.getName());

        entityDDLExecutor.execute(checkContext)
                .onComplete(ar -> testContext.verify(() -> {
                    assertTrue(ar.succeeded());
                    assertEquals(CREATE_TABLE_SCRIPT_EXPECTED,
                            ar.result().getResult().get(0).get("DDL_SCRIPT"));
                }).completeNow());
    }

    @Test
    void executeForViewEntity(VertxTestContext testContext) {
        entity = getEntityByType(EntityType.VIEW);
        when(entityDao.getEntity(SCHEMA, entity.getName())).thenReturn(Future.succeededFuture(entity));
        when(sqlGetEntityDdl.getEntity()).thenReturn(entity.getName());

        entityDDLExecutor.execute(checkContext)
                .onComplete(ar -> testContext.verify(() -> {
                    assertTrue(ar.succeeded());
                    assertEquals(CREATE_VIEW_SCRIPT_EXPECTED,
                            ar.result().getResult().get(0).get("DDL_SCRIPT"));
                }).completeNow());
    }

    @Test
    void executeForMaterializedViewEntity(VertxTestContext testContext) {
        entity = getEntityByType(EntityType.MATERIALIZED_VIEW);
        when(entityDao.getEntity(SCHEMA, entity.getName())).thenReturn(Future.succeededFuture(entity));
        when(sqlGetEntityDdl.getEntity()).thenReturn(entity.getName());

        entityDDLExecutor.execute(checkContext)
                .onComplete(ar -> testContext.verify(() -> {
                    assertTrue(ar.succeeded());
                    assertEquals(CREATE_MATERIALIZED_VIEW_SCRIPT_EXPECTED,
                            ar.result().getResult().get(0).get("DDL_SCRIPT"));
                }).completeNow());
    }

    @Test
    void executeErrorEntityNotExist(VertxTestContext testContext) {
        val errorMsg = "Entity " + INVALID_ENTITY_NAME + " doesn't exist";
        when(sqlGetEntityDdl.getEntity()).thenReturn(INVALID_ENTITY_NAME);
        when(entityDao.getEntity(any(), eq(INVALID_ENTITY_NAME))).thenReturn(Future.failedFuture(new DtmException(errorMsg)));

        entityDDLExecutor.execute(checkContext)
                .onComplete(ar -> testContext.verify(() -> {
                    assertTrue(ar.failed());
                    assertEquals(DtmException.class, ar.cause().getClass());
                    assertEquals(errorMsg, ar.cause().getMessage());
                }).completeNow());
    }

    private Entity getEntityByType(EntityType type) {
        if (type == EntityType.TABLE) {
            return Entity.builder()
                    .name(ENTITY_NAME_TABLE)
                    .entityType(EntityType.TABLE)
                    .schema(SCHEMA)
                    .fields(tableFields)
                    .destination(EnumSet.of(SourceType.ADB, SourceType.ADQM, SourceType.ADG, SourceType.ADP))
                    .build();
        } else if (type == EntityType.VIEW) {
            val queryViewAs = "select test_table2.name, " +
                    "TEST_TABLE.description " +
                    "from test.test_table " +
                    "left JOIN test.test_table2 on test_table2.id = test_table.foreign_key " +
                    "order by test_table2.name";
            return Entity.builder()
                    .name(ENTITY_NAME_VIEW)
                    .entityType(EntityType.VIEW)
                    .schema(SCHEMA)
                    .viewQuery(queryViewAs)
                    .build();
        } else if (type == EntityType.MATERIALIZED_VIEW) {
            val queryViewAs = "select id as id, " +
                    "description as description, " +
                    "foreign_key as foreign_key " +
                    "from test.test_table";
            return Entity.builder()
                    .name(ENTITY_NAME_MATVIEW)
                    .entityType(EntityType.MATERIALIZED_VIEW)
                    .schema(SCHEMA)
                    .fields(tableFields)
                    .destination(EnumSet.of(SourceType.ADB, SourceType.ADQM, SourceType.ADG, SourceType.ADP))
                    .viewQuery(queryViewAs)
                    .materializedDataSource(SourceType.ADQM)
                    .build();
        } else {
            return null;
        }
    }

}