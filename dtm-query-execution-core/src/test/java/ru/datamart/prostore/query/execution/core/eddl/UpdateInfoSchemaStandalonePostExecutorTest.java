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
package ru.datamart.prostore.query.execution.core.eddl;

import io.vertx.core.Future;
import io.vertx.junit5.VertxExtension;
import io.vertx.junit5.VertxTestContext;
import lombok.val;
import org.apache.calcite.sql.SqlKind;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import ru.datamart.prostore.common.exception.DtmException;
import ru.datamart.prostore.common.model.ddl.Entity;
import ru.datamart.prostore.common.model.ddl.EntityType;
import ru.datamart.prostore.query.execution.core.base.service.metadata.InformationSchemaService;
import ru.datamart.prostore.query.execution.core.eddl.dto.CreateDownloadExternalTableQuery;
import ru.datamart.prostore.query.execution.core.eddl.dto.CreateStandaloneExternalTableQuery;
import ru.datamart.prostore.query.execution.core.eddl.dto.DropStandaloneExternalTableQuery;
import ru.datamart.prostore.query.execution.core.eddl.dto.EddlAction;
import ru.datamart.prostore.query.execution.core.eddl.service.standalone.UpdateInfoSchemaStandalonePostExecutor;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@ExtendWith({VertxExtension.class, MockitoExtension.class})
class UpdateInfoSchemaStandalonePostExecutorTest {

    private static final String DATAMART = "dtm";
    private static final String TABLE = "tbl";
    private static final Entity ENTITY = Entity.builder()
            .schema(DATAMART)
            .name(TABLE)
            .build();
    private static final String ERROR_MSG = "error";

    @Mock
    private InformationSchemaService informationSchemaService;

    @InjectMocks
    private UpdateInfoSchemaStandalonePostExecutor postExecutor;

    @Captor
    private ArgumentCaptor<Entity> entityArgumentCaptor;

    @Test
    void shouldSuccessWhenCreateReadableExternalTable(VertxTestContext testContext) {
        val createEddl = CreateStandaloneExternalTableQuery.builder()
                .entity(ENTITY)
                .eddlAction(EddlAction.CREATE_READABLE_EXTERNAL_TABLE)
                .build();
        when(informationSchemaService.update(any(Entity.class), nullable(String.class), eq(SqlKind.CREATE_TABLE)))
                .thenReturn(Future.succeededFuture());

        postExecutor.execute(createEddl)
                .onComplete(ar -> testContext.verify(() -> {
                    assertTrue(ar.succeeded());

                    verify(informationSchemaService).update(entityArgumentCaptor.capture(), nullable(String.class), eq(SqlKind.CREATE_TABLE));
                    assertEquals(ENTITY, entityArgumentCaptor.getValue());
                }).completeNow());
    }

    @Test
    void shouldSuccessWhenCreateWritableExternalTable(VertxTestContext testContext) {
        val createEddl = CreateStandaloneExternalTableQuery.builder()
                .entity(ENTITY)
                .eddlAction(EddlAction.CREATE_WRITEABLE_EXTERNAL_TABLE)
                .build();
        when(informationSchemaService.update(any(Entity.class), nullable(String.class), eq(SqlKind.CREATE_TABLE)))
                .thenReturn(Future.succeededFuture());

        postExecutor.execute(createEddl)
                .onComplete(ar -> testContext.verify(() -> {
                    assertTrue(ar.succeeded());

                    verify(informationSchemaService).update(entityArgumentCaptor.capture(), nullable(String.class), eq(SqlKind.CREATE_TABLE));
                    assertEquals(ENTITY, entityArgumentCaptor.getValue());
                }).completeNow());
    }

    @Test
    void shouldFailWhenCreateWritableExternalTable(VertxTestContext testContext) {
        val createEddl = CreateStandaloneExternalTableQuery.builder()
                .entity(ENTITY)
                .eddlAction(EddlAction.CREATE_WRITEABLE_EXTERNAL_TABLE)
                .build();
        when(informationSchemaService.update(any(Entity.class), nullable(String.class), eq(SqlKind.CREATE_TABLE)))
                .thenReturn(Future.failedFuture(ERROR_MSG));

        postExecutor.execute(createEddl)
                .onComplete(ar -> testContext.verify(() -> {
                    assertTrue(ar.failed());
                    assertEquals(ERROR_MSG, ar.cause().getMessage());
                }).completeNow());
    }

    @Test
    void shouldSuccessWhenDropWritableExternalTable(VertxTestContext testContext) {
        val dropEddl = DropStandaloneExternalTableQuery.builder()
                .schemaName(DATAMART)
                .tableName(TABLE)
                .eddlAction(EddlAction.DROP_WRITEABLE_EXTERNAL_TABLE)
                .build();
        when(informationSchemaService.update(any(Entity.class), nullable(String.class), eq(SqlKind.DROP_TABLE)))
                .thenReturn(Future.succeededFuture());

        postExecutor.execute(dropEddl)
                .onComplete(ar -> testContext.verify(() -> {
                    assertTrue(ar.succeeded());

                    verify(informationSchemaService).update(entityArgumentCaptor.capture(), nullable(String.class), eq(SqlKind.DROP_TABLE));
                    val entity = entityArgumentCaptor.getValue();
                    assertEquals(DATAMART, entity.getSchema());
                    assertEquals(TABLE, entity.getName());
                    assertEquals(EntityType.WRITEABLE_EXTERNAL_TABLE, entity.getEntityType());
                }).completeNow());
    }

    @Test
    void shouldSuccessWhenDropReadableExternalTable(VertxTestContext testContext) {
        val dropEddl = DropStandaloneExternalTableQuery.builder()
                .schemaName(DATAMART)
                .tableName(TABLE)
                .eddlAction(EddlAction.DROP_READABLE_EXTERNAL_TABLE)
                .build();
        when(informationSchemaService.update(any(Entity.class), nullable(String.class), eq(SqlKind.DROP_TABLE)))
                .thenReturn(Future.succeededFuture());

        postExecutor.execute(dropEddl)
                .onComplete(ar -> testContext.verify(() -> {
                    assertTrue(ar.succeeded());

                    verify(informationSchemaService).update(entityArgumentCaptor.capture(), nullable(String.class), eq(SqlKind.DROP_TABLE));
                    val entity = entityArgumentCaptor.getValue();
                    assertEquals(DATAMART, entity.getSchema());
                    assertEquals(TABLE, entity.getName());
                    assertEquals(EntityType.READABLE_EXTERNAL_TABLE, entity.getEntityType());
                }).completeNow());
    }

    @Test
    void shouldFailWhenDropReadableExternalTable(VertxTestContext testContext) {
        val dropEddl = DropStandaloneExternalTableQuery.builder()
                .schemaName(DATAMART)
                .tableName(TABLE)
                .eddlAction(EddlAction.DROP_READABLE_EXTERNAL_TABLE)
                .build();
        when(informationSchemaService.update(any(Entity.class), nullable(String.class), eq(SqlKind.DROP_TABLE)))
                .thenReturn(Future.failedFuture(ERROR_MSG));

        postExecutor.execute(dropEddl)
                .onComplete(ar -> testContext.verify(() -> {
                    assertTrue(ar.failed());
                    assertEquals(ERROR_MSG, ar.cause().getMessage());
                }).completeNow());
    }

    @Test
    void shouldFailWhenUnsupportedQueryType(VertxTestContext testContext) {
        val dropEddl = CreateDownloadExternalTableQuery.builder()
                .build();

        postExecutor.execute(dropEddl)
                .onComplete(ar -> testContext.verify(() -> {
                    assertTrue(ar.failed());
                    assertTrue(ar.cause() instanceof DtmException);
                    assertTrue(ar.cause().getMessage().contains(EddlAction.CREATE_DOWNLOAD_EXTERNAL_TABLE.name()));
                }).completeNow());
    }

}
