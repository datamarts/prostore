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
package ru.datamart.prostore.query.execution.core.base.repository.zookeeper;

import io.vertx.core.Future;
import io.vertx.junit5.VertxExtension;
import io.vertx.junit5.VertxTestContext;
import lombok.val;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.Watcher;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import ru.datamart.prostore.common.exception.DtmException;
import ru.datamart.prostore.common.model.ddl.Changelog;
import ru.datamart.prostore.common.model.ddl.Entity;
import ru.datamart.prostore.query.execution.core.base.dto.metadata.DatamartEntity;
import ru.datamart.prostore.query.execution.core.base.exception.datamart.DatamartNotExistsException;
import ru.datamart.prostore.query.execution.core.base.exception.entity.EntityAlreadyExistsException;
import ru.datamart.prostore.query.execution.core.base.exception.entity.EntityNotExistsException;
import ru.datamart.prostore.query.execution.core.base.service.zookeeper.ZookeeperExecutor;
import ru.datamart.prostore.query.execution.core.delta.dto.OkDelta;
import ru.datamart.prostore.serialization.CoreSerialization;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

@ExtendWith(VertxExtension.class)
class EntityDaoTest {
    private static final String ENV = "test";
    private static final String DATAMART = "dtm";
    private static final String ENTITY_NAME = "tbl";
    private static final String ERROR = "error";
    private static final List<String> NON_LOGICAL_ENTITIES = Arrays.asList("tbl1", "tbl2", "logic_schema_entities");
    private static final List<String> ALL_ENTITIES = Arrays.asList("tbl1", "tbl2", "logic_schema_entities");
    private static final ZookeeperExecutor EXECUTOR = mock(ZookeeperExecutor.class);
    private static final Entity ENTITY = Entity.builder()
            .schema(DATAMART)
            .name(ENTITY_NAME)
            .build();

    private static EntityDao entityDao = new EntityDao(EXECUTOR, ENV);

    @Test
    void getEntitiesMetaShouldSucceed(VertxTestContext testContext) {
        //arrange
        when(EXECUTOR.getChildren(anyString())).thenReturn(Future.succeededFuture(ALL_ENTITIES));

        //act
        entityDao.getEntitiesMeta(DATAMART)
                .onComplete(ar -> testContext.verify(() -> {
                    //assert
                    assertTrue(ar.succeeded());
                    assertEquals(2, ar.result().size());
                    assertTrue(ar.result().stream()
                            .map(DatamartEntity::getMnemonic)
                            .allMatch(NON_LOGICAL_ENTITIES::contains));
                }).completeNow());
    }

    @Test
    void getEntitiesMetaShouldFailedWhenNoNodeException(VertxTestContext testContext) {
        //arrange
        when(EXECUTOR.getChildren(anyString())).thenReturn(Future.failedFuture(new KeeperException.NoNodeException()));

        //act
        entityDao.getEntitiesMeta(DATAMART)
                .onComplete(ar -> testContext.verify(() -> {
                    //assert
                    assertTrue(ar.failed());
                    assertTrue(ar.cause() instanceof DatamartNotExistsException);
                }).completeNow());
    }

    @Test
    void getEntitiesMetaShouldFailedWhenOtherException(VertxTestContext testContext) {
        //arrange
        when(EXECUTOR.getChildren(anyString())).thenReturn(Future.failedFuture(new DtmException(ERROR)));

        //act
        entityDao.getEntitiesMeta(DATAMART)
                .onComplete(ar -> testContext.verify(() -> {
                    //assert
                    assertTrue(ar.failed());
                    assertTrue(ar.cause() instanceof DtmException);
                }).completeNow());
    }

    @Test
    void setEntityStateShouldSucceedWhenCreate(VertxTestContext testContext) {
        //arrange
        val changeQuery = "change query";
        val deltaNum = 0L;
        val changelog = Changelog.builder()
                .deltaNum(deltaNum)
                .changeQuery(changeQuery)
                .build();
        when(EXECUTOR.getData(anyString(), nullable(Watcher.class), any())).thenReturn(Future.succeededFuture(CoreSerialization.serialize(changelog)));
        when(EXECUTOR.getChildren(anyString())).thenReturn(Future.succeededFuture(Collections.emptyList()));
        when(EXECUTOR.multi(anyCollection())).thenReturn(Future.succeededFuture());

        //act
        val deltaOk = OkDelta.builder().deltaNum(deltaNum).build();
        entityDao.setEntityState(ENTITY, deltaOk, changeQuery, SetEntityState.CREATE)
                .onComplete(ar -> testContext.verify(() -> {
                    //assert
                    assertTrue(ar.succeeded());
                }).completeNow());
    }

    @Test
    void setEntityStateShouldSucceedWhenUpdate(VertxTestContext testContext) {
        //arrange
        val changeQuery = "change query";
        val deltaNum = 0L;
        val changelog = Changelog.builder()
                .deltaNum(deltaNum)
                .changeQuery(changeQuery)
                .build();
        when(EXECUTOR.getData(anyString(), nullable(Watcher.class), any())).thenReturn(Future.succeededFuture(CoreSerialization.serialize(changelog)));
        when(EXECUTOR.getChildren(anyString())).thenReturn(Future.succeededFuture(Collections.emptyList()));
        when(EXECUTOR.multi(anyCollection())).thenReturn(Future.succeededFuture());

        //act
        val deltaOk = OkDelta.builder().deltaNum(deltaNum).build();
        entityDao.setEntityState(ENTITY, deltaOk, changeQuery, SetEntityState.UPDATE)
                .onComplete(ar -> testContext.verify(() -> {
                    //assert
                    assertTrue(ar.succeeded());
                }).completeNow());
    }

    @Test
    void setEntityStateShouldSucceedWhenDelete(VertxTestContext testContext) {
        //arrange
        val changeQuery = "change query";
        val deltaNum = 0L;
        val changelog = Changelog.builder()
                .deltaNum(deltaNum)
                .changeQuery(changeQuery)
                .build();
        when(EXECUTOR.getData(anyString(), nullable(Watcher.class), any())).thenReturn(Future.succeededFuture(CoreSerialization.serialize(changelog)));
        when(EXECUTOR.getChildren(anyString())).thenReturn(Future.succeededFuture(Collections.emptyList()));
        when(EXECUTOR.multi(anyCollection())).thenReturn(Future.succeededFuture());

        //act
        val deltaOk = OkDelta.builder().deltaNum(deltaNum).build();
        entityDao.setEntityState(ENTITY, deltaOk, changeQuery, SetEntityState.DELETE)
                .onComplete(ar -> testContext.verify(() -> {
                    //assert
                    assertTrue(ar.succeeded());
                }).completeNow());
    }

    @Test
    void setEntityStateShouldFailedWhenCheckChangelogNull(VertxTestContext testContext) {
        //arrange
        val changeQuery = "change query";
        val deltaNum = 0L;
        when(EXECUTOR.getData(anyString(), nullable(Watcher.class), any())).thenReturn(Future.succeededFuture(null));

        //act
        val deltaOk = OkDelta.builder().deltaNum(deltaNum).build();
        entityDao.setEntityState(ENTITY, deltaOk, changeQuery, SetEntityState.DELETE)
                .onComplete(ar -> testContext.verify(() -> {
                    //assert
                    assertTrue(ar.failed());
                    assertTrue(ar.cause() instanceof DtmException);
                    assertTrue(ar.cause().getMessage().contains("No changelog data"));
                }).completeNow());
    }

    @Test
    void setEntityStateShouldFailedWhenCheckChangelogPreviousNotComplete(VertxTestContext testContext) {
        //arrange
        val changeQuery = "change query";
        val deltaNum = 0L;
        val changelog = Changelog.builder()
                .deltaNum(deltaNum)
                .changeQuery(changeQuery)
                .build();
        when(EXECUTOR.getData(anyString(), nullable(Watcher.class), any())).thenReturn(Future.succeededFuture(CoreSerialization.serialize(changelog)));

        //act
        val deltaOk = OkDelta.builder().deltaNum(deltaNum).build();
        entityDao.setEntityState(ENTITY, deltaOk, changeQuery + "1", SetEntityState.DELETE)
                .onComplete(ar -> testContext.verify(() -> {
                    //assert
                    assertTrue(ar.failed());
                    assertTrue(ar.cause() instanceof DtmException);
                    assertTrue(ar.cause().getMessage().contains("Previous change operation is not completed in datamart"));
                }).completeNow());
    }

    @Test
    void setEntityStateShouldFailedWhenGetDataFailed(VertxTestContext testContext) {
        //arrange
        val changeQuery = "change query";
        val deltaNum = 0L;
        when(EXECUTOR.getData(anyString(), nullable(Watcher.class), any())).thenReturn(Future.failedFuture(ERROR));

        //act
        val deltaOk = OkDelta.builder().deltaNum(deltaNum).build();
        entityDao.setEntityState(ENTITY, deltaOk, changeQuery, SetEntityState.DELETE)
                .onComplete(ar -> testContext.verify(() -> {
                    //assert
                    assertTrue(ar.failed());
                }).completeNow());
    }

    @Test
    void setEntityStateShouldFailedWhenGetChildrenFailed(VertxTestContext testContext) {
        //arrange
        val changeQuery = "change query";
        val deltaNum = 0L;
        val changelog = Changelog.builder()
                .deltaNum(deltaNum)
                .changeQuery(changeQuery)
                .build();
        when(EXECUTOR.getData(anyString(), nullable(Watcher.class), any())).thenReturn(Future.succeededFuture(CoreSerialization.serialize(changelog)));
        when(EXECUTOR.getChildren(anyString())).thenReturn(Future.failedFuture(ERROR));

        //act
        val deltaOk = OkDelta.builder().deltaNum(deltaNum).build();
        entityDao.setEntityState(ENTITY, deltaOk, changeQuery, SetEntityState.DELETE)
                .onComplete(ar -> testContext.verify(() -> {
                    //assert
                    assertTrue(ar.failed());
                }).completeNow());
    }

    @Test
    void setEntityStateShouldFailedWhenMultiFailed(VertxTestContext testContext) {
        //arrange
        val changeQuery = "change query";
        val deltaNum = 0L;
        val changelog = Changelog.builder()
                .deltaNum(deltaNum)
                .changeQuery(changeQuery)
                .build();
        when(EXECUTOR.getData(anyString(), nullable(Watcher.class), any())).thenReturn(Future.succeededFuture(CoreSerialization.serialize(changelog)));
        when(EXECUTOR.getChildren(anyString())).thenReturn(Future.succeededFuture(Collections.emptyList()));
        when(EXECUTOR.multi(anyCollection())).thenReturn(Future.failedFuture(ERROR));

        //act
        val deltaOk = OkDelta.builder().deltaNum(deltaNum).build();
        entityDao.setEntityState(ENTITY, deltaOk, changeQuery, SetEntityState.DELETE)
                .onComplete(ar -> testContext.verify(() -> {
                    //assert
                    assertTrue(ar.failed());
                }).completeNow());
    }

    @Test
    void setEntityStateShouldFailedWhenMultiNodeExistsException(VertxTestContext testContext) {
        //arrange
        val changeQuery = "change query";
        val deltaNum = 0L;
        val changelog = Changelog.builder()
                .deltaNum(deltaNum)
                .changeQuery(changeQuery)
                .build();
        when(EXECUTOR.getData(anyString(), nullable(Watcher.class), any())).thenReturn(Future.succeededFuture(CoreSerialization.serialize(changelog)));
        when(EXECUTOR.getChildren(anyString())).thenReturn(Future.succeededFuture(Collections.emptyList()));
        when(EXECUTOR.multi(anyCollection())).thenReturn(Future.failedFuture(new KeeperException.NodeExistsException()));

        //act
        val deltaOk = OkDelta.builder().deltaNum(deltaNum).build();
        entityDao.setEntityState(ENTITY, deltaOk, changeQuery, SetEntityState.DELETE)
                .onComplete(ar -> testContext.verify(() -> {
                    //assert
                    assertTrue(ar.failed());
                    assertTrue(ar.cause() instanceof EntityAlreadyExistsException);
                }).completeNow());
    }

    @Test
    void setEntityStateShouldFailedWhenMultiBadVersionException(VertxTestContext testContext) {
        //arrange
        val changeQuery = "change query";
        val deltaNum = 0L;
        val changelog = Changelog.builder()
                .deltaNum(deltaNum)
                .changeQuery(changeQuery)
                .build();
        when(EXECUTOR.getData(anyString(), nullable(Watcher.class), any())).thenReturn(Future.succeededFuture(CoreSerialization.serialize(changelog)));
        when(EXECUTOR.getChildren(anyString())).thenReturn(Future.succeededFuture(Collections.emptyList()));
        when(EXECUTOR.multi(anyCollection())).thenReturn(Future.failedFuture(new KeeperException.BadVersionException()));

        //act
        val deltaOk = OkDelta.builder().deltaNum(deltaNum).build();
        entityDao.setEntityState(ENTITY, deltaOk, changeQuery, SetEntityState.DELETE)
                .onComplete(ar -> testContext.verify(() -> {
                    //assert
                    assertTrue(ar.failed());
                    assertTrue(ar.cause() instanceof DtmException);
                    assertTrue(ar.cause().getMessage().contains("Previous change operation is not completed in datamart"));
                }).completeNow());
    }

    @Test
    void createEntityShouldSucceed(VertxTestContext testContext) {
        //arrange
        when(EXECUTOR.createPersistentPath(anyString(), any())).thenReturn(Future.succeededFuture());

        //act
        entityDao.createEntity(ENTITY)
                .onComplete(ar -> testContext.verify(() -> {
                    //assert
                    assertTrue(ar.succeeded());
                }).completeNow());
    }

    @Test
    void createEntityShouldFailedWhenNoNodeException(VertxTestContext testContext) {
        //arrange
        when(EXECUTOR.createPersistentPath(anyString(), any())).thenReturn(Future.failedFuture(new KeeperException.NoNodeException()));

        //act
        entityDao.createEntity(ENTITY)
                .onComplete(ar -> testContext.verify(() -> {
                    //assert
                    assertTrue(ar.failed());
                    assertTrue(ar.cause() instanceof DatamartNotExistsException);
                }).completeNow());
    }

    @Test
    void createEntityShouldFailedWhenNodeExistsException(VertxTestContext testContext) {
        //arrange
        when(EXECUTOR.createPersistentPath(anyString(), any())).thenReturn(Future.failedFuture(new KeeperException.NodeExistsException()));

        //act
        entityDao.createEntity(ENTITY)
                .onComplete(ar -> testContext.verify(() -> {
                    //assert
                    assertTrue(ar.failed());
                    assertTrue(ar.cause() instanceof EntityAlreadyExistsException);
                }).completeNow());
    }

    @Test
    void createEntityShouldFailedWhenOtherException(VertxTestContext testContext) {
        //arrange
        when(EXECUTOR.createPersistentPath(anyString(), any())).thenReturn(Future.failedFuture(ERROR));

        //act
        entityDao.createEntity(ENTITY)
                .onComplete(ar -> testContext.verify(() -> {
                    //assert
                    assertTrue(ar.failed());
                    assertTrue(ar.cause() instanceof DtmException);
                }).completeNow());
    }

    @Test
    void updateEntityShouldSucceed(VertxTestContext testContext) {
        //arrange
        when(EXECUTOR.setData(anyString(), any(), anyInt())).thenReturn(Future.succeededFuture());

        //act
        entityDao.updateEntity(ENTITY)
                .onComplete(ar -> testContext.verify(() -> {
                    //assert
                    assertTrue(ar.succeeded());
                }).completeNow());
    }

    @Test
    void updateEntityShouldFailedWhenNoNodeException(VertxTestContext testContext) {
        //arrange
        when(EXECUTOR.setData(anyString(), any(), anyInt())).thenReturn(Future.failedFuture(new KeeperException.NoNodeException()));

        //act
        entityDao.updateEntity(ENTITY)
                .onComplete(ar -> testContext.verify(() -> {
                    //assert
                    assertTrue(ar.failed());
                    assertTrue(ar.cause() instanceof EntityNotExistsException);
                }).completeNow());
    }

    @Test
    void updateEntityShouldFailedWhenOtherException(VertxTestContext testContext) {
        //arrange
        when(EXECUTOR.setData(anyString(), any(), anyInt())).thenReturn(Future.failedFuture(ERROR));

        //act
        entityDao.updateEntity(ENTITY)
                .onComplete(ar -> testContext.verify(() -> {
                    //assert
                    assertTrue(ar.failed());
                    assertTrue(ar.cause() instanceof DtmException);
                }).completeNow());
    }

    @Test
    void existsEntityShouldSucceed(VertxTestContext testContext) {
        //arrange
        when(EXECUTOR.exists(anyString())).thenReturn(Future.succeededFuture(true));

        //act
        entityDao.existsEntity(DATAMART, ENTITY_NAME)
                .onComplete(ar -> testContext.verify(() -> {
                    //assert
                    assertTrue(ar.succeeded());
                    assertTrue(ar.result());
                }).completeNow());
    }

    @Test
    void existsEntityShouldFailedWhenException(VertxTestContext testContext) {
        //arrange
        when(EXECUTOR.exists(anyString())).thenReturn(Future.failedFuture(ERROR));

        //act
        entityDao.existsEntity(DATAMART, ENTITY_NAME)
                .onComplete(ar -> testContext.verify(() -> {
                    //assert
                    assertTrue(ar.failed());
                }).completeNow());
    }

    @Test
    void deleteEntityShouldSucceed(VertxTestContext testContext) {
        //arrange
        when(EXECUTOR.delete(anyString(), anyInt())).thenReturn(Future.succeededFuture());

        //act
        entityDao.deleteEntity(DATAMART, ENTITY_NAME)
                .onComplete(ar -> testContext.verify(() -> {
                    //assert
                    assertTrue(ar.succeeded());
                }).completeNow());
    }

    @Test
    void deleteEntityShouldFailedWhenNoNodeException(VertxTestContext testContext) {
        //arrange
        when(EXECUTOR.delete(anyString(), anyInt())).thenReturn(Future.failedFuture(new KeeperException.NoNodeException()));

        //act
        entityDao.deleteEntity(DATAMART, ENTITY_NAME)
                .onComplete(ar -> testContext.verify(() -> {
                    //assert
                    assertTrue(ar.failed());
                    assertTrue(ar.cause() instanceof EntityNotExistsException);
                }).completeNow());
    }

    @Test
    void deleteEntityShouldFailedWhenOtherException(VertxTestContext testContext) {
        //arrange
        when(EXECUTOR.delete(anyString(), anyInt())).thenReturn(Future.failedFuture(ERROR));

        //act
        entityDao.deleteEntity(DATAMART, ENTITY_NAME)
                .onComplete(ar -> testContext.verify(() -> {
                    //assert
                    assertTrue(ar.failed());
                    assertTrue(ar.cause() instanceof DtmException);
                }).completeNow());
    }

    @Test
    void getEntityShouldSucceed(VertxTestContext testContext) {
        //arrange
        when(EXECUTOR.getData(anyString())).thenReturn(Future.succeededFuture(CoreSerialization.serialize(ENTITY)));

        //act
        entityDao.getEntity(DATAMART, ENTITY_NAME)
                .onComplete(ar -> testContext.verify(() -> {
                    //assert
                    assertTrue(ar.succeeded());
                    assertEquals(ENTITY, ar.result());
                }).completeNow());
    }

    @Test
    void getEntityShouldFailedWhenNoNodeException(VertxTestContext testContext) {
        //arrange
        when(EXECUTOR.getData(anyString())).thenReturn(Future.failedFuture(new KeeperException.NoNodeException()));

        //act
        entityDao.getEntity(DATAMART, ENTITY_NAME)
                .onComplete(ar -> testContext.verify(() -> {
                    //assert
                    assertTrue(ar.failed());
                    assertTrue(ar.cause() instanceof EntityNotExistsException);
                }).completeNow());
    }

    @Test
    void getEntityShouldFailedWhenOtherException(VertxTestContext testContext) {
        //arrange
        when(EXECUTOR.getData(anyString())).thenReturn(Future.failedFuture(ERROR));

        //act
        entityDao.getEntity(DATAMART, ENTITY_NAME)
                .onComplete(ar -> testContext.verify(() -> {
                    //assert
                    assertTrue(ar.failed());
                    assertTrue(ar.cause() instanceof DtmException);
                }).completeNow());
    }

    @Test
    void getEntityNamesByDatamartShouldSucceed(VertxTestContext testContext) {
        //arrange
        when(EXECUTOR.getChildren(anyString())).thenReturn(Future.succeededFuture(ALL_ENTITIES));

        //act
        entityDao.getEntityNamesByDatamart(DATAMART)
                .onComplete(ar -> testContext.verify(() -> {
                    //assert
                    assertTrue(ar.succeeded());
                    assertEquals(ALL_ENTITIES, ar.result());
                }).completeNow());
    }

    @Test
    void getEntityNamesByDatamartShouldFailedWhenNoNodeException(VertxTestContext testContext) {
        //arrange
        when(EXECUTOR.getChildren(anyString())).thenReturn(Future.failedFuture(new KeeperException.NoNodeException()));

        //act
        entityDao.getEntityNamesByDatamart(DATAMART)
                .onComplete(ar -> testContext.verify(() -> {
                    //assert
                    assertTrue(ar.failed());
                    assertTrue(ar.cause() instanceof DatamartNotExistsException);
                }).completeNow());
    }

    @Test
    void getEntityNamesByDatamartShouldFailedWhenOtherException(VertxTestContext testContext) {
        //arrange
        when(EXECUTOR.getChildren(anyString())).thenReturn(Future.failedFuture(ERROR));

        //act
        entityDao.getEntityNamesByDatamart(DATAMART)
                .onComplete(ar -> testContext.verify(() -> {
                    //assert
                    assertTrue(ar.failed());
                    assertTrue(ar.cause() instanceof DtmException);
                }).completeNow());
    }
}
