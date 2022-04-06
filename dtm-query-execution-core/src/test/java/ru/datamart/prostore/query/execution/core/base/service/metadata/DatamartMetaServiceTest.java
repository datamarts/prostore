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
package ru.datamart.prostore.query.execution.core.base.service.metadata;

import io.vertx.core.Future;
import io.vertx.junit5.VertxExtension;
import io.vertx.junit5.VertxTestContext;
import lombok.val;
import org.assertj.core.util.Lists;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.junit.jupiter.MockitoExtension;
import ru.datamart.prostore.common.model.ddl.ColumnType;
import ru.datamart.prostore.common.model.ddl.Entity;
import ru.datamart.prostore.common.model.ddl.EntityField;
import ru.datamart.prostore.common.model.ddl.EntityType;
import ru.datamart.prostore.common.reader.SourceType;
import ru.datamart.prostore.query.execution.core.base.dto.metadata.DatamartEntity;
import ru.datamart.prostore.query.execution.core.base.dto.metadata.EntityAttribute;
import ru.datamart.prostore.query.execution.core.base.repository.ServiceDbFacade;
import ru.datamart.prostore.query.execution.core.base.repository.ServiceDbFacadeImpl;
import ru.datamart.prostore.query.execution.core.base.repository.zookeeper.DatamartDao;
import ru.datamart.prostore.query.execution.core.base.repository.zookeeper.EntityDao;
import ru.datamart.prostore.query.execution.core.base.repository.zookeeper.ServiceDbDao;

import java.util.EnumSet;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

@ExtendWith({MockitoExtension.class, VertxExtension.class})
class DatamartMetaServiceTest {
    private final static String DTM = "DTM";
    private final static String ENTITY = "ENTITY";
    private final static String ERROR = "ERROR";

    private final ServiceDbDao serviceDbDao = mock(ServiceDbDao.class);
    private final EntityDao entityDao = mock(EntityDao.class);
    private final DatamartDao datamartDao = mock(DatamartDao.class);
    private final ServiceDbFacade serviceDbFacade = mock(ServiceDbFacadeImpl.class);

    private DatamartMetaService metaService;

    @BeforeEach
    void setUp() {
        when(serviceDbFacade.getServiceDbDao()).thenReturn(serviceDbDao);
        when(serviceDbDao.getEntityDao()).thenReturn(entityDao);
        when(serviceDbDao.getDatamartDao()).thenReturn(datamartDao);
        metaService = new DatamartMetaService(serviceDbFacade);
    }

    @Test
    void shouldSuccessDatamartMeta(VertxTestContext testContext) {
        //arrange
        when(datamartDao.getDatamartMeta()).thenReturn(Future.succeededFuture());

        //act
        metaService.getDatamartMeta()
                .onComplete(ar -> testContext.verify(() -> {

                    //assert
                    assertTrue(ar.succeeded());
                }).completeNow());
    }

    @Test
    void shouldFailWhenDatamartMetaError(VertxTestContext testContext) {
        //arrange
        when(datamartDao.getDatamartMeta()).thenReturn(Future.failedFuture(ERROR));

        //act
        metaService.getDatamartMeta()
                .onComplete(ar -> testContext.verify(() -> {

                    //assert
                    assertTrue(ar.failed());
                    assertEquals(ERROR, ar.cause().getMessage());
                }).completeNow());
    }

    @Test
    void shouldSuccessWhenEntitiesMeta(VertxTestContext testContext) {
        //arrange
        val datamartEntities = Lists.newArrayList(new DatamartEntity(1L, ENTITY, DTM));
        when(entityDao.getEntitiesMeta(DTM)).thenReturn(Future.succeededFuture(datamartEntities));

        //act
        metaService.getEntitiesMeta(DTM)
                .onComplete(ar -> testContext.verify(() -> {

                    //assert
                    assertTrue(ar.succeeded());
                }).completeNow());
    }

    @Test
    void shouldFailWhenEntitiesMetaError(VertxTestContext testContext) {
        //arrange
        when(entityDao.getEntitiesMeta(DTM)).thenReturn(Future.failedFuture(ERROR));

        //act
        metaService.getEntitiesMeta(DTM)
                .onComplete(ar -> testContext.verify(() -> {

                    //assert
                    assertTrue(ar.failed());
                    assertEquals(ERROR, ar.cause().getMessage());
                }).completeNow());
    }

    @Test
    void shouldSuccessWhenAttributesMeta(VertxTestContext testContext) {
        //arrange
        when(entityDao.getEntity(DTM, ENTITY)).thenReturn(Future.succeededFuture(getEntity()));
        val expectedAttributesResult = getAttributes();

        //act
        metaService.getAttributesMeta(DTM, ENTITY)
                .onComplete(ar -> testContext.verify(() -> {

                    //assert
                    assertTrue(ar.succeeded());
                    val actualResult = ar.result();
                    assertThat(actualResult).hasSameElementsAs(expectedAttributesResult);

                }).completeNow());
    }

    @Test
    void shouldFailWhenAttributesMetaError(VertxTestContext testContext) {
        //arrange
        when(entityDao.getEntity(DTM, ENTITY)).thenReturn(Future.failedFuture(ERROR));

        //act
        metaService.getAttributesMeta(DTM, ENTITY)
                .onComplete(ar -> testContext.verify(() -> {

                    //assert
                    assertTrue(ar.failed());
                    assertEquals(ERROR, ar.cause().getMessage());

                }).completeNow());
    }

    private Entity getEntity() {
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
        return Entity.builder()
                .name(ENTITY)
                .entityType(EntityType.TABLE)
                .fields(Lists.newArrayList(idCol, descriptionCol, foreignKeyCol, timeCol))
                .destination(EnumSet.of(SourceType.ADB, SourceType.ADQM, SourceType.ADG, SourceType.ADP))
                .build();
    }

    private List<EntityAttribute> getAttributes() {
        return Lists.newArrayList(
                EntityAttribute.builder()
                        .entityMnemonic(ENTITY)
                        .datamartMnemonic(DTM)
                        .ordinalPosition(0)
                        .mnemonic("ID")
                        .dataType(ColumnType.INT)
                        .nullable(false)
                        .primaryKeyOrder(1)
                        .distributeKeykOrder(1)
                        .build(),
                EntityAttribute.builder()
                        .entityMnemonic(ENTITY)
                        .datamartMnemonic(DTM)
                        .ordinalPosition(1)
                        .mnemonic("DESCRIPTION")
                        .dataType(ColumnType.VARCHAR)
                        .length(200)
                        .nullable(false)
                        .build(),
                EntityAttribute.builder()
                        .entityMnemonic(ENTITY)
                        .datamartMnemonic(DTM)
                        .ordinalPosition(2)
                        .mnemonic("FOREIGN_KEY")
                        .dataType(ColumnType.INT)
                        .nullable(false)
                        .build(),
                EntityAttribute.builder()
                        .entityMnemonic(ENTITY)
                        .datamartMnemonic(DTM)
                        .ordinalPosition(3)
                        .mnemonic("TIME_COL")
                        .dataType(ColumnType.TIME)
                        .accuracy(5)
                        .nullable(true)
                        .build()
        );
    }
}