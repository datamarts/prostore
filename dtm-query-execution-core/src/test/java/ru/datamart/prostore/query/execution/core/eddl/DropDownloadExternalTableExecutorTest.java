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

import ru.datamart.prostore.common.model.ddl.ColumnType;
import ru.datamart.prostore.common.model.ddl.Entity;
import ru.datamart.prostore.common.model.ddl.EntityField;
import ru.datamart.prostore.common.model.ddl.EntityType;
import ru.datamart.prostore.common.reader.QueryResult;
import ru.datamart.prostore.query.execution.core.base.exception.entity.EntityNotExistsException;
import ru.datamart.prostore.query.execution.core.base.repository.ServiceDbFacade;
import ru.datamart.prostore.query.execution.core.base.repository.ServiceDbFacadeImpl;
import ru.datamart.prostore.query.execution.core.base.repository.zookeeper.DatamartDao;
import ru.datamart.prostore.query.execution.core.base.repository.zookeeper.EntityDao;
import ru.datamart.prostore.query.execution.core.base.repository.zookeeper.ServiceDbDao;
import ru.datamart.prostore.query.execution.core.eddl.dto.DropDownloadExternalTableQuery;
import ru.datamart.prostore.query.execution.core.eddl.service.EddlExecutor;
import ru.datamart.prostore.query.execution.core.eddl.service.download.DropDownloadExternalTableExecutor;
import io.vertx.core.Future;
import io.vertx.core.Promise;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import java.util.Arrays;

import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class DropDownloadExternalTableExecutorTest {

    private final ServiceDbFacade serviceDbFacade = mock(ServiceDbFacadeImpl.class);
    private final ServiceDbDao serviceDbDao = mock(ServiceDbDao.class);
    private final DatamartDao datamartDao = mock(DatamartDao.class);
    private final EntityDao entityDao = mock(EntityDao.class);
    private EddlExecutor dropDownloadExternalTableExecutor;
    private DropDownloadExternalTableQuery query;
    private String schema;
    private String table;
    private Entity entity;
    private Entity entityWithWrongType;

    @BeforeEach
    void setUp() {
        when(serviceDbFacade.getServiceDbDao()).thenReturn(serviceDbDao);
        when(serviceDbDao.getEntityDao()).thenReturn(entityDao);
        when(serviceDbDao.getDatamartDao()).thenReturn(datamartDao);
        dropDownloadExternalTableExecutor = new DropDownloadExternalTableExecutor(serviceDbFacade);
        schema = "shares";
        table = "accounts";
        query = new DropDownloadExternalTableQuery(schema, table);

        EntityField f1 = new EntityField(0, "id", ColumnType.INT, false);
        EntityField f2 = new EntityField(1, "name", ColumnType.VARCHAR, true);
        f2.setSize(20);
        entity = new Entity(table, schema, Arrays.asList(f1, f2));
        entity.setEntityType(EntityType.DOWNLOAD_EXTERNAL_TABLE);

        entityWithWrongType = new Entity(table, schema, Arrays.asList(f1, f2));
        entityWithWrongType.setEntityType(EntityType.UPLOAD_EXTERNAL_TABLE);
    }

    @Test
    void executeSuccess() {
        Promise<QueryResult> promise = Promise.promise();

        Mockito.when(entityDao.getEntity(eq(schema), eq(table)))
                .thenReturn(Future.succeededFuture(entity));

        Mockito.when(entityDao.deleteEntity(eq(schema), eq(table)))
                .thenReturn(Future.succeededFuture());

        dropDownloadExternalTableExecutor.execute(query)
                .onComplete(promise);

        assertTrue(promise.future().succeeded());
    }

    @Test
    void executeTableExistsWithWrongType() {
        Promise<QueryResult> promise = Promise.promise();

        Mockito.when(entityDao.getEntity(eq(schema), eq(table)))
                .thenReturn(Future.succeededFuture(entityWithWrongType));

        dropDownloadExternalTableExecutor.execute(query)
                .onComplete(promise);

        assertTrue(promise.future().failed());
        assertTrue(promise.future().cause() instanceof EntityNotExistsException);
    }

    @Test
    void executeTableNotExists() {
        Promise<QueryResult> promise = Promise.promise();

        Mockito.when(entityDao.getEntity(eq(schema), eq(table)))
                .thenReturn(Future.failedFuture(new EntityNotExistsException("")));

        dropDownloadExternalTableExecutor.execute(query)
                .onComplete(promise);

        assertTrue(promise.future().failed());
        assertTrue(promise.future().cause() instanceof EntityNotExistsException);
    }

    @Test
    void executeDeleteEntityError() {
        Promise<QueryResult> promise = Promise.promise();

        Mockito.when(entityDao.getEntity(eq(schema), eq(table)))
                .thenReturn(Future.succeededFuture(entity));

        Mockito.when(entityDao.deleteEntity(eq(schema), eq(table)))
                .thenReturn(Future.failedFuture("delete entity error"));

        dropDownloadExternalTableExecutor.execute(query)
                .onComplete(promise);

        assertTrue(promise.future().failed());
    }
}
