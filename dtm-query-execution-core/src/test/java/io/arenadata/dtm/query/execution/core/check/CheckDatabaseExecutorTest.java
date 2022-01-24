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

import io.arenadata.dtm.common.metrics.RequestMetrics;
import io.arenadata.dtm.common.model.ddl.Entity;
import io.arenadata.dtm.common.model.ddl.EntityType;
import io.arenadata.dtm.common.reader.QueryRequest;
import io.arenadata.dtm.common.reader.SourceType;
import io.arenadata.dtm.common.request.DatamartRequest;
import io.arenadata.dtm.query.calcite.core.extension.check.CheckType;
import io.arenadata.dtm.query.execution.core.base.repository.zookeeper.DatamartDao;
import io.arenadata.dtm.query.execution.core.base.repository.zookeeper.EntityDao;
import io.arenadata.dtm.query.execution.core.check.dto.CheckContext;
import io.arenadata.dtm.query.execution.core.check.factory.CheckQueryResultFactory;
import io.arenadata.dtm.query.execution.core.check.service.impl.CheckDatabaseExecutor;
import io.arenadata.dtm.query.execution.core.check.service.impl.CheckTableService;
import io.arenadata.dtm.query.execution.core.plugin.service.DataSourcePluginService;
import io.arenadata.dtm.query.execution.core.plugin.service.impl.DataSourcePluginServiceImpl;
import io.vertx.core.Future;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;

class CheckDatabaseExecutorTest {
    private final static String DATAMART_MNEMONIC = "schema";
    private final static Set<SourceType> SOURCE_TYPES = Stream.of(SourceType.ADB, SourceType.ADG, SourceType.ADQM)
            .collect(Collectors.toSet());

    private final DataSourcePluginService dataSourcePluginService = mock(DataSourcePluginServiceImpl.class);
    private final EntityDao entityDao = mock(EntityDao.class);
    private final DatamartDao datamartDao = mock(DatamartDao.class);
    private final CheckQueryResultFactory queryResultFactory = mock(CheckQueryResultFactory.class);
    private final CheckTableService checkTableService = mock(CheckTableService.class);
    private final CheckDatabaseExecutor checkDatabaseExecutor = new CheckDatabaseExecutor(
            entityDao, datamartDao, queryResultFactory, checkTableService);

    @BeforeEach
    void setUp() {
        when(dataSourcePluginService.getSourceTypes()).thenReturn(SOURCE_TYPES);
        when(dataSourcePluginService.checkTable(any(), any(), any())).thenReturn(Future.succeededFuture());
        when(datamartDao.getDatamart(DATAMART_MNEMONIC)).thenReturn(Future.succeededFuture(new byte[10]));
        when(checkTableService.checkEntity(any(), any())).thenReturn(Future.succeededFuture("Table is ok"));
        Entity entity1 = Entity.builder()
                .schema(DATAMART_MNEMONIC)
                .entityType(EntityType.TABLE)
                .destination(SOURCE_TYPES)
                .name("entity1")
                .build();
        Entity entity2 = entity1.toBuilder()
                .name("entity2")
                .build();
        when(entityDao.getEntityNamesByDatamart(DATAMART_MNEMONIC))
                .thenReturn(Future.succeededFuture(Arrays.asList(entity1.getName(), entity2.getName())));
        when(entityDao.getEntity(DATAMART_MNEMONIC, entity1.getName()))
                .thenReturn(Future.succeededFuture(entity1));
        when(entityDao.getEntity(DATAMART_MNEMONIC, entity2.getName()))
                .thenReturn(Future.succeededFuture(entity2));
    }

    @Test
    void testSuccess() {
        QueryRequest queryRequest = new QueryRequest();
        queryRequest.setDatamartMnemonic(DATAMART_MNEMONIC);
        CheckContext checkContext = new CheckContext(new RequestMetrics(), "env",
                new DatamartRequest(queryRequest), CheckType.DATABASE, null);
        checkDatabaseExecutor.execute(checkContext)
                .onComplete(ar -> assertTrue(ar.succeeded()));
        verify(checkTableService, times(2)).checkEntity(any(), any());
    }
}
