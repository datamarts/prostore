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
package ru.datamart.prostore.query.execution.core.ddl.service.impl;

import ru.datamart.prostore.common.model.ddl.Entity;
import ru.datamart.prostore.common.model.ddl.EntityType;
import ru.datamart.prostore.common.post.PostSqlActionType;
import ru.datamart.prostore.common.reader.QueryResult;
import ru.datamart.prostore.query.calcite.core.extension.OperationNames;
import ru.datamart.prostore.query.calcite.core.extension.ddl.truncate.SqlTruncateHistory;
import ru.datamart.prostore.query.execution.core.base.exception.entity.EntityNotExistsException;
import ru.datamart.prostore.query.execution.core.base.repository.ServiceDbFacade;
import ru.datamart.prostore.query.execution.core.base.repository.zookeeper.EntityDao;
import ru.datamart.prostore.query.execution.core.base.service.metadata.MetadataExecutor;
import ru.datamart.prostore.query.execution.core.ddl.dto.DdlRequestContext;
import ru.datamart.prostore.query.execution.core.ddl.service.QueryResultDdlExecutor;
import ru.datamart.prostore.query.execution.core.delta.repository.zookeeper.DeltaServiceDao;
import ru.datamart.prostore.query.execution.core.plugin.service.DataSourcePluginService;
import ru.datamart.prostore.query.execution.plugin.api.dto.TruncateHistoryRequest;
import io.vertx.core.CompositeFuture;
import io.vertx.core.Future;
import lombok.extern.slf4j.Slf4j;
import lombok.val;
import org.apache.calcite.sql.SqlDialect;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Component;

import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

@Slf4j
@Component
public class TruncateExecutor extends QueryResultDdlExecutor {

    private final DataSourcePluginService dataSourcePluginService;
    private final EntityDao entityDao;
    private final DeltaServiceDao deltaServiceDao;

    @Autowired
    public TruncateExecutor(MetadataExecutor metadataExecutor,
                            ServiceDbFacade serviceDbFacade,
                            @Qualifier("coreSqlDialect") SqlDialect sqlDialect,
                            DataSourcePluginService dataSourcePluginService,
                            DeltaServiceDao deltaServiceDao) {
        super(metadataExecutor, serviceDbFacade, sqlDialect);
        this.dataSourcePluginService = dataSourcePluginService;
        this.deltaServiceDao = deltaServiceDao;
        this.entityDao = serviceDbFacade.getServiceDbDao().getEntityDao();
    }

    @Override
    public Future<QueryResult> execute(DdlRequestContext context, String sqlNodeName) {
        return Future.future(promise -> {
            val schema = getSchemaName(context.getDatamartName(), sqlNodeName);
            context.setDatamartName(schema);
            val table = getTableName(sqlNodeName);
            val sqlTruncateHistory = (SqlTruncateHistory) context.getSqlCall();
            CompositeFuture.join(getTableEntity(schema, table), calcSysCn(schema, sqlTruncateHistory))
                    .compose(entitySysCn -> CompositeFuture.join(executeTruncate(entitySysCn, context, sqlTruncateHistory)))
                    .onSuccess(success -> promise.complete(QueryResult.emptyResult()))
                    .onFailure(promise::fail);
        });
    }

    private List<Future> executeTruncate(CompositeFuture entitySysCn, DdlRequestContext context, SqlTruncateHistory sqlTruncateHistory) {
        val entity = (Entity) entitySysCn.resultAt(0);
        val sysCn = (Long) entitySysCn.resultAt(1);
        return entity.getDestination().stream()
                .map(sourceType -> {
                    val truncateHistoryRequest = TruncateHistoryRequest.builder()
                            .datamartMnemonic(context.getDatamartName())
                            .conditions(sqlTruncateHistory.getConditions())
                            .entity(entity)
                            .envName(context.getEnvName())
                            .requestId(context.getRequest().getQueryRequest().getRequestId())
                            .sysCn(sysCn)
                            .build();
                    return dataSourcePluginService.truncateHistory(sourceType, context.getMetrics(), truncateHistoryRequest);
                })
                .collect(Collectors.toList());
    }

    private Future<Entity> getTableEntity(String schema, String table) {
        return Future.future(p -> entityDao.getEntity(schema, table)
                .onSuccess(entity -> {
                    if (EntityType.TABLE.equals(entity.getEntityType())) {
                        p.complete(entity);
                    } else {
                        p.fail(new EntityNotExistsException(schema, table));
                    }
                })
                .onFailure(p::fail));
    }

    private Future<Long> calcSysCn(String schema, SqlTruncateHistory truncateHistory) {
        return Future.future(p -> {
            if (truncateHistory.isInfinite()) {
                p.complete();
            } else {
                deltaServiceDao.getDeltaByDateTime(schema, truncateHistory.getDateTime())
                        .onSuccess(delta -> p.complete(delta.getCnTo()))
                        .onFailure(p::fail);
            }
        });
    }

    @Override
    public String getOperationKind() {
        return OperationNames.TRUNCATE_HISTORY;
    }

    @Override
    public List<PostSqlActionType> getPostActions() {
        return Collections.singletonList(PostSqlActionType.PUBLISH_STATUS);
    }
}
