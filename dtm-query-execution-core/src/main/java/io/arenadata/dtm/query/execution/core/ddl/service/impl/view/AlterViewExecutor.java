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
package io.arenadata.dtm.query.execution.core.ddl.service.impl.view;

import io.arenadata.dtm.cache.service.CacheService;
import io.arenadata.dtm.common.model.ddl.Entity;
import io.arenadata.dtm.common.reader.QueryResult;
import io.arenadata.dtm.query.calcite.core.extension.OperationNames;
import io.arenadata.dtm.query.calcite.core.extension.ddl.SqlAlterView;
import io.arenadata.dtm.query.calcite.core.rel2sql.DtmRelToSqlConverter;
import io.arenadata.dtm.query.calcite.core.service.QueryParserService;
import io.arenadata.dtm.query.execution.core.base.dto.cache.EntityKey;
import io.arenadata.dtm.query.execution.core.base.repository.ServiceDbFacade;
import io.arenadata.dtm.query.execution.core.base.repository.zookeeper.SetEntityState;
import io.arenadata.dtm.query.execution.core.base.service.metadata.LogicalSchemaProvider;
import io.arenadata.dtm.query.execution.core.base.service.metadata.MetadataExecutor;
import io.arenadata.dtm.query.execution.core.ddl.dto.DdlRequestContext;
import io.arenadata.dtm.query.execution.core.delta.dto.OkDelta;
import io.arenadata.dtm.query.execution.core.dml.service.ColumnMetadataService;
import io.vertx.core.Future;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import lombok.val;
import org.apache.calcite.sql.SqlDialect;
import org.apache.calcite.sql.SqlNode;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Component;

import static io.arenadata.dtm.query.execution.core.ddl.utils.ValidationUtils.checkTimestampFormat;

@Slf4j
@Component
public class AlterViewExecutor extends CreateViewExecutor {

    @Autowired
    public AlterViewExecutor(MetadataExecutor metadataExecutor,
                             ServiceDbFacade serviceDbFacade,
                             @Qualifier("coreSqlDialect") SqlDialect sqlDialect,
                             @Qualifier("entityCacheService") CacheService<EntityKey, Entity> entityCacheService,
                             LogicalSchemaProvider logicalSchemaProvider,
                             ColumnMetadataService columnMetadataService,
                             @Qualifier("coreCalciteDMLQueryParserService") QueryParserService parserService,
                             @Qualifier("coreRelToSqlConverter") DtmRelToSqlConverter relToSqlConverter) {
        super(metadataExecutor,
                serviceDbFacade,
                sqlDialect,
                entityCacheService,
                logicalSchemaProvider,
                columnMetadataService,
                parserService,
                relToSqlConverter);
    }

    @Override
    public Future<QueryResult> execute(DdlRequestContext context, String sqlNodeName) {
        return checkViewQuery(context)
                .compose(v -> parseSelect(((SqlAlterView) context.getSqlNode()).getQuery(), context.getDatamartName()))
                .map(parserResponse -> {
                    checkTimestampFormat(parserResponse.getSqlNode());
                    return parserResponse;
                })
                .compose(response -> getCreateViewContext(context, response))
                .compose(viewContext -> writeViewChangelodRecord(viewContext)
                        .compose(delta -> updateEntity(viewContext, context, delta)));
    }

    @Override
    protected Future<Entity> checkEntity(Entity generatedEntity, boolean orReplace) {
        return super.checkEntity(generatedEntity, true);
    }

    private Future<QueryResult> updateEntity(CreateViewContext viewContext, DdlRequestContext context, OkDelta deltaOk) {
        return Future.future(promise -> {
            val viewEntity = viewContext.getViewEntity();
            context.setDatamartName(viewEntity.getSchema());
            entityDao.getEntity(viewEntity.getSchema(), viewEntity.getName())
                    .compose(this::checkEntityType)
                    .compose(r -> entityDao.setEntityState(viewEntity, deltaOk, viewContext.getChangeQuery(), SetEntityState.UPDATE))
                    .onSuccess(success -> promise.complete(QueryResult.emptyResult()))
                    .onFailure(promise::fail);
        });
    }

    @SneakyThrows
    @Override
    protected void replaceSqlSelectQuery(DdlRequestContext context, boolean replace, SqlNode newSelectNode) {
        val sql = (SqlAlterView) context.getSqlNode();
        val newSql = new SqlAlterView(sql.getParserPosition(), sql.getName(), sql.getColumnList(), newSelectNode);
        context.setSqlNode(newSql);
    }

    @Override
    public String getOperationKind() {
        return OperationNames.ALTER_VIEW;
    }

}
