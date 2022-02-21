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
package ru.datamart.prostore.query.execution.core.ddl.service.impl.matview;

import ru.datamart.prostore.cache.service.CacheService;
import ru.datamart.prostore.cache.service.EvictQueryTemplateCacheService;
import ru.datamart.prostore.common.model.ddl.Entity;
import ru.datamart.prostore.common.model.ddl.EntityType;
import ru.datamart.prostore.common.reader.QueryResult;
import ru.datamart.prostore.common.reader.SourceType;
import ru.datamart.prostore.query.calcite.core.extension.OperationNames;
import ru.datamart.prostore.query.calcite.core.extension.ddl.SqlDropMaterializedView;
import ru.datamart.prostore.query.execution.core.base.dto.cache.EntityKey;
import ru.datamart.prostore.query.execution.core.base.dto.cache.MaterializedViewCacheValue;
import ru.datamart.prostore.query.execution.core.base.repository.ServiceDbFacade;
import ru.datamart.prostore.query.execution.core.base.service.hsql.HSQLClient;
import ru.datamart.prostore.query.execution.core.base.service.metadata.MetadataExecutor;
import ru.datamart.prostore.query.execution.core.ddl.dto.DdlRequestContext;
import ru.datamart.prostore.query.execution.core.ddl.service.impl.table.DropTableExecutor;
import ru.datamart.prostore.query.execution.core.plugin.service.DataSourcePluginService;
import io.vertx.core.Future;
import lombok.val;
import org.apache.calcite.sql.SqlDialect;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Component;

@Component
public class DropMaterializedViewExecutor extends DropTableExecutor {

    private final CacheService<EntityKey, MaterializedViewCacheValue> materializedViewCacheService;

    public DropMaterializedViewExecutor(MetadataExecutor metadataExecutor,
                                        ServiceDbFacade serviceDbFacade,
                                        @Qualifier("coreSqlDialect") SqlDialect sqlDialect,
                                        @Qualifier("entityCacheService") CacheService<EntityKey, Entity> entityCacheService,
                                        DataSourcePluginService dataSourcePluginService,
                                        @Qualifier("materializedViewCacheService") CacheService<EntityKey, MaterializedViewCacheValue> materializedViewCacheService,
                                        HSQLClient hsqlClient,
                                        EvictQueryTemplateCacheService evictQueryTemplateCacheService) {
        super(metadataExecutor,
                serviceDbFacade,
                sqlDialect,
                entityCacheService,
                dataSourcePluginService,
                hsqlClient,
                evictQueryTemplateCacheService);
        this.materializedViewCacheService = materializedViewCacheService;
    }

    @Override
    public Future<QueryResult> execute(DdlRequestContext context, String sqlNodeName) {
        val datamartName = getSchemaName(context.getDatamartName(), sqlNodeName);
        val tableName = getTableName(sqlNodeName);

        return super.execute(context, sqlNodeName)
                .onSuccess(ar -> {
                    val cacheValue = materializedViewCacheService.get(new EntityKey(datamartName, tableName));
                    if (cacheValue != null) {
                        cacheValue.markForDeletion();
                    }
                });
    }

    @Override
    protected Entity createClassTable(String schema, String tableName) {
        return Entity.builder()
                .schema(schema)
                .name(tableName)
                .entityType(EntityType.MATERIALIZED_VIEW)
                .build();
    }

    @Override
    protected SourceType getSourceType(DdlRequestContext context) {
        return ((SqlDropMaterializedView) context.getSqlNode()).getDestination().getValue();
    }

    @Override
    public String getOperationKind() {
        return OperationNames.DROP_MATERIALIZED_VIEW;
    }
}
