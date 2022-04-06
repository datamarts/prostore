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
package ru.datamart.prostore.query.execution.core.eddl.service.standalone;

import io.vertx.core.Future;
import org.springframework.stereotype.Component;
import ru.datamart.prostore.cache.service.EvictQueryTemplateCacheService;
import ru.datamart.prostore.common.exception.DtmException;
import ru.datamart.prostore.common.model.ddl.Entity;
import ru.datamart.prostore.common.model.ddl.EntityType;
import ru.datamart.prostore.query.execution.core.base.repository.zookeeper.EntityDao;
import ru.datamart.prostore.query.execution.core.ddl.service.impl.validate.RelatedViewChecker;
import ru.datamart.prostore.query.execution.core.eddl.dto.EddlAction;
import ru.datamart.prostore.query.execution.core.plugin.service.DataSourcePluginService;

@Component
public class DropReadableExternalTableExecutor extends DropStandaloneExternalTableExecutor {

    private final RelatedViewChecker relatedViewChecker;

    public DropReadableExternalTableExecutor(DataSourcePluginService dataSourcePluginService,
                                             EntityDao entityDao,
                                             EvictQueryTemplateCacheService evictQueryTemplateCacheService,
                                             RelatedViewChecker relatedViewChecker,
                                             UpdateInfoSchemaStandalonePostExecutor postExecutor) {
        super(dataSourcePluginService, entityDao, evictQueryTemplateCacheService, postExecutor);
        this.relatedViewChecker = relatedViewChecker;
    }

    @Override
    protected Future<Entity> validateEntity(Entity entity) {
        if (!EntityType.READABLE_EXTERNAL_TABLE.equals(entity.getEntityType())) {
            return Future.failedFuture(new DtmException(String.format("Readable external table %s does not exist", entity.getNameWithSchema())));
        }

        return relatedViewChecker.checkRelatedViews(entity, null)
                .map(v -> entity);
    }

    @Override
    public EddlAction getAction() {
        return EddlAction.DROP_READABLE_EXTERNAL_TABLE;
    }
}
