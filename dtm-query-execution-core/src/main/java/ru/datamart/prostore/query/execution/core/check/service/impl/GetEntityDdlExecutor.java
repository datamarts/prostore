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
package ru.datamart.prostore.query.execution.core.check.service.impl;

import ru.datamart.prostore.common.exception.DtmException;
import ru.datamart.prostore.common.model.ddl.Entity;
import ru.datamart.prostore.common.model.ddl.EntityType;
import ru.datamart.prostore.common.reader.InformationSchemaView;
import ru.datamart.prostore.common.reader.QueryResult;
import ru.datamart.prostore.query.calcite.core.extension.check.CheckType;
import ru.datamart.prostore.query.calcite.core.extension.check.SqlGetEntityDdl;
import ru.datamart.prostore.query.execution.core.base.exception.entity.EntityNotExistsException;
import ru.datamart.prostore.query.execution.core.base.repository.zookeeper.EntityDao;
import ru.datamart.prostore.query.execution.core.base.service.metadata.query.DdlQueryGenerator;
import ru.datamart.prostore.query.execution.core.check.dto.CheckContext;
import ru.datamart.prostore.query.execution.core.check.factory.CheckEntityDdlResultFactory;
import ru.datamart.prostore.query.execution.core.check.service.CheckExecutor;
import io.vertx.core.Future;
import lombok.val;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Service;

@Service("getEntityDdlExecutor")
public class GetEntityDdlExecutor implements CheckExecutor {
    private final CheckEntityDdlResultFactory resultFactory;
    private final EntityDao entityDao;
    private final DdlQueryGenerator ddlQueryGenerator;

    public GetEntityDdlExecutor(EntityDao entityDao,
                                CheckEntityDdlResultFactory resultFactory,
                                @Qualifier("coreDdlQueryGenerator") DdlQueryGenerator ddlQueryGenerator) {
        this.resultFactory = resultFactory;
        this.entityDao = entityDao;
        this.ddlQueryGenerator = ddlQueryGenerator;
    }

    @Override
    public Future<QueryResult> execute(CheckContext context) {
        return Future.future(promise -> {
            val sqlNode = (SqlGetEntityDdl) context.getSqlNode();
            val datamart = context.getRequest().getQueryRequest().getDatamartMnemonic();
            entityDao.getEntity(datamart, sqlNode.getEntity())
                    .map(entity -> {
                        checkNotInformationSchemaView(entity);
                        return createQueryResultByEntityType(entity, entity.getEntityType());
                    })
                    .onComplete(promise);
        });
    }

    private QueryResult createQueryResultByEntityType(Entity entity, EntityType type) {
        switch (type) {
            case TABLE:
                return resultFactory.create(ddlQueryGenerator.generateCreateTableQuery(entity));
            case MATERIALIZED_VIEW:
                return resultFactory.create(ddlQueryGenerator.generateCreateMaterializedView(entity));
            case VIEW:
                return resultFactory.create(ddlQueryGenerator.generateCreateViewQuery(entity, ""));
            default:
                throw new DtmException(String.format("%s.%s doesn't exist", entity.getSchema(), entity.getName()));
        }
    }

    private void checkNotInformationSchemaView(Entity entity) {
        if (InformationSchemaView.isInformationSchemaView(entity.getName())) {
            throw new EntityNotExistsException(entity.getName());
        }
    }

    @Override
    public CheckType getType() {
        return CheckType.ENTITY_DDL;
    }
}
