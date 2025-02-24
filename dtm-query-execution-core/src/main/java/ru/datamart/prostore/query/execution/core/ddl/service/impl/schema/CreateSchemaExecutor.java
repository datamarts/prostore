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
package ru.datamart.prostore.query.execution.core.ddl.service.impl.schema;

import ru.datamart.prostore.common.reader.QueryResult;
import ru.datamart.prostore.query.calcite.core.extension.OperationNames;
import ru.datamart.prostore.query.calcite.core.extension.eddl.SqlCreateDatabase;
import ru.datamart.prostore.query.execution.core.base.exception.datamart.DatamartAlreadyExistsException;
import ru.datamart.prostore.query.execution.core.base.repository.ServiceDbFacade;
import ru.datamart.prostore.query.execution.core.base.repository.zookeeper.DatamartDao;
import ru.datamart.prostore.query.execution.core.base.service.metadata.MetadataExecutor;
import ru.datamart.prostore.query.execution.core.ddl.dto.DdlRequestContext;
import ru.datamart.prostore.query.execution.core.ddl.service.QueryResultDdlExecutor;
import ru.datamart.prostore.query.execution.core.ddl.utils.ValidationUtils;
import io.vertx.core.Future;
import lombok.extern.slf4j.Slf4j;
import org.apache.calcite.sql.SqlDialect;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Component;

import static ru.datamart.prostore.query.execution.core.ddl.dto.DdlType.CREATE_SCHEMA;

@Slf4j
@Component
public class CreateSchemaExecutor extends QueryResultDdlExecutor {

    private final DatamartDao datamartDao;

    @Autowired
    public CreateSchemaExecutor(MetadataExecutor metadataExecutor,
                                ServiceDbFacade serviceDbFacade,
                                @Qualifier("coreSqlDialect") SqlDialect sqlDialect) {
        super(metadataExecutor, serviceDbFacade, sqlDialect);
        datamartDao = serviceDbFacade.getServiceDbDao().getDatamartDao();
    }

    @Override
    public Future<QueryResult> execute(DdlRequestContext context, String sqlNodeName) {
        return Future.future(promise -> {
            String datamartName = ((SqlCreateDatabase) context.getSqlNode()).getName().getSimple();
            context.getRequest().setQueryRequest(replaceDatabaseInSql(context.getRequest().getQueryRequest()));
            context.setDdlType(CREATE_SCHEMA);
            context.setDatamartName(datamartName);

            ValidationUtils.checkName(datamartName);

            datamartDao.existsDatamart(datamartName)
                    .compose(isExists -> isExists ? getDatamartAlreadyExistsFuture(datamartName) : executeRequest(context))
                    .compose(v -> datamartDao.createDatamart(datamartName))
                    .onSuccess(success -> {
                        log.debug("Datamart [{}] successfully created", datamartName);
                        promise.complete(QueryResult.emptyResult());
                    })
                    .onFailure(promise::fail);
        });
    }

    private Future<Void> getDatamartAlreadyExistsFuture(String datamartName) {
        return Future.failedFuture(new DatamartAlreadyExistsException(datamartName));
    }

    @Override
    public String getOperationKind() {
        return OperationNames.CREATE_DATABASE;
    }
}
