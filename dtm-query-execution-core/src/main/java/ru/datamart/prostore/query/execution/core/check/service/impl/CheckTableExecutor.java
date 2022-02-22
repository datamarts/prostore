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
package ru.datamart.prostore.query.execution.core.check.service.impl;

import ru.datamart.prostore.common.exception.DtmException;
import ru.datamart.prostore.common.model.ddl.EntityType;
import ru.datamart.prostore.common.reader.QueryResult;
import ru.datamart.prostore.query.calcite.core.extension.check.CheckType;
import ru.datamart.prostore.query.calcite.core.extension.check.SqlCheckTable;
import ru.datamart.prostore.query.execution.core.base.repository.zookeeper.EntityDao;
import ru.datamart.prostore.query.execution.core.check.dto.CheckContext;
import ru.datamart.prostore.query.execution.core.check.factory.CheckQueryResultFactory;
import ru.datamart.prostore.query.execution.core.check.service.CheckExecutor;
import io.vertx.core.Future;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

@Service("checkTableExecutor")
public class CheckTableExecutor implements CheckExecutor {

    private final CheckTableService checkTableService;
    private final EntityDao entityDao;
    private final CheckQueryResultFactory queryResultFactory;

    @Autowired
    public CheckTableExecutor(CheckTableService checkTableService,
                              EntityDao entityDao,
                              CheckQueryResultFactory queryResultFactory) {
        this.checkTableService = checkTableService;
        this.entityDao = entityDao;
        this.queryResultFactory = queryResultFactory;
    }

    @Override
    public Future<QueryResult> execute(CheckContext context) {
        String tableName = ((SqlCheckTable) context.getSqlNode()).getTable();
        String datamartMnemonic = context.getRequest().getQueryRequest().getDatamartMnemonic();
        return entityDao.getEntity(datamartMnemonic, tableName)
                .compose(entity -> {
                    if (EntityType.TABLE.equals(entity.getEntityType())) {
                        return Future.succeededFuture(entity);
                    } else {
                        return Future.failedFuture(new DtmException(String.format("%s.%s doesn't exist",
                                datamartMnemonic,
                                tableName)));
                    }
                })
                .compose(entity -> checkTableService.checkEntity(entity, context))
                .map(queryResultFactory::create);
    }

    @Override
    public CheckType getType() {
        return CheckType.TABLE;
    }
}
