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
package io.arenadata.dtm.query.execution.core.check.service.impl;

import io.arenadata.dtm.common.exception.DtmException;
import io.arenadata.dtm.common.model.ddl.EntityType;
import io.arenadata.dtm.common.reader.QueryResult;
import io.arenadata.dtm.query.calcite.core.extension.check.CheckType;
import io.arenadata.dtm.query.calcite.core.extension.check.SqlCheckTable;
import io.arenadata.dtm.query.execution.core.base.repository.zookeeper.EntityDao;
import io.arenadata.dtm.query.execution.core.check.dto.CheckContext;
import io.arenadata.dtm.query.execution.core.check.factory.CheckQueryResultFactory;
import io.arenadata.dtm.query.execution.core.check.service.CheckExecutor;
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
