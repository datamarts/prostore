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

import io.vertx.core.Future;
import lombok.val;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import ru.datamart.prostore.common.reader.QueryResult;
import ru.datamart.prostore.query.calcite.core.extension.check.CheckType;
import ru.datamart.prostore.query.execution.core.base.exception.datamart.DatamartNotExistsException;
import ru.datamart.prostore.query.execution.core.base.repository.zookeeper.ChangelogDao;
import ru.datamart.prostore.query.execution.core.base.repository.zookeeper.DatamartDao;
import ru.datamart.prostore.query.execution.core.check.dto.CheckContext;
import ru.datamart.prostore.query.execution.core.check.factory.ChangesResultFactory;
import ru.datamart.prostore.query.execution.core.check.service.CheckExecutor;

@Service
public class GetChangesExecutor implements CheckExecutor {

    private final DatamartDao datamartDao;
    private final ChangelogDao changelogDao;

    @Autowired
    public GetChangesExecutor(DatamartDao datamartDao,
                              ChangelogDao changelogDao) {
        this.datamartDao = datamartDao;
        this.changelogDao = changelogDao;
    }

    @Override
    public Future<QueryResult> execute(CheckContext context) {
        val datamart = context.getSqlNode().getSchema() == null ?
                context.getRequest().getQueryRequest().getDatamartMnemonic() :
                context.getSqlNode().getSchema();
        return datamartDao.existsDatamart(datamart)
                .compose(datamartExist -> datamartExist ? changelogDao.getChanges(datamart) : Future.failedFuture(new DatamartNotExistsException(datamart)))
                .map(ChangesResultFactory::getQueryResult);
    }

    @Override
    public CheckType getType() {
        return CheckType.CHANGES;
    }
}
