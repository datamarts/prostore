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
package ru.datamart.prostore.query.execution.core.ddl.service.impl;

import io.vertx.core.Future;
import lombok.val;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;
import ru.datamart.prostore.common.post.PostSqlActionType;
import ru.datamart.prostore.common.reader.QueryResult;
import ru.datamart.prostore.query.calcite.core.extension.OperationNames;
import ru.datamart.prostore.query.calcite.core.extension.ddl.EraseChangeOperation;
import ru.datamart.prostore.query.execution.core.base.exception.datamart.DatamartNotExistsException;
import ru.datamart.prostore.query.execution.core.base.repository.zookeeper.ChangelogDao;
import ru.datamart.prostore.query.execution.core.base.repository.zookeeper.DatamartDao;
import ru.datamart.prostore.query.execution.core.check.factory.ChangesResultFactory;
import ru.datamart.prostore.query.execution.core.ddl.dto.DdlRequestContext;
import ru.datamart.prostore.query.execution.core.ddl.service.DdlExecutor;

import java.util.Collections;
import java.util.List;

@Component
public class EraseChangeOperationExecutor implements DdlExecutor {

    private final DatamartDao datamartDao;
    private final ChangelogDao changelogDao;

    @Autowired
    public EraseChangeOperationExecutor(DatamartDao datamartDao, ChangelogDao changelogDao) {
        this.datamartDao = datamartDao;
        this.changelogDao = changelogDao;
    }

    @Override
    public Future<QueryResult> execute(DdlRequestContext context, String sqlNodeName) {
        val eraseChangeOperation = (EraseChangeOperation) context.getSqlNode();
        val datamart = eraseChangeOperation.getDatamart() == null ?
                context.getRequest().getQueryRequest().getDatamartMnemonic() :
                eraseChangeOperation.getDatamart();
        val eraseOperationNum = eraseChangeOperation.getChangeOperationNumber();
        return datamartDao.existsDatamart(datamart)
                .compose(datamartExist -> datamartExist ? changelogDao.eraseChangeOperation(datamart, eraseOperationNum) : Future.failedFuture(new DatamartNotExistsException(datamart)))
                .map(ChangesResultFactory::getQueryResult);
    }

    @Override
    public String getOperationKind() {
        return OperationNames.ERASE_CHANGE_OPERATION;
    }

    @Override
    public List<PostSqlActionType> getPostActions() {
        return Collections.emptyList();
    }
}
