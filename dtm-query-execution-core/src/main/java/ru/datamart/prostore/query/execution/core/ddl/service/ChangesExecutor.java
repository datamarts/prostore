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
package ru.datamart.prostore.query.execution.core.ddl.service;

import ru.datamart.prostore.common.post.PostSqlActionType;
import ru.datamart.prostore.common.reader.QueryResult;
import ru.datamart.prostore.query.calcite.core.extension.ddl.SqlChanges;
import ru.datamart.prostore.query.execution.core.base.repository.zookeeper.ChangesDao;
import ru.datamart.prostore.query.execution.core.ddl.dto.DdlRequestContext;
import io.vertx.core.Future;
import lombok.val;

import java.util.Collections;
import java.util.List;

public abstract class ChangesExecutor implements DdlExecutor {

    protected ChangesDao changesDao;

    protected ChangesExecutor(ChangesDao changesDao) {
        this.changesDao = changesDao;
    }

    @Override
    public Future<QueryResult> execute(DdlRequestContext context, String sqlNodeName) {
        return Future.future(promise -> {
            val sqlDenyChanges = (SqlChanges) context.getSqlCall();
            val datamart = sqlDenyChanges.getIdentifier() == null ?
                    context.getRequest().getQueryRequest().getDatamartMnemonic() :
                    sqlDenyChanges.getIdentifier().getSimple();
            val denyCode = sqlDenyChanges.getDenyCode() == null ? "" :
                    sqlDenyChanges.getDenyCode().getNlsString().getValue();
            runOperation(datamart, denyCode)
                    .map(v -> QueryResult.emptyResult())
                    .onComplete(promise);
        });
    }

    protected abstract Future<Void> runOperation(String datamart, String denyCode);

    @Override
    public List<PostSqlActionType> getPostActions() {
        return Collections.emptyList();
    }
}
