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
package ru.datamart.prostore.query.execution.plugin.adqm.rollback.service;

import ru.datamart.prostore.common.plugin.sql.PreparedStatementRequest;
import ru.datamart.prostore.query.execution.plugin.adqm.query.service.AdqmQueryExecutor;
import ru.datamart.prostore.query.execution.plugin.adqm.rollback.dto.AdqmRollbackRequest;
import ru.datamart.prostore.query.execution.plugin.api.dto.RollbackRequest;
import ru.datamart.prostore.query.execution.plugin.api.factory.RollbackRequestFactory;
import ru.datamart.prostore.query.execution.plugin.api.service.RollbackService;
import io.vertx.core.Future;
import lombok.extern.slf4j.Slf4j;
import lombok.val;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

@Slf4j
@Service("adqmRollbackService")
public class AdqmRollbackService implements RollbackService<Void> {
    private final RollbackRequestFactory<AdqmRollbackRequest> rollbackRequestFactory;
    private final AdqmQueryExecutor adqmQueryExecutor;

    @Autowired
    public AdqmRollbackService(RollbackRequestFactory<AdqmRollbackRequest> rollbackRequestFactory,
                               AdqmQueryExecutor adqmQueryExecutor) {
        this.rollbackRequestFactory = rollbackRequestFactory;
        this.adqmQueryExecutor = adqmQueryExecutor;
    }

    @Override
    public Future<Void> execute(RollbackRequest request) {
        return Future.future(promise -> {
            val rollbackRequest = rollbackRequestFactory.create(request);
            Future<Void> executingFuture = Future.succeededFuture();
            for (PreparedStatementRequest statement : rollbackRequest.getStatements()) {
                executingFuture = executingFuture.compose(v -> adqmQueryExecutor.executeUpdate(statement.getSql()));
            }
            executingFuture.onSuccess(success -> promise.complete())
                    .onFailure(promise::fail);
        });
    }
}
