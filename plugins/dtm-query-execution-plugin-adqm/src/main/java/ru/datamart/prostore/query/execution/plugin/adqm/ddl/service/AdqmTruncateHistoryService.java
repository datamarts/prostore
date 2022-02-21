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
package ru.datamart.prostore.query.execution.plugin.adqm.ddl.service;

import ru.datamart.prostore.query.execution.plugin.adqm.ddl.factory.AdqmTruncateHistoryQueriesFactory;
import ru.datamart.prostore.query.execution.plugin.adqm.factory.AdqmProcessingSqlFactory;
import ru.datamart.prostore.query.execution.plugin.adqm.query.service.DatabaseExecutor;
import ru.datamart.prostore.query.execution.plugin.api.dto.TruncateHistoryRequest;
import ru.datamart.prostore.query.execution.plugin.api.service.ddl.TruncateHistoryService;
import io.vertx.core.Future;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

@Service("adqmTruncateHistoryService")
public class AdqmTruncateHistoryService implements TruncateHistoryService {

    private final DatabaseExecutor adqmQueryExecutor;
    private final AdqmTruncateHistoryQueriesFactory queriesFactory;
    private final AdqmProcessingSqlFactory adqmProcessingSqlFactory;

    @Autowired
    public AdqmTruncateHistoryService(DatabaseExecutor adqmQueryExecutor,
                                      AdqmTruncateHistoryQueriesFactory queriesFactory,
                                      AdqmProcessingSqlFactory adqmProcessingSqlFactory) {
        this.adqmQueryExecutor = adqmQueryExecutor;
        this.queriesFactory = queriesFactory;
        this.adqmProcessingSqlFactory = adqmProcessingSqlFactory;
    }

    @Override
    public Future<Void> truncateHistory(TruncateHistoryRequest request) {
        return adqmQueryExecutor.execute(queriesFactory.insertIntoActualQuery(request))
                .compose(result -> adqmQueryExecutor.execute(adqmProcessingSqlFactory.getFlushActualSql(request.getEnvName(), request.getEntity().getSchema(), request.getEntity().getName())))
                .compose(result -> adqmQueryExecutor.execute(adqmProcessingSqlFactory.getOptimizeActualSql(request.getEnvName(), request.getEntity().getSchema(), request.getEntity().getName())))
                .compose(result -> Future.succeededFuture());
    }
}
