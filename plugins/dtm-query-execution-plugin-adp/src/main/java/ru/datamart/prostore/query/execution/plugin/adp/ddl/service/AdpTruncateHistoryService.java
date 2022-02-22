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
package ru.datamart.prostore.query.execution.plugin.adp.ddl.service;

import ru.datamart.prostore.common.plugin.sql.PreparedStatementRequest;
import ru.datamart.prostore.query.execution.plugin.adp.db.service.DatabaseExecutor;
import ru.datamart.prostore.query.execution.plugin.adp.ddl.factory.TruncateHistoryFactory;
import ru.datamart.prostore.query.execution.plugin.api.dto.TruncateHistoryRequest;
import ru.datamart.prostore.query.execution.plugin.api.service.ddl.TruncateHistoryService;
import io.vertx.core.Future;
import lombok.AllArgsConstructor;
import lombok.val;
import org.springframework.stereotype.Service;

import java.util.Arrays;

@Service("adpTruncateHistoryService")
@AllArgsConstructor
public class AdpTruncateHistoryService implements TruncateHistoryService {

    private final DatabaseExecutor adpQueryExecutor;
    private final TruncateHistoryFactory queriesFactory;

    @Override
    public Future<Void> truncateHistory(TruncateHistoryRequest request) {
        return request.getSysCn() != null ? executeWithSysCn(request) : execute(request);
    }

    private Future<Void> execute(TruncateHistoryRequest request) {
        val query = queriesFactory.create(request);
        val preparedQuery = PreparedStatementRequest.onlySql(query);
        return adpQueryExecutor.executeInTransaction(Arrays.asList(preparedQuery));
    }

    private Future<Void> executeWithSysCn(TruncateHistoryRequest request) {
        return adpQueryExecutor.execute(queriesFactory.createWithSysCn(request))
                .compose(result -> Future.succeededFuture());
    }

}
