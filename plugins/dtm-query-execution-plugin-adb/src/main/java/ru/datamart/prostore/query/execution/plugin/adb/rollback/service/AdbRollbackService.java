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
package ru.datamart.prostore.query.execution.plugin.adb.rollback.service;

import ru.datamart.prostore.query.execution.plugin.adb.query.service.DatabaseExecutor;
import ru.datamart.prostore.query.execution.plugin.adb.rollback.dto.AdbRollbackRequest;
import ru.datamart.prostore.query.execution.plugin.api.dto.RollbackRequest;
import ru.datamart.prostore.query.execution.plugin.api.factory.RollbackRequestFactory;
import ru.datamart.prostore.query.execution.plugin.api.service.RollbackService;
import io.vertx.core.Future;
import lombok.extern.slf4j.Slf4j;
import lombok.val;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Service;

@Slf4j
@Service("adbRollbackService")
public class AdbRollbackService implements RollbackService<Void> {

    private final RollbackRequestFactory<AdbRollbackRequest> rollbackRequestFactory;
    private final DatabaseExecutor adbQueryExecutor;

    @Autowired
    public AdbRollbackService(RollbackRequestFactory<AdbRollbackRequest> rollbackRequestFactory,
                              @Qualifier("adbQueryExecutor") DatabaseExecutor adbQueryExecutor) {
        this.rollbackRequestFactory = rollbackRequestFactory;
        this.adbQueryExecutor = adbQueryExecutor;
    }

    @Override
    public Future<Void> execute(RollbackRequest request) {
        return Future.future(promise -> {
            val rollbackRequest = rollbackRequestFactory.create(request);
            adbQueryExecutor.executeUpdate(rollbackRequest.getTruncate().getSql())
                    .compose(v -> adbQueryExecutor.executeUpdate(rollbackRequest.getDeleteFromActual().getSql()))
                    .compose(v -> adbQueryExecutor.executeInTransaction(rollbackRequest.getEraseOps()))
                    .onSuccess(success -> promise.complete())
                    .onFailure(promise::fail);
        });
    }
}
