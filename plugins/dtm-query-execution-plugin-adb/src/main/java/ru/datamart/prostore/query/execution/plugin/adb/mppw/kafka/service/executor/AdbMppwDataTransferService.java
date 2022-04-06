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
package ru.datamart.prostore.query.execution.plugin.adb.mppw.kafka.service.executor;

import io.vertx.core.Future;
import lombok.extern.slf4j.Slf4j;
import lombok.val;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Component;
import ru.datamart.prostore.query.execution.plugin.adb.mppw.kafka.dto.TransferDataRequest;
import ru.datamart.prostore.query.execution.plugin.adb.mppw.kafka.factory.MppwRequestFactory;
import ru.datamart.prostore.query.execution.plugin.adb.query.service.DatabaseExecutor;

@Component
@Slf4j
public class AdbMppwDataTransferService {

    private final MppwRequestFactory mppwRequestFactory;
    private final DatabaseExecutor adbQueryExecutor;

    @Autowired
    public AdbMppwDataTransferService(MppwRequestFactory mppwRequestFactory,
                                      @Qualifier("adbQueryExecutor") DatabaseExecutor adbQueryExecutor) {
        this.mppwRequestFactory = mppwRequestFactory;
        this.adbQueryExecutor = adbQueryExecutor;
    }

    public Future<Void> execute(TransferDataRequest dataRequest) {
        return Future.future(promise -> {
            if (dataRequest.getHotDelta() == null) {
                promise.complete();
                return;
            }

            log.info("[ADB] Start transfer data");
            val transferRequest = mppwRequestFactory.create(dataRequest);
            //if ADB_WITH_HISTORY_TABLE = false, then queries can be performed without transactions
            adbQueryExecutor.executeInTransaction(transferRequest.getFirstTransaction())
                    .compose(v -> adbQueryExecutor.executeInTransaction(transferRequest.getSecondTransaction()))
                    .onComplete(promise);
        });
    }
}
