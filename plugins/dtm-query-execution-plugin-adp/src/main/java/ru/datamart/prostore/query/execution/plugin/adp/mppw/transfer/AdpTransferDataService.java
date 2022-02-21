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
package ru.datamart.prostore.query.execution.plugin.adp.mppw.transfer;

import ru.datamart.prostore.query.execution.plugin.adp.db.service.DatabaseExecutor;
import ru.datamart.prostore.query.execution.plugin.adp.mppw.dto.AdpTransferDataRequest;
import ru.datamart.prostore.query.execution.plugin.adp.mppw.factory.AdpTransferDataSqlFactory;
import io.vertx.core.Future;
import lombok.extern.slf4j.Slf4j;
import lombok.val;
import org.springframework.stereotype.Service;

@Slf4j
@Service
public class AdpTransferDataService {

    private final AdpTransferDataSqlFactory transferDataSqlFactory;
    private final DatabaseExecutor databaseExecutor;

    public AdpTransferDataService(AdpTransferDataSqlFactory transferDataSqlFactory,
                                  DatabaseExecutor databaseExecutor) {
        this.transferDataSqlFactory = transferDataSqlFactory;
        this.databaseExecutor = databaseExecutor;
    }

    public Future<Void> transferData(AdpTransferDataRequest request) {
        return Future.future(promise -> {
            log.info("[ADP] Start transfer data");

            val sql = String.join("",
                    transferDataSqlFactory.getCloseVersionOfRecordsSql(request.getDatamart(), request.getTableName(), request.getPrimaryKeys(), request.getSysCn()),
                    transferDataSqlFactory.getUploadHotRecordsSql(request.getDatamart(), request.getTableName(), request.getAllFields(), request.getPrimaryKeys(), request.getSysCn()),
                    transferDataSqlFactory.getTruncateSql(request.getDatamart(), request.getTableName())
            );

            databaseExecutor.executeUpdate(sql)
                    .onSuccess(v -> {
                        log.info("[ADP] Transfer data completed successfully");
                        promise.complete();
                    })
                    .onFailure(t -> {
                        log.error("[ADP] Transfer data failed with exception", t);
                        promise.fail(t);
                    });
        });
    }
}
