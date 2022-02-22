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
package ru.datamart.prostore.query.execution.plugin.adb.dml.service;

import io.vertx.core.Future;
import lombok.val;
import org.springframework.stereotype.Component;
import ru.datamart.prostore.common.model.ddl.EntityFieldUtils;
import ru.datamart.prostore.query.execution.plugin.adb.dml.dto.UpsertTransferRequest;
import ru.datamart.prostore.query.execution.plugin.adb.query.service.DatabaseExecutor;

import static ru.datamart.prostore.query.execution.plugin.adb.dml.factory.AdbUpsertSqlFactory.*;

@Component
public class AdbUpsertDataTransferService {

    private final DatabaseExecutor adbQueryExecutor;

    public AdbUpsertDataTransferService(DatabaseExecutor adbQueryExecutor) {
        this.adbQueryExecutor = adbQueryExecutor;
    }

    public Future<Void> transfer(UpsertTransferRequest request) {
        return Future.future(promise -> {
            val entity = request.getEntity();
            val nameWithSchema = entity.getNameWithSchema();
            val pkFieldNames = EntityFieldUtils.getPkFieldNames(entity);
            adbQueryExecutor.executeUpdate(updateSql(nameWithSchema, pkFieldNames, request.getSysCn()))
                    .compose(ignore -> adbQueryExecutor.executeUpdate(insertSql(nameWithSchema, EntityFieldUtils.getFieldNames(entity), pkFieldNames, request.getTargetColumnList(), request.getSysCn())))
                    .compose(ignore -> adbQueryExecutor.executeUpdate(truncateSql(nameWithSchema)))
                    .onComplete(promise);
        });
    }
}
