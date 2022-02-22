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
package ru.datamart.prostore.query.execution.plugin.adb.mppw.kafka.service.handler;

import ru.datamart.prostore.query.execution.plugin.adb.mppw.kafka.dto.MppwKafkaLoadRequest;
import ru.datamart.prostore.query.execution.plugin.adb.mppw.kafka.dto.MppwKafkaRequestContext;
import ru.datamart.prostore.query.execution.plugin.adb.mppw.kafka.factory.KafkaMppwSqlFactory;
import ru.datamart.prostore.query.execution.plugin.adb.mppw.kafka.service.executor.AdbMppwDataTransferService;
import ru.datamart.prostore.query.execution.plugin.adb.query.service.DatabaseExecutor;
import io.vertx.core.Future;
import lombok.extern.slf4j.Slf4j;
import lombok.val;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Component;

@Component("adbMppwTransferDataHandler")
@Slf4j
public class AdbMppwTransferDataHandler {

    private final DatabaseExecutor adbQueryExecutor;
    private final KafkaMppwSqlFactory kafkaMppwSqlFactory;
    private final AdbMppwDataTransferService mppwDataTransferService;

    @Autowired
    public AdbMppwTransferDataHandler(@Qualifier("adbQueryExecutor") DatabaseExecutor adbQueryExecutor,
                                      KafkaMppwSqlFactory kafkaMppwSqlFactory,
                                      AdbMppwDataTransferService mppwDataTransferService) {
        this.adbQueryExecutor = adbQueryExecutor;
        this.kafkaMppwSqlFactory = kafkaMppwSqlFactory;
        this.mppwDataTransferService = mppwDataTransferService;
    }

    public Future<Void> handle(MppwKafkaRequestContext requestContext) {
        return insertIntoStagingTable(requestContext.getMppwKafkaLoadRequest())
                .compose(v -> commitKafkaMessages(requestContext))
                .compose(s -> mppwDataTransferService.execute(requestContext.getTransferDataRequest()));
    }

    private Future<Void> commitKafkaMessages(MppwKafkaRequestContext requestContext) {
        return Future.future(promise -> {
            val schema = requestContext.getMppwKafkaLoadRequest().getDatamart();
            val table = kafkaMppwSqlFactory.getTableName(requestContext.getMppwKafkaLoadRequest().getRequestId());
            val commitOffsetsSql = kafkaMppwSqlFactory.commitOffsetsSqlQuery(schema, table);
            adbQueryExecutor.executeUpdate(commitOffsetsSql)
                    .onComplete(promise);
        });
    }

    private Future<Void> insertIntoStagingTable(MppwKafkaLoadRequest request) {
        return Future.future(promise -> {
            val schema = request.getDatamart();
            val columns = String.join(", ", request.getColumns());
            val extTable = request.getWritableExtTableName().replace("-", "_");
            val stagingTable = request.getTableName();
            adbQueryExecutor.executeUpdate(kafkaMppwSqlFactory.insertIntoStagingTableSqlQuery(schema,
                    columns,
                    stagingTable,
                    extTable))
                    .onComplete(promise);
        });
    }
}
