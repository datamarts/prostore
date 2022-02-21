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
package ru.datamart.prostore.query.execution.plugin.adb.mppw.kafka.service.executor.impl.pxf;

import ru.datamart.prostore.common.dto.KafkaBrokerInfo;
import ru.datamart.prostore.common.model.ddl.*;
import ru.datamart.prostore.query.execution.plugin.adb.mppw.kafka.factory.KafkaMppwSqlFactory;
import ru.datamart.prostore.query.execution.plugin.adb.mppw.kafka.service.executor.AdbMppwRequestExecutor;
import ru.datamart.prostore.query.execution.plugin.adb.query.service.DatabaseExecutor;
import ru.datamart.prostore.query.execution.plugin.api.exception.MppwDatasourceException;
import ru.datamart.prostore.query.execution.plugin.api.mppw.MppwRequest;
import ru.datamart.prostore.query.execution.plugin.api.mppw.kafka.MppwKafkaRequest;
import io.vertx.core.Future;
import lombok.extern.slf4j.Slf4j;
import lombok.val;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.stereotype.Component;

import java.util.Comparator;
import java.util.List;
import java.util.stream.Collectors;

@ConditionalOnProperty(
        value = "adb.mppw.usePxfConnector",
        havingValue = "true")
@Component("adbMppwStartRequestExecutor")
@Slf4j
public class AdbMppwStartRequestExecutorPxfImpl implements AdbMppwRequestExecutor {

    private static final String CONSUMER_GROUP_TEMPLATE = "kgw_%s_%s";
    private static final String CAST_AS_TIME_TEMPLATE = "CAST(%s AS TIME)";
    private static final String SYS_OP = "sys_op";
    private static final String COLUMNS_DELIMITER = ", ";

    private final DatabaseExecutor adbQueryExecutor;
    private final KafkaMppwSqlFactory kafkaMppwSqlFactory;
    private final Long pollTimeoutMs;

    public AdbMppwStartRequestExecutorPxfImpl(DatabaseExecutor adbQueryExecutor,
                                              KafkaMppwSqlFactory kafkaMppwSqlFactory,
                                              @Value("${core.kafka.admin.inputStreamTimeoutMs}") Long pollTimeoutMs) {
        this.adbQueryExecutor = adbQueryExecutor;
        this.kafkaMppwSqlFactory = kafkaMppwSqlFactory;
        this.pollTimeoutMs = pollTimeoutMs;
    }

    @Override
    public Future<String> execute(MppwKafkaRequest request) {
        val format = request.getUploadMetadata().getFormat();
        if (!ExternalTableFormat.AVRO.equals(format)) {
            return Future.failedFuture(new MppwDatasourceException(String.format("Format %s not implemented", format)));
        }

        val requestId = request.getRequestId().toString().replace("-", "_");
        val actualConsumerGroup = String.format(CONSUMER_GROUP_TEMPLATE, request.getEnvName(), request.getSourceEntity().getNameWithSchema());
        return createReadableExternalTable(request, requestId, actualConsumerGroup)
                .compose(ignore -> executeInsertIntoStaging(request, requestId))
                .map(ignore -> actualConsumerGroup)
                .onFailure(err -> dropExtTable(request, requestId));
    }

    private Future<Void> createReadableExternalTable(MppwKafkaRequest request, String requestId, String actualConsumerGroup) {
        val columns = kafkaMppwSqlFactory.getPxfColumnsFromEntity(request.getSourceEntity());
        columns.add("sys_op int");
        val brokersList = request.getBrokers().stream()
                .map(KafkaBrokerInfo::getAddress)
                .collect(Collectors.joining(","));
        return adbQueryExecutor.executeUpdate(kafkaMppwSqlFactory.createReadableExtTableSqlQuery(request.getSourceEntity().getNameWithSchema(),
                requestId,
                columns,
                request.getTopic(),
                brokersList,
                actualConsumerGroup,
                pollTimeoutMs));
    }

    private Future<Void> executeInsertIntoStaging(MppwKafkaRequest request, String requestId) {
        val insertColumns = getColumnString(EntityFieldUtils.getFieldNames(request.getSourceEntity()));
        val selectColumns = getColumnString(getColumnListWithTimeCast(request.getSourceEntity()));
        return adbQueryExecutor.executeUpdate(kafkaMppwSqlFactory.insertIntoStagingTablePxfSqlQuery(request.getDestinationEntity().getSchema(),
                insertColumns,
                selectColumns,
                request.getDestinationEntity().getName(),
                String.format("%s_%s_ext", request.getSourceEntity().getName(), requestId)));
    }

    private String getColumnString(List<String> columns) {
        columns.add(SYS_OP);
        return String.join(COLUMNS_DELIMITER, columns);
    }

    private List<String> getColumnListWithTimeCast(Entity entity) {
        return entity.getFields().stream()
                .sorted(Comparator.comparingInt(EntityField::getOrdinalPosition))
                .map(field -> {
                    if (field.getType().equals(ColumnType.TIME)) {
                        return String.format(CAST_AS_TIME_TEMPLATE, field.getName());
                    } else {
                        return field.getName();
                    }
                })
                .collect(Collectors.toList());
    }

    private void dropExtTable(MppwRequest request, String requestId) {
        adbQueryExecutor.executeUpdate(kafkaMppwSqlFactory.dropExtTableSqlQuery(request.getSourceEntity().getSchema(),
                request.getSourceEntity().getName(),
                requestId));
    }
}
