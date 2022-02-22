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
package ru.datamart.prostore.query.execution.plugin.adb.mppw.kafka.factory;

import ru.datamart.prostore.common.dto.KafkaBrokerInfo;
import ru.datamart.prostore.query.execution.plugin.adb.mppw.configuration.properties.AdbMppwProperties;
import ru.datamart.prostore.query.execution.plugin.adb.mppw.kafka.dto.MppwKafkaLoadRequest;
import ru.datamart.prostore.query.execution.plugin.api.mppw.kafka.MppwKafkaRequest;
import ru.datamart.prostore.query.execution.plugin.api.mppw.kafka.UploadExternalEntityMetadata;
import lombok.val;
import org.apache.avro.Schema;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

import static ru.datamart.prostore.query.execution.plugin.adb.base.factory.Constants.SYS_FROM_ATTR;
import static ru.datamart.prostore.query.execution.plugin.adb.base.factory.Constants.SYS_TO_ATTR;

@Component
public class MppwKafkaLoadRequestFactory {

    private final List<String> excludeSystemFields = Arrays.asList(SYS_FROM_ATTR, SYS_TO_ATTR);
    private final KafkaMppwSqlFactory kafkaMppwSqlFactory;

    @Autowired
    public MppwKafkaLoadRequestFactory(KafkaMppwSqlFactory kafkaMppwSqlFactory) {
        this.kafkaMppwSqlFactory = kafkaMppwSqlFactory;
    }

    public MppwKafkaLoadRequest create(MppwKafkaRequest request, String server, AdbMppwProperties adbMppwProperties) {
        val uploadMeta = (UploadExternalEntityMetadata) request.getUploadMetadata();
        val schema = new Schema.Parser().parse(uploadMeta.getExternalSchema());
        val reqId = request.getRequestId().toString();
        return MppwKafkaLoadRequest.builder()
            .requestId(reqId)
            .datamart(request.getDatamartMnemonic())
            .tableName(request.getDestinationEntity().getName())
            .writableExtTableName(kafkaMppwSqlFactory.getTableName(reqId))
            .columns(getColumns(schema))
            .schema(schema)
            .brokers(request.getBrokers().stream()
                    .map(KafkaBrokerInfo::getAddress)
                    .collect(Collectors.joining(",")))
            .consumerGroup(adbMppwProperties.getConsumerGroup())
            .timeout(adbMppwProperties.getStopTimeoutMs())
            .topic(request.getTopic())
            .uploadMessageLimit(adbMppwProperties.getDefaultMessageLimit())
            .server(server)
            .build();
    }

    private List<String> getColumns(Schema schema) {
        return schema.getFields().stream()
            .map(Schema.Field::name)
            .filter(field -> excludeSystemFields.stream()
                .noneMatch(sysName -> sysName.equals(field)))
            .collect(Collectors.toList());
    }
}
