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
package ru.datamart.prostore.query.execution.plugin.adqm.mppw.kafka.service.load;

import ru.datamart.prostore.query.execution.plugin.adqm.base.utils.AdqmDdlUtil;
import ru.datamart.prostore.query.execution.plugin.adqm.ddl.configuration.properties.DdlProperties;
import ru.datamart.prostore.query.execution.plugin.adqm.mppw.configuration.properties.AdqmMppwProperties;
import lombok.NonNull;
import org.apache.avro.Schema;

import java.util.stream.Collectors;

import static ru.datamart.prostore.query.execution.plugin.adqm.base.utils.Constants.EXT_SHARD_POSTFIX;
import static java.lang.String.format;

public class KafkaExtTableCreator implements ExtTableCreator {
    private static final String KAFKA_ENGINE_TEMPLATE = "ENGINE = Kafka()\n" +
            "  SETTINGS\n" +
            "    kafka_broker_list = '%s',\n" +
            "    kafka_topic_list = '%s',\n" +
            "    kafka_group_name = '%s',\n" +
            "    kafka_format = '%s'";
    private static final String EXT_SHARD_TEMPLATE =
            "CREATE TABLE IF NOT EXISTS %s ON CLUSTER %s (\n" +
                    "  %s\n" +
                    ")\n" +
                    "%s\n";

    private final DdlProperties ddlProperties;
    private final AdqmMppwProperties adqmMppwProperties;

    public KafkaExtTableCreator(DdlProperties ddlProperties, AdqmMppwProperties adqmMppwProperties) {
        this.ddlProperties = ddlProperties;
        this.adqmMppwProperties = adqmMppwProperties;
    }

    @Override
    public String generate(@NonNull String topic, @NonNull String table, @NonNull Schema schema, @NonNull String sortingKey) {
        String kafkaSettings = genKafkaEngine(topic, table);

        String columns = schema.getFields().stream()
                .map(AdqmDdlUtil::avroFieldToString)
                .collect(Collectors.joining(", "));
        return format(EXT_SHARD_TEMPLATE, table + EXT_SHARD_POSTFIX, ddlProperties.getCluster(), columns, kafkaSettings);
    }

    private String genKafkaEngine(@NonNull String topic, @NonNull String tableName) {
        String brokers = adqmMppwProperties.getKafkaBrokers();
        String consumerGroup = getConsumerGroupName(tableName);
        // FIXME Support other formats (Text, CSV, Json?)
        String format = "Avro";
        return format(KAFKA_ENGINE_TEMPLATE, brokers, topic, consumerGroup, format);
    }

    @NonNull
    private String getConsumerGroupName(@NonNull String tableName) {
        return adqmMppwProperties.getConsumerGroup() + tableName;
    }
}
