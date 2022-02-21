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
package ru.datamart.prostore.query.execution.plugin.adb.mppr.kafka.dto;

import ru.datamart.prostore.common.dto.KafkaBrokerInfo;
import ru.datamart.prostore.common.reader.SourceType;
import ru.datamart.prostore.query.execution.model.metadata.ColumnMetadata;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.List;

/**
 * Mppr request for kafka connector
 *
 * @table table name
 * @datamart datamart
 * @sql sql query
 * @zookeeperHost Zookeeper host (not used)
 * @zookeeperPort Zookeeper port (not used)
 * @kafkaTopic kafka topic
 * @chunkSize chunk size
 */
@Data
@NoArgsConstructor
@AllArgsConstructor
@Builder
public class MpprKafkaConnectorRequest {
    String table;
    String datamart;
    String sql;
    List<KafkaBrokerInfo> kafkaBrokers;
    String kafkaTopic;
    Integer chunkSize;
    SourceType sourceType;
    String avroSchema;
    List<ColumnMetadata> metadata;
}
