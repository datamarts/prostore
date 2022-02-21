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
package ru.datamart.prostore.query.execution.plugin.api.mppr.kafka;

import ru.datamart.prostore.common.delta.DeltaInformation;
import ru.datamart.prostore.common.dto.KafkaBrokerInfo;
import ru.datamart.prostore.common.model.ddl.Entity;
import ru.datamart.prostore.common.model.ddl.ExternalTableLocationType;
import ru.datamart.prostore.query.execution.model.metadata.ColumnMetadata;
import ru.datamart.prostore.query.execution.model.metadata.Datamart;
import ru.datamart.prostore.query.execution.plugin.api.edml.BaseExternalEntityMetadata;
import ru.datamart.prostore.query.execution.plugin.api.mppr.MpprRequest;
import lombok.Builder;
import lombok.Getter;
import org.apache.calcite.sql.SqlNode;

import java.util.List;
import java.util.UUID;

@Getter
public class MpprKafkaRequest extends MpprRequest {
    private final SqlNode dmlSubQuery;
    private final BaseExternalEntityMetadata downloadMetadata;
    private final List<KafkaBrokerInfo> brokers;
    private final String topic;
    private final String sql;

    @Builder
    public MpprKafkaRequest(UUID requestId,
                            String envName,
                            String datamartMnemonic,
                            SqlNode sqlNode,
                            List<Datamart> logicalSchema,
                            List<ColumnMetadata> metadata,
                            Entity destinationEntity,
                            List<DeltaInformation> deltaInformations,
                            SqlNode dmlSubQuery,
                            BaseExternalEntityMetadata downloadMetadata,
                            List<KafkaBrokerInfo> brokers,
                            String topic, String sql) {
        super(requestId,
                envName,
                datamartMnemonic,
                sqlNode,
                logicalSchema,
                metadata,
                destinationEntity,
                deltaInformations,
                ExternalTableLocationType.KAFKA);
        this.dmlSubQuery = dmlSubQuery;
        this.downloadMetadata = downloadMetadata;
        this.brokers = brokers;
        this.topic = topic;
        this.sql = sql;
    }
}
