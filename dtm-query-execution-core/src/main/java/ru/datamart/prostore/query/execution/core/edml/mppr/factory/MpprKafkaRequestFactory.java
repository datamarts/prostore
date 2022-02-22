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
package ru.datamart.prostore.query.execution.core.edml.mppr.factory;

import ru.datamart.prostore.common.dto.KafkaBrokerInfo;
import ru.datamart.prostore.kafka.core.repository.ZookeeperKafkaProviderRepository;
import ru.datamart.prostore.query.execution.core.edml.dto.EdmlRequestContext;
import ru.datamart.prostore.query.execution.core.edml.exception.UnreachableLocationException;
import ru.datamart.prostore.query.execution.core.edml.service.LocationUriParser;
import ru.datamart.prostore.query.execution.model.metadata.ColumnMetadata;
import ru.datamart.prostore.query.execution.plugin.api.mppr.MpprRequest;
import ru.datamart.prostore.query.execution.plugin.api.mppr.kafka.DownloadExternalEntityMetadata;
import ru.datamart.prostore.query.execution.plugin.api.mppr.kafka.MpprKafkaRequest;
import io.vertx.core.Future;
import io.vertx.core.Vertx;
import lombok.val;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Component;

import java.util.List;

@Component
public class MpprKafkaRequestFactory {

    private final ZookeeperKafkaProviderRepository zkConnProviderRepository;
    private final LocationUriParser locationUriParser;
    private final Vertx vertx;

    @Autowired
    public MpprKafkaRequestFactory(@Qualifier("mapZkKafkaProviderRepository") ZookeeperKafkaProviderRepository zkConnProviderRepository,
                                   @Qualifier("coreVertx") Vertx vertx,
                                   LocationUriParser locationUriParser) {
        this.zkConnProviderRepository = zkConnProviderRepository;
        this.locationUriParser = locationUriParser;
        this.vertx = vertx;
    }

    public Future<MpprRequest> create(EdmlRequestContext context, List<ColumnMetadata> columnMetadata) {
        return Future.future(p -> {
            val kafkaTopicUri = locationUriParser.parseKafkaLocationPath(context.getDestinationEntity().getExternalTableLocationPath());
            getBrokers(kafkaTopicUri.getAddress())
                    .map(kafkaBrokers -> MpprKafkaRequest.builder()
                            .requestId(context.getRequest().getQueryRequest().getRequestId())
                            .envName(context.getEnvName())
                            .datamartMnemonic(context.getRequest().getQueryRequest().getDatamartMnemonic())
                            .sql(context.getRequest().getQueryRequest().getSql())
                            .sqlNode(context.getSqlNode())
                            .brokers(kafkaBrokers)
                            .dmlSubQuery(context.getDmlSubQuery())
                            .destinationEntity(context.getDestinationEntity())
                            .logicalSchema(context.getLogicalSchema())
                            .deltaInformations(context.getDeltaInformations())
                            .topic(kafkaTopicUri.getTopic())
                            .metadata(columnMetadata)
                            .downloadMetadata(DownloadExternalEntityMetadata.builder()
                                    .name(context.getDestinationEntity().getName())
                                    .format(context.getDestinationEntity().getExternalTableFormat())
                                    .externalSchema(context.getDestinationEntity().getExternalTableSchema())
                                    .locationPath(context.getDestinationEntity().getExternalTableLocationPath())
                                    .chunkSize(context.getDestinationEntity().getExternalTableDownloadChunkSize())
                                    .build())
                            .build())
                    .onSuccess(p::complete)
                    .onFailure(p::fail);
        });
    }

    private Future<List<KafkaBrokerInfo>> getBrokers(String connectionString) {
        return vertx.executeBlocking(p -> {
            try {
                val kafkaBrokers = zkConnProviderRepository.getOrCreate(connectionString).getKafkaBrokers();
                p.complete(kafkaBrokers);
            } catch (Exception e) {
                p.fail(new UnreachableLocationException(connectionString, e));
            }
        }, false);
    }
}
