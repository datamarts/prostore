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
package ru.datamart.prostore.query.execution.plugin.adqm.mppw.kafka.service;

import ru.datamart.prostore.common.model.ddl.EntityFieldUtils;
import ru.datamart.prostore.query.execution.plugin.adqm.base.utils.AdqmDdlUtil;
import ru.datamart.prostore.query.execution.plugin.adqm.ddl.configuration.properties.DdlProperties;
import ru.datamart.prostore.query.execution.plugin.adqm.factory.AdqmTablesSqlFactory;
import ru.datamart.prostore.query.execution.plugin.adqm.mppw.configuration.properties.AdqmMppwProperties;
import ru.datamart.prostore.query.execution.plugin.adqm.mppw.kafka.dto.RestMppwKafkaLoadRequest;
import ru.datamart.prostore.query.execution.plugin.adqm.mppw.kafka.factory.AdqmRestMppwKafkaRequestFactory;
import ru.datamart.prostore.query.execution.plugin.adqm.mppw.kafka.service.load.*;
import ru.datamart.prostore.query.execution.plugin.adqm.query.service.DatabaseExecutor;
import ru.datamart.prostore.query.execution.plugin.adqm.status.dto.StatusReportDto;
import ru.datamart.prostore.query.execution.plugin.adqm.status.service.StatusReporter;
import ru.datamart.prostore.query.execution.plugin.api.exception.DataSourceException;
import ru.datamart.prostore.query.execution.plugin.api.exception.MppwDatasourceException;
import ru.datamart.prostore.query.execution.plugin.api.mppw.kafka.MppwKafkaRequest;
import io.vertx.core.Future;
import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;
import lombok.val;
import org.apache.avro.Schema;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.util.Arrays;
import java.util.EnumMap;
import java.util.Map;

import static ru.datamart.prostore.query.execution.plugin.adqm.base.utils.AdqmDdlUtil.sequenceAll;
import static ru.datamart.prostore.query.execution.plugin.adqm.base.utils.Constants.*;
import static ru.datamart.prostore.query.execution.plugin.adqm.mppw.kafka.service.load.LoadType.KAFKA;
import static ru.datamart.prostore.query.execution.plugin.adqm.mppw.kafka.service.load.LoadType.REST;

@Component("adqmMppwStartRequestHandler")
@Slf4j
public class MppwStartRequestHandler extends AbstractMppwRequestHandler {

    private final AdqmMppwProperties mppwProperties;
    private final StatusReporter statusReporter;
    private final Map<LoadType, ExtTableCreator> extTableCreators = new EnumMap<>(LoadType.class);
    private final RestLoadClient restLoadClient;
    private final AdqmRestMppwKafkaRequestFactory restMppwKafkaRequestFactory;
    private final AdqmTablesSqlFactory adqmTablesSqlFactory;

    @Autowired
    public MppwStartRequestHandler(DatabaseExecutor databaseExecutor,
                                   DdlProperties ddlProperties,
                                   AdqmMppwProperties mppwProperties,
                                   StatusReporter statusReporter,
                                   RestLoadClient restLoadClient,
                                   AdqmRestMppwKafkaRequestFactory restMppwKafkaRequestFactory,
                                   AdqmTablesSqlFactory adqmTablesSqlFactory) {
        super(databaseExecutor, ddlProperties);
        this.mppwProperties = mppwProperties;
        this.statusReporter = statusReporter;
        this.restLoadClient = restLoadClient;
        this.restMppwKafkaRequestFactory = restMppwKafkaRequestFactory;
        this.adqmTablesSqlFactory = adqmTablesSqlFactory;

        extTableCreators.put(KAFKA, new KafkaExtTableCreator(ddlProperties, mppwProperties));
        extTableCreators.put(REST, new RestExtTableCreator(ddlProperties));
    }

    @Override
    public Future<String> execute(MppwKafkaRequest request) {
        val err = AdqmDdlUtil.validateRequest(request);
        if (err.isPresent()) {
            return Future.failedFuture(new DataSourceException(err.get()));
        }

        Schema schema;
        try {
            schema = new Schema.Parser().parse(request.getUploadMetadata().getExternalSchema());
        } catch (Exception e) {
            return Future.failedFuture(new DataSourceException("Error in starting mppw request", e));
        }

        val fullName = AdqmDdlUtil.getQualifiedTableName(request);
        reportStart(request.getTopic(), fullName);

        return sequenceAll(Arrays.asList(
                fullName + EXT_SHARD_POSTFIX,
                fullName + ACTUAL_LOADER_SHARD_POSTFIX,
                fullName + BUFFER_LOADER_SHARD_POSTFIX,
                fullName + BUFFER_POSTFIX,
                fullName + BUFFER_SHARD_POSTFIX
        ), this::dropTable)
                .compose(v -> {
                    val pkNamesString = String.join(", ", EntityFieldUtils.getPkFieldNames(request.getDestinationEntity()));
                    return createExternalShardTable(request.getTopic(), fullName, schema, pkNamesString);
                })
                .compose(v -> createBufferShardTable(request))
                .compose(v -> createBufferTable(request))
                .compose(v -> createBufferLoaderShardTable(request))
                .compose(v -> createActualLoaderShardTable(request))
                .compose(v -> createRestInitiator(request))
                .map(v -> getConsumerGroupName(fullName))
                .onSuccess(Future::succeededFuture)
                .onFailure(fail -> {
                    reportError(request.getTopic());
                    Future.failedFuture(new DataSourceException(fail));
                });
    }

    @NonNull
    private String getConsumerGroupName(@NonNull String tableName) {
        return mppwProperties.getLoadType() == KAFKA ?
                mppwProperties.getConsumerGroup() + "_" + tableName :
                mppwProperties.getRestLoadConsumerGroup();
    }

    private Future<Void> createExternalShardTable(@NonNull String topic,
                                                  @NonNull String table,
                                                  @NonNull Schema schema,
                                                  @NonNull String sortingKey) {
        LoadType loadType = mppwProperties.getLoadType();
        ExtTableCreator creator = extTableCreators.get(loadType);
        String query = creator.generate(topic, table, schema, sortingKey);
        return databaseExecutor.executeUpdate(query);
    }

    private Future<Void> createBufferShardTable(MppwKafkaRequest request) {
        val query = adqmTablesSqlFactory.getCreateBufferShardSql(request.getEnvName(), request.getDatamartMnemonic(), request.getDestinationEntity());
        return databaseExecutor.executeUpdate(query);
    }

    private Future<Void> createBufferTable(MppwKafkaRequest request) {
        val query = adqmTablesSqlFactory.getCreateBufferSql(request.getEnvName(), request.getDatamartMnemonic(), request.getDestinationEntity());
        return databaseExecutor.executeUpdate(query);
    }

    private Future<Void> createBufferLoaderShardTable(MppwKafkaRequest request) {
        val query = adqmTablesSqlFactory.getCreateBufferLoaderShardSql(request.getEnvName(), request.getDatamartMnemonic(), request.getDestinationEntity());
        return databaseExecutor.executeUpdate(query);
    }

    private Future<Void> createActualLoaderShardTable(MppwKafkaRequest request) {
        val query = adqmTablesSqlFactory.getCreateActualLoaderShardSql(request.getEnvName(), request.getDatamartMnemonic(),
                request.getDestinationEntity(), request.getSysCn());
        return databaseExecutor.executeUpdate(query);
    }

    private Future<Void> createRestInitiator(MppwKafkaRequest mppwPluginRequest) {
        LoadType loadType = mppwProperties.getLoadType();
        //it means that if we use KAFKA instead of REST load type of mppw, we shouldn't send rest request
        if (loadType == KAFKA) {
            return Future.succeededFuture();
        }
        try {
            final RestMppwKafkaLoadRequest mppwKafkaLoadRequest = restMppwKafkaRequestFactory.create(mppwPluginRequest);
            log.debug("ADQM: Send mppw kafka starting rest request {}", mppwKafkaLoadRequest);
            return restLoadClient.initiateLoading(mppwKafkaLoadRequest);
        } catch (Exception e) {
            return Future.failedFuture(new MppwDatasourceException("Error generating mppw kafka request", e));
        }
    }

    private void reportStart(String topic, String fullName) {
        StatusReportDto start = new StatusReportDto(topic, getConsumerGroupName(fullName));
        statusReporter.onStart(start);
    }

    private void reportError(String topic) {
        StatusReportDto start = new StatusReportDto(topic);
        statusReporter.onError(start);
    }
}
