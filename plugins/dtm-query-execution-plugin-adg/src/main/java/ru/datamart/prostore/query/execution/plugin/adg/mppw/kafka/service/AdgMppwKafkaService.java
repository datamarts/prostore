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
package ru.datamart.prostore.query.execution.plugin.adg.mppw.kafka.service;

import ru.datamart.prostore.common.model.ddl.ExternalTableFormat;
import ru.datamart.prostore.common.model.ddl.ExternalTableLocationType;
import ru.datamart.prostore.query.execution.plugin.adg.base.model.callback.function.TtTransferDataScdCallbackFunction;
import ru.datamart.prostore.query.execution.plugin.adg.base.model.callback.params.TtTransferDataScdCallbackParameter;
import ru.datamart.prostore.query.execution.plugin.adg.base.model.cartridge.request.AdgSubscriptionKafkaRequest;
import ru.datamart.prostore.query.execution.plugin.adg.base.model.cartridge.request.AdgTransferDataEtlRequest;
import ru.datamart.prostore.query.execution.plugin.adg.base.service.client.AdgCartridgeClient;
import ru.datamart.prostore.query.execution.plugin.adg.mppw.AdgMppwExecutor;
import ru.datamart.prostore.query.execution.plugin.adg.mppw.configuration.properties.AdgMppwProperties;
import ru.datamart.prostore.query.execution.plugin.adg.mppw.kafka.dto.AdgMppwKafkaContext;
import ru.datamart.prostore.query.execution.plugin.adg.mppw.kafka.factory.AdgMppwKafkaContextFactory;
import ru.datamart.prostore.query.execution.plugin.api.exception.MppwDatasourceException;
import ru.datamart.prostore.query.execution.plugin.api.mppw.MppwRequest;
import ru.datamart.prostore.query.execution.plugin.api.mppw.kafka.MppwKafkaRequest;
import io.vertx.core.Future;
import lombok.extern.slf4j.Slf4j;
import lombok.val;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.Collections;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;

@Slf4j
@Service("adgMppwKafkaService")
public class AdgMppwKafkaService implements AdgMppwExecutor {

    private final AdgMppwKafkaContextFactory contextFactory;
    private final Map<String, String> initializedLoadingByTopic;
    private final AdgMppwProperties mppwProperties;
    private final AdgCartridgeClient cartridgeClient;

    @Autowired
    public AdgMppwKafkaService(AdgMppwKafkaContextFactory contextFactory,
                               AdgCartridgeClient cartridgeClient,
                               AdgMppwProperties mppwProperties) {
        this.contextFactory = contextFactory;
        this.cartridgeClient = cartridgeClient;
        this.mppwProperties = mppwProperties;
        initializedLoadingByTopic = new ConcurrentHashMap<>();
    }

    @Override
    public Future<String> execute(MppwRequest request) {
        return Future.future(promise -> {
            if (request.getUploadMetadata().getFormat() != ExternalTableFormat.AVRO) {
                promise.fail(new MppwDatasourceException(String.format("Format %s not implemented",
                        request.getUploadMetadata().getFormat())));
                return;
            }
            val mppwKafkaContext = contextFactory.create((MppwKafkaRequest) request);
            if (request.isLoadStart()) {
                log.debug("mppw start for request {}", request);
                initializeLoading(mppwKafkaContext, request.getSourceEntity().getExternalTableUploadMessageLimit())
                        .onComplete(promise);
            } else {
                log.debug("mppw stop for request {}", request);
                cancelLoadData(mppwKafkaContext)
                        .onComplete(promise);
            }
        });
    }

    @Override
    public ExternalTableLocationType getType() {
        return ExternalTableLocationType.KAFKA;
    }

    private Future<String> initializeLoading(AdgMppwKafkaContext ctx, Integer externalTableUploadMessageLimit) {
        if (initializedLoadingByTopic.containsKey(ctx.getTopicName())) {
            return transferData(ctx);
        } else {
            Long maxNumberOfMessages = Optional.ofNullable(externalTableUploadMessageLimit)
                    .map(Integer::longValue)
                    .orElse(mppwProperties.getMaxNumberOfMessagesPerPartition());
            return Future.future(promise -> {
                val callbackFunctionParameter = new TtTransferDataScdCallbackParameter(
                        ctx.getHelperTableNames().getStaging(),
                        ctx.getHelperTableNames().getStaging(),
                        ctx.getHelperTableNames().getActual(),
                        ctx.getHelperTableNames().getHistory(),
                        ctx.getHotDelta());

                val callbackFunction = new TtTransferDataScdCallbackFunction(
                        mppwProperties.getCallbackFunctionName(),
                        callbackFunctionParameter,
                        maxNumberOfMessages,
                        mppwProperties.getCallbackFunctionSecIdle());

                val request = new AdgSubscriptionKafkaRequest(
                        maxNumberOfMessages,
                        null,
                        ctx.getTopicName(),
                        Collections.singletonList(ctx.getHelperTableNames().getStaging()),
                        callbackFunction);

                cartridgeClient.subscribe(request)
                        .onSuccess(result -> {
                            log.debug("Loading initialize completed by [{}]", request);
                            initializedLoadingByTopic.put(ctx.getTopicName(), ctx.getConsumerTableName());
                            promise.complete(mppwProperties.getConsumerGroup());
                        })
                        .onFailure(promise::fail);
            });
        }
    }

    private Future<String> cancelLoadData(AdgMppwKafkaContext ctx) {
        return Future.future(promise -> {
            val topicName = ctx.getTopicName();
            transferData(ctx)
                    .compose(result -> cartridgeClient.cancelSubscription(topicName))
                    .onSuccess(result -> {
                        initializedLoadingByTopic.remove(topicName);
                        log.debug("Cancel Load Data completed by request [{}]", topicName);
                        promise.complete();
                    })
                    .onFailure(promise::fail);
        });
    }

    private Future<String> transferData(AdgMppwKafkaContext ctx) {
        return Future.future(promise -> {
            val request = new AdgTransferDataEtlRequest(ctx.getHelperTableNames(), ctx.getHotDelta());
            cartridgeClient.transferDataToScdTable(request)
                    .onSuccess(result -> {
                                log.debug("Transfer Data completed by request [{}]", request);
                                promise.complete(mppwProperties.getConsumerGroup());
                            }
                    )
                    .onFailure(promise::fail);
        });
    }
}
