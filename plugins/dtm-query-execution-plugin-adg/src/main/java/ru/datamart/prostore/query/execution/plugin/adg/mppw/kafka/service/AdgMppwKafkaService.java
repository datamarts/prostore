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

import io.vertx.core.Future;
import lombok.extern.slf4j.Slf4j;
import lombok.val;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import ru.datamart.prostore.common.model.ddl.ExternalTableFormat;
import ru.datamart.prostore.common.model.ddl.ExternalTableLocationType;
import ru.datamart.prostore.query.execution.plugin.adg.base.factory.AdgHelperTableNamesFactory;
import ru.datamart.prostore.query.execution.plugin.adg.base.model.callback.function.TtTransferDataScdCallbackFunction;
import ru.datamart.prostore.query.execution.plugin.adg.base.model.callback.params.TtTransferDataScdCallbackParameter;
import ru.datamart.prostore.query.execution.plugin.adg.base.model.cartridge.request.AdgSubscriptionKafkaRequest;
import ru.datamart.prostore.query.execution.plugin.adg.base.model.cartridge.request.AdgTransferDataEtlRequest;
import ru.datamart.prostore.query.execution.plugin.adg.base.service.client.AdgCartridgeClient;
import ru.datamart.prostore.query.execution.plugin.adg.mppw.AdgMppwExecutor;
import ru.datamart.prostore.query.execution.plugin.adg.mppw.configuration.properties.AdgMppwProperties;
import ru.datamart.prostore.query.execution.plugin.api.exception.MppwDatasourceException;
import ru.datamart.prostore.query.execution.plugin.api.mppw.MppwRequest;
import ru.datamart.prostore.query.execution.plugin.api.mppw.kafka.MppwKafkaRequest;

import java.util.Collections;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

@Slf4j
@Service("adgMppwKafkaService")
public class AdgMppwKafkaService implements AdgMppwExecutor {
    private final Map<String, String> initializedLoadingByTopic;
    private final AdgMppwProperties mppwProperties;
    private final AdgCartridgeClient cartridgeClient;
    private final AdgHelperTableNamesFactory helperTableNamesFactory;

    @Autowired
    public AdgMppwKafkaService(AdgCartridgeClient cartridgeClient,
                               AdgMppwProperties mppwProperties,
                               AdgHelperTableNamesFactory helperTableNamesFactory) {
        this.cartridgeClient = cartridgeClient;
        this.mppwProperties = mppwProperties;
        this.helperTableNamesFactory = helperTableNamesFactory;
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
            val mppwKafkaContext = (MppwKafkaRequest) request;
            if (request.isLoadStart()) {
                log.debug("[ADG] mppw start for request [{}]", mppwKafkaContext);
                initializeLoading(mppwKafkaContext)
                        .onComplete(promise);
            } else {
                log.debug("[ADG] mppw stop for request [{}]", mppwKafkaContext);
                cancelLoadData(mppwKafkaContext)
                        .onComplete(promise);
            }
        });
    }

    @Override
    public ExternalTableLocationType getType() {
        return ExternalTableLocationType.KAFKA;
    }

    private Future<String> initializeLoading(MppwKafkaRequest mppwRequest) {
        if (initializedLoadingByTopic.containsKey(mppwRequest.getTopic())) {
            if (mppwRequest.getSysCn() == null) {
                return Future.succeededFuture();
            }

            return transferData(mppwRequest);
        } else {
            return Future.future(promise -> {
                val subscribeRequest = createSubscribeRequest(mppwRequest);
                cartridgeClient.subscribe(subscribeRequest)
                        .onSuccess(result -> {
                            log.debug("Loading initialize completed by [{}]", subscribeRequest);
                            initializedLoadingByTopic.put(mppwRequest.getTopic(), mppwRequest.getDestinationEntity().getName());
                            promise.complete(mppwProperties.getConsumerGroup());
                        })
                        .onFailure(promise::fail);
            });
        }
    }

    private AdgSubscriptionKafkaRequest createSubscribeRequest(MppwKafkaRequest mppwRequest) {
        val maxNumberOfMessages = getMaxNumberOfMessages(mppwRequest);
        val sysCn = mppwRequest.getSysCn();
        if (sysCn == null) {
            return new AdgSubscriptionKafkaRequest(
                    maxNumberOfMessages,
                    null,
                    mppwRequest.getTopic(),
                    Collections.singletonList(mppwRequest.getDestinationEntity().getExternalTableLocationPath()),
                    new TtTransferDataScdCallbackFunction(mppwProperties.getCallbackFunctionName(), null, null, null));
        }

        val helperTableNames = helperTableNamesFactory.create(
                mppwRequest.getEnvName(),
                mppwRequest.getDatamartMnemonic(),
                mppwRequest.getDestinationEntity().getName());
        val callbackFunctionParameter = new TtTransferDataScdCallbackParameter(
                helperTableNames.getStaging(),
                helperTableNames.getStaging(),
                helperTableNames.getActual(),
                helperTableNames.getHistory(),
                sysCn);

        val callbackFunction = new TtTransferDataScdCallbackFunction(
                mppwProperties.getCallbackFunctionName(),
                callbackFunctionParameter,
                maxNumberOfMessages,
                mppwProperties.getCallbackFunctionSecIdle());

        return new AdgSubscriptionKafkaRequest(
                maxNumberOfMessages,
                null,
                mppwRequest.getTopic(),
                Collections.singletonList(helperTableNames.getStaging()),
                callbackFunction
        );
    }

    private Future<String> cancelLoadData(MppwKafkaRequest mppwRequest) {
        return Future.future(promise -> {
            val topicName = mppwRequest.getTopic();
            transferData(mppwRequest)
                    .compose(result -> cartridgeClient.cancelSubscription(topicName))
                    .onSuccess(result -> {
                        initializedLoadingByTopic.remove(topicName);
                        log.debug("Cancel Load Data completed by request [{}]", topicName);
                        promise.complete();
                    })
                    .onFailure(promise::fail);
        });
    }

    private Future<String> transferData(MppwKafkaRequest mppwRequest) {
        return Future.future(promise -> {
            val sysCn = mppwRequest.getSysCn();
            if (sysCn == null) {
                promise.complete();
                return;
            }

            val helperTableNames = helperTableNamesFactory.create(
                    mppwRequest.getEnvName(),
                    mppwRequest.getDatamartMnemonic(),
                    mppwRequest.getDestinationEntity().getName());
            val transferDataRequest = new AdgTransferDataEtlRequest(helperTableNames, sysCn);
            cartridgeClient.transferDataToScdTable(transferDataRequest)
                    .onSuccess(result -> {
                        log.debug("Transfer Data completed by request [{}]", transferDataRequest);
                        promise.complete(mppwProperties.getConsumerGroup());
                    })
                    .onFailure(promise::fail);
        });
    }

    private long getMaxNumberOfMessages(MppwKafkaRequest mppwRequest) {
        val limit = mppwRequest.getSourceEntity().getExternalTableUploadMessageLimit();
        if (limit == null) {
            return mppwProperties.getMaxNumberOfMessagesPerPartition();
        }

        return limit.longValue();
    }
}
