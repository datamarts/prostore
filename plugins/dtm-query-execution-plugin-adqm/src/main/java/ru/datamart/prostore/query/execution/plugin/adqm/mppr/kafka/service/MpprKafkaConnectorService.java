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
package ru.datamart.prostore.query.execution.plugin.adqm.mppr.kafka.service;

import ru.datamart.prostore.common.reader.QueryResult;
import ru.datamart.prostore.query.execution.plugin.adqm.mppr.configuration.properties.AdqmMpprProperties;
import ru.datamart.prostore.query.execution.plugin.adqm.mppr.kafka.dto.MpprKafkaConnectorRequest;
import io.vertx.core.Future;
import io.vertx.ext.web.client.WebClient;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Service;

import java.net.HttpURLConnection;

@Service
@Slf4j
public class MpprKafkaConnectorService {

    private final AdqmMpprProperties adqmMpprProperties;
    private final WebClient client;

    @Autowired
    public MpprKafkaConnectorService(AdqmMpprProperties adqmMpprProperties,
                                     @Qualifier("adqmWebClient") WebClient webClient) {
        this.adqmMpprProperties = adqmMpprProperties;
        this.client = webClient;
    }

    public Future<QueryResult> call(MpprKafkaConnectorRequest request) {
        return Future.future(promise -> {
            log.debug("Calling MpprKafkaConnector with url: {}", adqmMpprProperties.getLoadingUrl());
            client.postAbs(adqmMpprProperties.getLoadingUrl())
                    .sendJson(request, ar -> {
                        if (ar.succeeded()) {
                            if (ar.result().statusCode() == HttpURLConnection.HTTP_OK) {
                                promise.complete(QueryResult.emptyResult());
                            } else {
                                promise.fail(ar.cause());
                            }
                        } else {
                            promise.fail(ar.cause());
                        }
                    });
        });
    }
}
