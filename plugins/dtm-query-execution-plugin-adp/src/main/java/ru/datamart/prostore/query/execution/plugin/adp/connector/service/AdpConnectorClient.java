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
package ru.datamart.prostore.query.execution.plugin.adp.connector.service;

import ru.datamart.prostore.common.exception.DtmException;
import ru.datamart.prostore.common.version.VersionInfo;
import ru.datamart.prostore.query.execution.plugin.adp.base.properties.AdpMpprProperties;
import ru.datamart.prostore.query.execution.plugin.adp.base.properties.AdpMppwProperties;
import ru.datamart.prostore.query.execution.plugin.adp.connector.dto.AdpConnectorMpprRequest;
import ru.datamart.prostore.query.execution.plugin.adp.connector.dto.AdpConnectorMppwStartRequest;
import ru.datamart.prostore.query.execution.plugin.adp.connector.dto.AdpConnectorMppwStopRequest;
import ru.datamart.prostore.query.execution.plugin.api.exception.DataSourceException;
import io.vertx.core.Future;
import io.vertx.core.buffer.Buffer;
import io.vertx.core.json.Json;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.web.client.HttpResponse;
import io.vertx.ext.web.client.WebClient;
import lombok.extern.slf4j.Slf4j;
import lombok.val;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Service;

import java.util.List;
import java.util.stream.Collectors;

@Slf4j
@Service
public class AdpConnectorClient {
    private final WebClient webClient;
    private final AdpMppwProperties mppwProperties;
    private final AdpMpprProperties mpprProperties;

    public AdpConnectorClient(@Qualifier("adpWebClient") WebClient webClient,
                              AdpMppwProperties mppwProperties,
                              AdpMpprProperties mpprProperties) {
        this.webClient = webClient;
        this.mppwProperties = mppwProperties;
        this.mpprProperties = mpprProperties;
    }

    public Future<Void> startMppw(AdpConnectorMppwStartRequest request) {
        return Future.future(event -> {
            JsonObject data = JsonObject.mapFrom(request);
            executePostRequest(mppwProperties.getRestStartLoadUrl(), data)
                    .onComplete(event);
        });
    }

    public Future<Void> stopMppw(AdpConnectorMppwStopRequest request) {
        return Future.future(event -> {
            JsonObject data = JsonObject.mapFrom(request);
            executePostRequest(mppwProperties.getRestStopLoadUrl(), data)
                    .onComplete(event);
        });
    }

    public Future<Void> runMppr(AdpConnectorMpprRequest request) {
        return Future.future(event -> {
            JsonObject data = JsonObject.mapFrom(request);
            executePostRequest(mpprProperties.getRestLoadUrl(), data)
                    .onComplete(event);
        });
    }

    public Future<List<VersionInfo>> getMppwVersion() {
        return executeGetRequest(mppwProperties.getRestVersionUrl())
                .map(this::handleVersionInfoResponse);
    }

    public Future<List<VersionInfo>> getMpprVersion() {
        return executeGetRequest(mpprProperties.getRestVersionUrl())
                .map(this::handleVersionInfoResponse);
    }

    private Future<Void> executePostRequest(String uri, JsonObject data) {
        return Future.future(promise -> {
            log.debug("[ADP] Request[POST] to [{}] trying to send, data: [{}]", uri, data);
            webClient.postAbs(uri)
                    .sendJsonObject(data)
                    .onSuccess(response -> {
                        if (response.statusCode() < 400 && response.statusCode() >= 200) {
                            log.debug("[ADP] Request[POST] to [{}] succeeded", uri);
                            promise.complete();
                        } else {
                            log.error("[ADP] Request[POST] to [{}] failed with error [{}] response [{}]",
                                    uri, response.statusCode(), response.body());
                            promise.fail(new DataSourceException(response.bodyAsString()));
                        }
                    })
                    .onFailure(t -> {
                        log.error("[ADP] Request[POST] to [{}] failed with exception", uri, t);
                        promise.fail(new DataSourceException(
                                String.format("Request[POST] to [%s] failed", uri), t));
                    });
        });
    }

    private Future<HttpResponse<Buffer>> executeGetRequest(String url) {
        return Future.future(promise -> {
            log.debug("Send request to [{}]", url);
            webClient.getAbs(url)
                    .send(promise);
        });
    }

    private List<VersionInfo> handleVersionInfoResponse(HttpResponse<Buffer> response) {
        log.trace("Handle response [{}]", response);
        val statusCode = response.statusCode();
        if (statusCode == 200) {
            return response.bodyAsJsonArray().stream()
                    .map(o -> Json.decodeValue(o.toString(), VersionInfo.class))
                    .collect(Collectors.toList());
        } else {
            throw new DtmException("Error in receiving version info");
        }
    }
}
