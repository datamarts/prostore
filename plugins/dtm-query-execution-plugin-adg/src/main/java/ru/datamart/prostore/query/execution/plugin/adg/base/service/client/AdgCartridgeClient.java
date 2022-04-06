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
package ru.datamart.prostore.query.execution.plugin.adg.base.service.client;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import ru.datamart.prostore.common.exception.DtmException;
import ru.datamart.prostore.common.version.VersionInfo;
import ru.datamart.prostore.query.execution.plugin.adg.base.configuration.properties.TarantoolCartridgeProperties;
import ru.datamart.prostore.query.execution.plugin.adg.base.model.cartridge.OperationFile;
import ru.datamart.prostore.query.execution.plugin.adg.base.model.cartridge.OperationYaml;
import ru.datamart.prostore.query.execution.plugin.adg.base.model.cartridge.request.*;
import ru.datamart.prostore.query.execution.plugin.adg.base.model.cartridge.response.AdgCartridgeError;
import ru.datamart.prostore.query.execution.plugin.adg.base.model.cartridge.response.ResOperation;
import ru.datamart.prostore.query.execution.plugin.adg.base.model.cartridge.response.TtLoadDataKafkaError;
import ru.datamart.prostore.query.execution.plugin.adg.base.model.cartridge.response.TtLoadDataKafkaResponse;
import ru.datamart.prostore.query.execution.plugin.adg.base.model.cartridge.schema.Space;
import ru.datamart.prostore.query.execution.plugin.adg.rollback.dto.ReverseHistoryTransferRequest;
import ru.datamart.prostore.query.execution.plugin.api.exception.DataSourceException;
import io.vertx.circuitbreaker.CircuitBreaker;
import io.vertx.core.Future;
import io.vertx.core.buffer.Buffer;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.web.client.HttpResponse;
import io.vertx.ext.web.client.WebClient;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import lombok.val;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Service;
import org.springframework.util.CollectionUtils;

import java.util.*;
import java.util.function.Function;
import java.util.stream.Collectors;

@Slf4j
@Service
public class AdgCartridgeClient {
    private static final String STAGE_DATA_TABLE_NAME = "_stage_data_table_name";
    private static final String ACTUAL_DATA_TABLE_NAME = "_actual_data_table_name";
    private static final String HISTORICAL_DATA_TABLE_NAME = "_historical_data_table_name";
    private static final String DELTA_NUMBER = "_delta_number";
    private final WebClient webClient;
    private final TarantoolCartridgeProperties cartridgeProperties;
    private final CircuitBreaker circuitBreaker;
    private final ObjectMapper objectMapper;

    @Autowired
    public AdgCartridgeClient(TarantoolCartridgeProperties cartridgeProperties,
                              @Qualifier("adgWebClient") WebClient webClient,
                              @Qualifier("adgCircuitBreaker") CircuitBreaker circuitBreaker,
                              @Qualifier("yamlMapper") ObjectMapper objectMapper) {
        this.cartridgeProperties = cartridgeProperties;
        this.webClient = webClient;
        this.circuitBreaker = circuitBreaker;
        this.objectMapper = objectMapper;
    }

    public Future<ResOperation> getFiles() {
        return executePostRequest(new GetFilesOperation());
    }

    public Future<ResOperation> setFiles(List<OperationFile> files) {
        return executePostRequest(new SetFilesOperation(files));
    }

    public Future<ResOperation> getSchema() {
        return executePostRequest(new GetSchemaOperation());
    }

    public Future<ResOperation> setSchema(String yaml) {
        return executePostRequest(new SetSchemaOperation(yaml));
    }

    public Future<Void> uploadData(AdgUploadDataKafkaRequest request) {
        val uri = cartridgeProperties.getUrl() + cartridgeProperties.getKafkaUploadDataUrl();
        return executePostRequest(uri, request)
                .compose(this::handleUploadData);
    }

    public Future<Void> subscribe(AdgSubscriptionKafkaRequest request) {
        val uri = cartridgeProperties.getUrl() + cartridgeProperties.getKafkaSubscriptionUrl();
        return executePostRequest(uri, request)
                .compose(this::handleSubscription);
    }

    public Future<TtLoadDataKafkaResponse> loadData(AdgLoadDataKafkaRequest request) {
        val uri = cartridgeProperties.getUrl() + cartridgeProperties.getKafkaLoadDataUrl();
        return executePostRequest(uri, request)
                .compose(this::handleLoadData);
    }

    public Future<Void> transferDataToScdTable(AdgTransferDataEtlRequest request) {
        val uri = cartridgeProperties.getUrl() + cartridgeProperties.getTransferDataToScdTableUrl();
        return executeGetTransferDataRequest(uri, request)
                .compose(this::handleTransferData);
    }

    public Future<Void> cancelSubscription(String topicName) {
        val uri = cartridgeProperties.getUrl() + cartridgeProperties.getKafkaSubscriptionUrl() + "/" + topicName;
        return executeDeleteRequest(uri)
                .compose(this::handleCancelSubscription);
    }

    public Future<Map<String, Space>> getSpaceDescriptions(Set<String> spaceNames) {
        Map<String, Set<String>> body = new HashMap<>();
        body.put("spaces", spaceNames);
        val uri = cartridgeProperties.getUrl() + cartridgeProperties.getTableSchemaUrl();
        return executePostRequest(uri, body)
                .compose(response -> parseSpaces(response.bodyAsString(), spaceNames));
    }

    public Future<Void> reverseHistoryTransfer(ReverseHistoryTransferRequest request) {
        val uri = cartridgeProperties.getUrl() + cartridgeProperties.getReverseHistoryTransferUrl();
        return executePostRequestAsJsonObject(uri, request)
                .compose(this::handleReverseHistoryTransfer);
    }

    public Future<Void> executeCreateSpacesQueued(OperationYaml request) {
        val uri = cartridgeProperties.getUrl() + cartridgeProperties.getTableQueuedCreate();
        return executePostRequest(uri, request)
                .compose(this::handleExecuteCreateSpacesQueued);
    }

    public Future<Void> executeDeleteSpacesQueued(AdgDeleteTablesRequest request) {
        val uri = cartridgeProperties.getUrl() + cartridgeProperties.getTableQueuedDelete();
        return executeDeleteRequest(uri, request)
                .compose(this::handleExecuteDeleteSpacesQueued);
    }

    public Future<Void> executeDeleteSpacesWithPrefixQueued(AdgDeleteTablesWithPrefixRequest request) {
        val uri = cartridgeProperties.getUrl() + cartridgeProperties.getTableQueuedDelete() + "/prefix/" +
                request.getTablePrefix();
        return executeDeleteRequest(uri, request)
                .compose(this::handleExecuteDeleteSpacesWithPrefixQueued);
    }

    public Future<Long> getCheckSumByInt32Hash(String actualDataTableName,
                                               String historicalDataTableName,
                                               Long sysCn,
                                               Set<String> columnList,
                                               Long normalization) {
        val body = new HashMap<>();
        body.put("actualDataTableName", actualDataTableName);
        body.put("historicalDataTableName", historicalDataTableName);
        body.put("sysCn", sysCn);
        body.put("columnList", columnList);
        body.put("normalization", normalization);
        val uri = cartridgeProperties.getUrl() + cartridgeProperties.getCheckSumUrl();
        return executePostRequest(uri, body)
                .compose(this::handleCheckSumData);
    }

    public Future<Void> deleteSpaceTuples(String spaceName, String whereCondition) {
        val uri = cartridgeProperties.getUrl() + cartridgeProperties.getDeleteSpaceTuples();
        val body = new HashMap<>();
        body.put("spaceName", spaceName);
        body.put("whereCondition", whereCondition);
        return executePostRequest(uri, body)
                .compose(this::handleDeleteSpaceTuples);
    }

    public Future<Void> truncateSpace(String spaceName) {
        val uri = cartridgeProperties.getUrl() + cartridgeProperties.getTruncateSpace() + "?_space_name=" + spaceName;
        return executeGetRequest(uri)
                .compose(this::handleTruncateSpace);
    }

    public Future<List<VersionInfo>> getCheckVersions() {
        val uri = cartridgeProperties.getUrl() + cartridgeProperties.getCheckVersionsUrl();
        return executeGetRequest(uri)
                .compose(this::handleCheckVersionsTuples);
    }

    @SneakyThrows
    private Future<ResOperation> executePostRequest(ReqOperation reqOperation) {
        final String uri = cartridgeProperties.getUrl() + cartridgeProperties.getAdminApiUrl();
        return circuitBreaker.execute(promise -> executePostRequest(uri, reqOperation)
                .compose(this::handleResOperation)
                .onComplete(promise));
    }

    private Future<HttpResponse<Buffer>> executeGetRequest(String uri) {
        return Future.future(promise -> {
            log.debug("send GET to [{}]", uri);
            webClient.getAbs(uri)
                    .send(promise);
        });
    }

    private Future<HttpResponse<Buffer>> executePostRequest(String uri, Object request) {
        return Future.future(promise -> {
            log.debug("send POST to [{}] request [{}]", uri, request);
            webClient.postAbs(uri)
                    .sendJson(request, promise);
        });
    }

    private Future<HttpResponse<Buffer>> executePostRequestAsJsonObject(String uri, Object request) {
        return Future.future(promise -> {
            log.debug("send POST to [{}] request [{}]", uri, request);
            val data = JsonObject.mapFrom(request);
            webClient.postAbs(uri)
                    .sendJsonObject(data, promise);
        });
    }

    private Future<HttpResponse<Buffer>> executeDeleteRequest(String uri) {
        return Future.future(promise -> {
            log.debug("send DELETE to [{}]", uri);
            webClient.deleteAbs(uri)
                    .send(promise);
        });
    }

    private Future<HttpResponse<Buffer>> executeDeleteRequest(String uri, Object request) {
        return Future.future(promise -> {
            log.debug("send DELETE to [{}] request [{}]", uri, request);
            webClient.deleteAbs(uri)
                    .sendJson(request, promise);
        });
    }


    private Future<HttpResponse<Buffer>> executeGetTransferDataRequest(String uri, AdgTransferDataEtlRequest request) {
        return Future.future(promise -> {
            log.debug("send to [{}] request [{}]", uri, request);
            val tableNames = request.getHelperTableNames();
            webClient.getAbs(uri)
                    .addQueryParam(STAGE_DATA_TABLE_NAME, tableNames.getStaging())
                    .addQueryParam(ACTUAL_DATA_TABLE_NAME, tableNames.getActual())
                    .addQueryParam(HISTORICAL_DATA_TABLE_NAME, tableNames.getHistory())
                    .addQueryParam(DELTA_NUMBER, String.valueOf(request.getDeltaNumber()))
                    .send(promise);
        });
    }

    private Future<ResOperation> handleResOperation(HttpResponse<Buffer> response) {
        return Future.future(promise -> {
            try {
                ResOperation res = new JsonObject(response.body()).mapTo(ResOperation.class);
                if (CollectionUtils.isEmpty(res.getErrors())) {
                    promise.complete(res);
                } else {
                    promise.fail(new DataSourceException(res.getErrors().get(0).getMessage()));
                }
            } catch (Exception e) {
                promise.fail(new DataSourceException("Error in decoding response operation", e));
            }
        });
    }

    private Future<Void> handleUploadData(HttpResponse<Buffer> response) {
        return Future.future(promise -> {
            log.trace("handle [UploadData] statusCode [{}] response [{}]", response.statusCode(), response.bodyAsString());
            val statusCode = response.statusCode();
            if (statusCode == 200) {
                promise.complete();
                log.debug("UploadData Successful");
            } else if (statusCode == 500) {
                promise.fail(response.bodyAsJson(AdgCartridgeError.class));
            } else {
                promise.fail(unexpectedResponse(response));
            }
        });
    }

    private Future<Void> handleSubscription(HttpResponse<Buffer> response) {
        return Future.future(promise -> {
            log.trace("handle [subscription] statusCode [{}] response [{}]", response.statusCode(), response.bodyAsString());
            val statusCode = response.statusCode();
            if (statusCode == 200) {
                log.debug("Subscription Successful");
                promise.complete();
            } else if (statusCode == 500) {
                promise.fail(response.bodyAsJson(AdgCartridgeError.class));
            } else {
                promise.fail(unexpectedResponse(response));
            }
        });
    }

    private Future<Void> handleTransferData(HttpResponse<Buffer> response) {
        return Future.future(promise -> {
            log.trace("handle [transfer data to scd table] statusCode [{}] response [{}]", response.statusCode(), response.bodyAsString());
            val statusCode = response.statusCode();
            if (statusCode == 200) {
                promise.complete();
            } else {
                promise.fail(unexpectedResponse(response));
            }
        });
    }

    private Future<TtLoadDataKafkaResponse> handleLoadData(HttpResponse<Buffer> response) {
        return Future.future(promise -> {
            log.trace("handle [load data] statusCode [{}] response [{}]", response.statusCode(), response.bodyAsString());
            val statusCode = response.statusCode();
            if (statusCode == 200) {
                val successResponse = response.bodyAsJson(TtLoadDataKafkaResponse.class);
                promise.complete(successResponse);
                log.debug("Loading Successful");
            } else if (statusCode == 500) {
                promise.fail(response.bodyAsJson(TtLoadDataKafkaError.class));
            } else if (statusCode == 404) {
                promise.fail(response.bodyAsJson(AdgCartridgeError.class));
            } else {
                promise.fail(unexpectedResponse(response));
            }
        });
    }

    private Future<Void> handleCancelSubscription(HttpResponse<Buffer> response) {
        return Future.future(promise -> {
            log.trace("handle [cancel subscription] statusCode [{}] response [{}]", response.statusCode(), response.bodyAsString());
            val statusCode = response.statusCode();
            if (statusCode == 200) {
                promise.complete();
            } else if (statusCode == 404 || statusCode == 500) {
                promise.fail(response.bodyAsJson(AdgCartridgeError.class));
            } else {
                promise.fail(unexpectedResponse(response));
            }
        });
    }

    private Future<Map<String, Space>> parseSpaces(String yaml, Set<String> spaceNames) {
        return Future.future(promise -> {
            try {
                val jsonNode = objectMapper.readTree(yaml).get("spaces");
                promise.complete(spaceNames.stream()
                        .filter(name -> jsonNode.get(name) != null)
                        .collect(Collectors.toMap(Function.identity(), name -> objectMapper.convertValue(jsonNode.get(name), Space.class))));
            } catch (JsonProcessingException e) {
                promise.fail(new DataSourceException("Error in deserializing json node from yaml", e));
            }
        });
    }

    private Future<Void> handleReverseHistoryTransfer(HttpResponse<Buffer> response) {
        return Future.future(promise -> {
            log.trace("handle [reverse history transfer] statusCode [{}] response [{}]", response.statusCode(), response.bodyAsString());
            val statusCode = response.statusCode();
            if (statusCode == 200) {
                promise.complete();
            } else if (statusCode == 404 || statusCode == 500) {
                promise.fail(response.bodyAsJson(AdgCartridgeError.class));
            } else {
                promise.fail(unexpectedResponse(response));
            }
        });
    }

    private Future<Void> handleExecuteCreateSpacesQueued(HttpResponse<Buffer> response) {
        return Future.future(promise -> {
            log.trace("handle [executeCreateSpacesQueued] statusCode [{}] response [{}]", response.statusCode(), response.bodyAsString());
            val statusCode = response.statusCode();
            if (statusCode == 200) {
                promise.complete();
            } else if (statusCode == 500) {
                promise.fail(response.bodyAsJson((AdgCartridgeError.class)));
            } else {
                promise.fail(unexpectedResponse(response));
            }
        });
    }

    private Future<Void> handleExecuteDeleteSpacesQueued(HttpResponse<Buffer> response) {
        return Future.future(promise -> {
            log.trace("handle [executeDeleteSpacesQueued] statusCode [{}] response [{}]", response.statusCode(), response.bodyAsString());
            val statusCode = response.statusCode();
            if (statusCode == 200) {
                promise.complete();
            } else if (statusCode == 500) {
                promise.fail(response.bodyAsJson((AdgCartridgeError.class)));
            } else {
                promise.fail(unexpectedResponse(response));
            }
        });
    }

    private Future<Void> handleExecuteDeleteSpacesWithPrefixQueued(HttpResponse<Buffer> response) {
        return Future.future(promise -> {
            log.trace("handle [executeDeleteSpacesWithPrefixQueued] statusCode [{}] response [{}]", response.statusCode(), response.bodyAsString());
            val statusCode = response.statusCode();
            if (statusCode == 200) {
                promise.complete();
            } else if (statusCode == 500) {
                promise.fail(response.bodyAsJson((AdgCartridgeError.class)));
            } else {
                promise.fail(unexpectedResponse(response));
            }
        });
    }

    private Future<Long> handleCheckSumData(HttpResponse<Buffer> response) {
        return Future.future(promise -> {
            JsonObject jsonObject = response.bodyAsJsonObject();
            Optional<Long> checkSum = Optional.ofNullable(jsonObject.getLong("checksum"));
            if (checkSum.isPresent()) {
                promise.complete(checkSum.get());
            } else {
                promise.fail(new DataSourceException(jsonObject.getString("error")));
            }
        });
    }

    private Future<Void> handleDeleteSpaceTuples(HttpResponse<Buffer> response) {
        return Future.future(promise -> {
            JsonObject jsonObject = response.bodyAsJsonObject();
            if (jsonObject == null) {
                promise.complete();
            } else {
                promise.fail(new DataSourceException(jsonObject.getString("error")));
            }
        });
    }

    private Future<List<VersionInfo>> handleCheckVersionsTuples(HttpResponse<Buffer> response) {
        return Future.future(promise -> {
            log.trace("handle [checkVersions] response [{}]", response);
            val statusCode = response.statusCode();
            if (statusCode == 200) {
                promise.complete(Arrays.asList(response.bodyAsJson(VersionInfo[].class)));
            } else if (statusCode == 500) {
                promise.fail(response.bodyAsJson(AdgCartridgeError.class));
            } else {
                promise.fail(unexpectedResponse(response));
            }
        });
    }

    private Future<Void> handleTruncateSpace(HttpResponse<Buffer> response) {
        return Future.future(promise -> {
            log.trace("handle [truncateSpace] statusCode [{}] response [{}]", response.statusCode(), response.bodyAsString());
            val statusCode = response.statusCode();
            if (statusCode == 200) {
                promise.complete();
            } else if (statusCode == 500) {
                promise.fail(response.bodyAsJson((AdgCartridgeError.class)));
            } else {
                promise.fail(unexpectedResponse(response));
            }
        });
    }

    private DtmException unexpectedResponse(HttpResponse<Buffer> response) {
        String failureMessage = String.format("Unexpected response %s", response.bodyAsJsonObject());
        return new DataSourceException(failureMessage);
    }
}
