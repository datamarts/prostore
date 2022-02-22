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
import ru.datamart.prostore.common.version.VersionInfo;
import ru.datamart.prostore.query.execution.plugin.adg.base.configuration.properties.TarantoolCartridgeProperties;
import ru.datamart.prostore.query.execution.plugin.adg.base.model.cartridge.request.AdgLoadDataKafkaRequest;
import ru.datamart.prostore.query.execution.plugin.adg.base.model.cartridge.request.AdgSubscriptionKafkaRequest;
import ru.datamart.prostore.query.execution.plugin.adg.base.model.cartridge.request.AdgUploadDataKafkaRequest;
import ru.datamart.prostore.query.execution.plugin.adg.base.model.cartridge.response.AdgCartridgeError;
import ru.datamart.prostore.query.execution.plugin.adg.base.model.cartridge.response.TtLoadDataKafkaError;
import ru.datamart.prostore.query.execution.plugin.api.exception.DataSourceException;
import io.vertx.circuitbreaker.CircuitBreaker;
import io.vertx.core.Vertx;
import io.vertx.ext.web.client.WebClient;
import io.vertx.junit5.VertxExtension;
import io.vertx.junit5.VertxTestContext;
import lombok.val;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.util.Arrays;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.*;

@ExtendWith({VertxExtension.class, MockitoExtension.class})
class AdgCatridgeClientTest {

    private static final String CATRIDGE_URL = "http://localhost:8086";
    private static final AdgCartridgeError CODE_500_EXPECTED_ERROR = new AdgCartridgeError("500", "error_message");
    private static final AdgCartridgeError CODE_404_EXPECTED_ERROR = new AdgCartridgeError("404", "error_message");
    private static final String CODE_500_ERROR_RESPONSE_BODY = "{\"code\":\"500\",\"message\":\"error_message\"}";
    private static final String CODE_404_ERROR_RESPONSE_BODY = "{\"code\":\"404\",\"message\":\"error_message\"}";
    private static final String UNEXPECTED_ERROR_RESPONSE_BODY = "{\"message\":\"error_message\"}";
    private static final String UNEXPECTED_ERROR_EXCEPTION_MESSAGE = "Unexpected response " + UNEXPECTED_ERROR_RESPONSE_BODY;

    @Mock
    private TarantoolCartridgeProperties cartridgeProperties;
    @Mock
    private CircuitBreaker circuitBreaker;

    private final ObjectMapper mapper = new ObjectMapper();

    private AdgCartridgeClient cartridgeClient;

    @BeforeEach
    void setUp() {
        when(cartridgeProperties.getUrl()).thenReturn(CATRIDGE_URL);
    }

    @Test
    void getCheckVersionsSuccess(Vertx vertx, VertxTestContext testContext) throws JsonProcessingException {
        val firstVersionInfo = new VersionInfo("catridge_name1", "catridge_version1");
        val secondVersionInfo = new VersionInfo("catridge_name2", "catridge_version2");
        val checkVersionsResponse = mapper.writeValueAsString(Arrays.asList(firstVersionInfo, secondVersionInfo));
        vertx.createHttpServer()
                .requestHandler(req -> req.response().end(checkVersionsResponse))
                .listen(8086)
                .onComplete(testContext.succeeding(httpServer -> {
                    val client = WebClient.create(vertx);
                    cartridgeClient = new AdgCartridgeClient(cartridgeProperties, client, circuitBreaker, mapper);

                    when(cartridgeProperties.getCheckVersionsUrl()).thenReturn("/versions");

                    cartridgeClient.getCheckVersions()
                            .onComplete(testContext.succeeding(list ->
                                    testContext.verify(() -> {
                                        assertEquals(2, list.size());
                                        assertThat(firstVersionInfo).isEqualToComparingFieldByField(list.get(0));
                                        assertThat(secondVersionInfo).isEqualToComparingFieldByField(list.get(1));
                                        verify(cartridgeProperties).getUrl();
                                        verify(cartridgeProperties).getCheckVersionsUrl();
                                        verifyNoMoreInteractions(cartridgeProperties);
                                    }).completeNow()));
                }));
    }

    @Test
    void getCheckVersions500StatusFail(Vertx vertx, VertxTestContext testContext) {
        vertx.createHttpServer()
                .requestHandler(req -> req.response().setStatusCode(500).end(CODE_500_ERROR_RESPONSE_BODY))
                .listen(8086)
                .onComplete(testContext.succeeding(httpServer -> {
                    val client = WebClient.create(vertx);
                    cartridgeClient = new AdgCartridgeClient(cartridgeProperties, client, circuitBreaker, mapper);

                    when(cartridgeProperties.getCheckVersionsUrl()).thenReturn("/versions");

                    cartridgeClient.getCheckVersions()
                            .onComplete(testContext.failing(error ->
                                    testContext.verify(() -> {
                                        assertTrue(error instanceof AdgCartridgeError);
                                        assertThat(error).isEqualToComparingFieldByField(CODE_500_EXPECTED_ERROR);
                                        verify(cartridgeProperties).getUrl();
                                        verify(cartridgeProperties).getCheckVersionsUrl();
                                        verifyNoMoreInteractions(cartridgeProperties);
                                    }).completeNow()));
                }));
    }

    @Test
    void getCheckVersionsUnexpectedStatusFail(Vertx vertx, VertxTestContext testContext) {
        vertx.createHttpServer()
                .requestHandler(req -> req.response().setStatusCode(400).end(UNEXPECTED_ERROR_RESPONSE_BODY))
                .listen(8086)
                .onComplete(testContext.succeeding(httpServer -> {
                    val client = WebClient.create(vertx);
                    cartridgeClient = new AdgCartridgeClient(cartridgeProperties, client, circuitBreaker, mapper);

                    when(cartridgeProperties.getCheckVersionsUrl()).thenReturn("/versions");

                    cartridgeClient.getCheckVersions()
                            .onComplete(testContext.failing(error ->
                                    testContext.verify(() -> {
                                        assertTrue(error instanceof DataSourceException);
                                        assertEquals(UNEXPECTED_ERROR_EXCEPTION_MESSAGE, error.getMessage());
                                        verify(cartridgeProperties).getUrl();
                                        verify(cartridgeProperties).getCheckVersionsUrl();
                                        verifyNoMoreInteractions(cartridgeProperties);
                                    }).completeNow()));
                }));
    }

    @Test
    void uploadDataSuccess(Vertx vertx, VertxTestContext testContext) {
        vertx.createHttpServer()
                .requestHandler(req -> req.response().end())
                .listen(8086)
                .onComplete(testContext.succeeding(httpServer -> {
                    val client = WebClient.create(vertx);
                    cartridgeClient = new AdgCartridgeClient(cartridgeProperties, client, circuitBreaker, mapper);

                    when(cartridgeProperties.getKafkaUploadDataUrl()).thenReturn("/upload_data");

                    cartridgeClient.uploadData(new AdgUploadDataKafkaRequest())
                            .onComplete(testContext.succeeding(list ->
                                    testContext.verify(() -> {
                                        verify(cartridgeProperties).getUrl();
                                        verify(cartridgeProperties).getKafkaUploadDataUrl();
                                        verifyNoMoreInteractions(cartridgeProperties);
                                    }).completeNow()));
                }));
    }

    @Test
    void uploadData500StatusFail(Vertx vertx, VertxTestContext testContext) {
        vertx.createHttpServer()
                .requestHandler(req -> req.response().setStatusCode(500).end(CODE_500_ERROR_RESPONSE_BODY))
                .listen(8086)
                .onComplete(testContext.succeeding(httpServer -> {
                    val client = WebClient.create(vertx);
                    cartridgeClient = new AdgCartridgeClient(cartridgeProperties, client, circuitBreaker, mapper);

                    when(cartridgeProperties.getKafkaUploadDataUrl()).thenReturn("/upload_data");

                    cartridgeClient.uploadData(new AdgUploadDataKafkaRequest())
                            .onComplete(testContext.failing(error ->
                                    testContext.verify(() -> {
                                        assertTrue(error instanceof AdgCartridgeError);
                                        assertThat(error).isEqualToComparingFieldByField(CODE_500_EXPECTED_ERROR);
                                        verify(cartridgeProperties).getUrl();
                                        verify(cartridgeProperties).getKafkaUploadDataUrl();
                                        verifyNoMoreInteractions(cartridgeProperties);
                                    }).completeNow()));
                }));
    }

    @Test
    void uploadDataUnexpectedStatusFail(Vertx vertx, VertxTestContext testContext) {
        vertx.createHttpServer()
                .requestHandler(req -> req.response().setStatusCode(400).end(UNEXPECTED_ERROR_RESPONSE_BODY))
                .listen(8086)
                .onComplete(testContext.succeeding(httpServer -> {
                    val client = WebClient.create(vertx);
                    cartridgeClient = new AdgCartridgeClient(cartridgeProperties, client, circuitBreaker, mapper);

                    when(cartridgeProperties.getKafkaUploadDataUrl()).thenReturn("/upload_data");

                    cartridgeClient.uploadData(new AdgUploadDataKafkaRequest())
                            .onComplete(testContext.failing(error ->
                                    testContext.verify(() -> {
                                        assertTrue(error instanceof DataSourceException);
                                        assertEquals(UNEXPECTED_ERROR_EXCEPTION_MESSAGE, error.getMessage());
                                        verify(cartridgeProperties).getUrl();
                                        verify(cartridgeProperties).getKafkaUploadDataUrl();
                                        verifyNoMoreInteractions(cartridgeProperties);
                                    }).completeNow()));
                }));
    }

    @Test
    void subscribeSuccess(Vertx vertx, VertxTestContext testContext) {
        vertx.createHttpServer()
                .requestHandler(req -> req.response().end())
                .listen(8086)
                .onComplete(testContext.succeeding(httpServer -> {
                    val client = WebClient.create(vertx);
                    cartridgeClient = new AdgCartridgeClient(cartridgeProperties, client, circuitBreaker, mapper);

                    when(cartridgeProperties.getKafkaSubscriptionUrl()).thenReturn("/sub");

                    cartridgeClient.subscribe(new AdgSubscriptionKafkaRequest())
                            .onComplete(testContext.succeeding(list ->
                                    testContext.verify(() -> {
                                        verify(cartridgeProperties).getUrl();
                                        verify(cartridgeProperties).getKafkaSubscriptionUrl();
                                        verifyNoMoreInteractions(cartridgeProperties);
                                    }).completeNow()));
                }));
    }

    @Test
    void subscribe500StatusFail(Vertx vertx, VertxTestContext testContext) {
        vertx.createHttpServer()
                .requestHandler(req -> req.response().setStatusCode(500).end(CODE_500_ERROR_RESPONSE_BODY))
                .listen(8086)
                .onComplete(testContext.succeeding(httpServer -> {
                    val client = WebClient.create(vertx);
                    cartridgeClient = new AdgCartridgeClient(cartridgeProperties, client, circuitBreaker, mapper);

                    when(cartridgeProperties.getKafkaSubscriptionUrl()).thenReturn("/sub");

                    cartridgeClient.subscribe(new AdgSubscriptionKafkaRequest())
                            .onComplete(testContext.failing(error ->
                                    testContext.verify(() -> {
                                        assertTrue(error instanceof AdgCartridgeError);
                                        assertThat(error).isEqualToComparingFieldByField(CODE_500_EXPECTED_ERROR);
                                        verify(cartridgeProperties).getUrl();
                                        verify(cartridgeProperties).getKafkaSubscriptionUrl();
                                        verifyNoMoreInteractions(cartridgeProperties);
                                    }).completeNow()));
                }));
    }

    @Test
    void subscribeUnexpectedStatusFail(Vertx vertx, VertxTestContext testContext) {
        vertx.createHttpServer()
                .requestHandler(req -> req.response().setStatusCode(400).end(UNEXPECTED_ERROR_RESPONSE_BODY))
                .listen(8086)
                .onComplete(testContext.succeeding(httpServer -> {
                    val client = WebClient.create(vertx);
                    cartridgeClient = new AdgCartridgeClient(cartridgeProperties, client, circuitBreaker, mapper);

                    when(cartridgeProperties.getKafkaSubscriptionUrl()).thenReturn("/sub");

                    cartridgeClient.subscribe(new AdgSubscriptionKafkaRequest())
                            .onComplete(testContext.failing(error ->
                                    testContext.verify(() -> {
                                        assertTrue(error instanceof DataSourceException);
                                        assertEquals(UNEXPECTED_ERROR_EXCEPTION_MESSAGE, error.getMessage());
                                        verify(cartridgeProperties).getUrl();
                                        verify(cartridgeProperties).getKafkaSubscriptionUrl();
                                        verifyNoMoreInteractions(cartridgeProperties);
                                    }).completeNow()));
                }));
    }

    @Test
    void loadDataSuccess(Vertx vertx, VertxTestContext testContext) {
        vertx.createHttpServer()
                .requestHandler(req -> req.response().end())
                .listen(8086)
                .onComplete(testContext.succeeding(httpServer -> {
                    val client = WebClient.create(vertx);
                    cartridgeClient = new AdgCartridgeClient(cartridgeProperties, client, circuitBreaker, mapper);

                    when(cartridgeProperties.getKafkaLoadDataUrl()).thenReturn("/load_data");

                    cartridgeClient.loadData(new AdgLoadDataKafkaRequest())
                            .onComplete(testContext.succeeding(list ->
                                    testContext.verify(() -> {
                                        verify(cartridgeProperties).getUrl();
                                        verify(cartridgeProperties).getKafkaLoadDataUrl();
                                        verifyNoMoreInteractions(cartridgeProperties);
                                    }).completeNow()));
                }));
    }

    @Test
    void loadData500StatusFail(Vertx vertx, VertxTestContext testContext) {
        val ttLoadDataKafkaError = new TtLoadDataKafkaError();
        ttLoadDataKafkaError.setCode("500");
        ttLoadDataKafkaError.setMessage("error_message");
        ttLoadDataKafkaError.setMessageCount(1L);
        val response = "{\"code\":\"500\",\"message\":\"error_message\",\"messageCount\":1}";
        vertx.createHttpServer()
                .requestHandler(req -> req.response().setStatusCode(500).end(response))
                .listen(8086)
                .onComplete(testContext.succeeding(httpServer -> {
                    val client = WebClient.create(vertx);
                    cartridgeClient = new AdgCartridgeClient(cartridgeProperties, client, circuitBreaker, mapper);

                    when(cartridgeProperties.getKafkaLoadDataUrl()).thenReturn("/load_data");

                    cartridgeClient.loadData(new AdgLoadDataKafkaRequest())
                            .onComplete(testContext.failing(error ->
                                    testContext.verify(() -> {
                                        assertTrue(error instanceof TtLoadDataKafkaError);
                                        assertThat(error).isEqualToComparingFieldByField(ttLoadDataKafkaError);
                                        verify(cartridgeProperties).getUrl();
                                        verify(cartridgeProperties).getKafkaLoadDataUrl();
                                        verifyNoMoreInteractions(cartridgeProperties);
                                    }).completeNow()));
                }));
    }

    @Test
    void loadData404StatusFail(Vertx vertx, VertxTestContext testContext) {
        vertx.createHttpServer()
                .requestHandler(req -> req.response().setStatusCode(404).end(CODE_404_ERROR_RESPONSE_BODY))
                .listen(8086)
                .onComplete(testContext.succeeding(httpServer -> {
                    val client = WebClient.create(vertx);
                    cartridgeClient = new AdgCartridgeClient(cartridgeProperties, client, circuitBreaker, mapper);

                    when(cartridgeProperties.getKafkaLoadDataUrl()).thenReturn("/load_data");

                    cartridgeClient.loadData(new AdgLoadDataKafkaRequest())
                            .onComplete(testContext.failing(error ->
                                    testContext.verify(() -> {
                                        assertTrue(error instanceof AdgCartridgeError);
                                        assertThat(error).isEqualToComparingFieldByField(CODE_404_EXPECTED_ERROR);
                                        verify(cartridgeProperties).getUrl();
                                        verify(cartridgeProperties).getKafkaLoadDataUrl();
                                        verifyNoMoreInteractions(cartridgeProperties);
                                    }).completeNow()));
                }));
    }

    @Test
    void loadDataUnexpectedStatusFail(Vertx vertx, VertxTestContext testContext) {
        vertx.createHttpServer()
                .requestHandler(req -> req.response().setStatusCode(400).end(UNEXPECTED_ERROR_RESPONSE_BODY))
                .listen(8086)
                .onComplete(testContext.succeeding(httpServer -> {
                    val client = WebClient.create(vertx);
                    cartridgeClient = new AdgCartridgeClient(cartridgeProperties, client, circuitBreaker, mapper);

                    when(cartridgeProperties.getKafkaLoadDataUrl()).thenReturn("/load_data");

                    cartridgeClient.loadData(new AdgLoadDataKafkaRequest())
                            .onComplete(testContext.failing(error ->
                                    testContext.verify(() -> {
                                        assertTrue(error instanceof DataSourceException);
                                        assertEquals(UNEXPECTED_ERROR_EXCEPTION_MESSAGE, error.getMessage());
                                        verify(cartridgeProperties).getUrl();
                                        verify(cartridgeProperties).getKafkaLoadDataUrl();
                                        verifyNoMoreInteractions(cartridgeProperties);
                                    }).completeNow()));
                }));
    }
}
