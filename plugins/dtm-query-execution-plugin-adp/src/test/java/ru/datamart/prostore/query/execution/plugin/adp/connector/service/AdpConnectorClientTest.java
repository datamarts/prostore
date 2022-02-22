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
package ru.datamart.prostore.query.execution.plugin.adp.connector.service;

import ru.datamart.prostore.common.exception.DtmException;
import ru.datamart.prostore.common.version.VersionInfo;
import ru.datamart.prostore.query.execution.plugin.adp.base.properties.AdpMpprProperties;
import ru.datamart.prostore.query.execution.plugin.adp.base.properties.AdpMppwProperties;
import ru.datamart.prostore.query.execution.plugin.adp.connector.dto.*;
import ru.datamart.prostore.query.execution.plugin.api.exception.DataSourceException;
import io.vertx.core.Future;
import io.vertx.core.Promise;
import io.vertx.core.buffer.Buffer;
import io.vertx.core.buffer.impl.BufferImpl;
import io.vertx.core.http.HttpVersion;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.web.client.*;
import io.vertx.ext.web.client.impl.HttpResponseImpl;
import io.vertx.junit5.VertxExtension;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.*;

import java.util.Collections;
import java.util.List;

import static org.assertj.core.api.AssertionsForClassTypes.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.when;

@ExtendWith(VertxExtension.class)
public class AdpConnectorClientTest {

    private static final String START_MPPW_URI = "http://localhost/newdata/start";
    private static final String STOP_MPPW_URI = "http://localhost/newdata/stop";
    private static final String RUN_MPPR_URI = "http://localhost/query";
    private static final String MPPW_VERSION_URI = "http://localhost:8086/version";
    private static final String MPPR_VERSION_URI = "http://localhost:8087/version";

    @Mock
    private AdpMppwProperties mppwProperties;
    @Mock
    private AdpMpprProperties mpprProperties;
    @Mock
    private WebClient webClient;
    @Mock
    private HttpRequest httpRequest;

    @InjectMocks
    private AdpConnectorClient adpConnectorClient;

    private AdpConnectorMppwStartRequest mppwStartRequest;
    private AdpConnectorMppwStopRequest mppwStopRequest;
    private AdpConnectorMpprRequest mpprRequest;

    @BeforeEach
    public void setUp() {
        MockitoAnnotations.initMocks(this);

        mppwStartRequest = AdpConnectorMppwStartRequest
                .builder()
                .consumerGroup("consumer-group")
                .datamart("datamart")
                .format("avro")
                .kafkaBrokers(Collections.emptyList())
                .kafkaTopic("kafka-topic")
                .requestId("request-id")
                .tableName("table-name")
                .build();
        mppwStopRequest = new AdpConnectorMppwStopRequest("request-id", "kafka-topic");
        mpprRequest = AdpConnectorMpprRequest
                .builder()
                .requestId("request-id")
                .chunkSize(25)
                .datamart("datamart")
                .kafkaTopic("kafka-topic")
                .table("table")
                .build();

        when(mppwProperties.getRestStartLoadUrl()).thenReturn(START_MPPW_URI);
        when(mppwProperties.getRestStopLoadUrl()).thenReturn(STOP_MPPW_URI);
        when(mpprProperties.getRestLoadUrl()).thenReturn(RUN_MPPR_URI);
        when(mppwProperties.getRestVersionUrl()).thenReturn(MPPW_VERSION_URI);
        when(mpprProperties.getRestVersionUrl()).thenReturn(MPPR_VERSION_URI);
        when(webClient.postAbs(START_MPPW_URI)).thenReturn(httpRequest);
        when(webClient.postAbs(STOP_MPPW_URI)).thenReturn(httpRequest);
        when(webClient.postAbs(RUN_MPPR_URI)).thenReturn(httpRequest);
        when(webClient.getAbs(MPPW_VERSION_URI)).thenReturn(httpRequest);
        when(webClient.getAbs(MPPR_VERSION_URI)).thenReturn(httpRequest);
    }

    @Test
    public void testStartMppwSuccess() {
        HttpResponse mppwStartResponse = new HttpResponseImpl(
                HttpVersion.HTTP_2,
                200,
                "OK",
                null,
                null,
                Collections.emptyList(),
                "",
                Collections.emptyList()
        );

        Future<HttpResponse> responseFuture = Future.succeededFuture(mppwStartResponse);
        when(httpRequest.sendJsonObject(JsonObject.mapFrom(mppwStartRequest))).thenReturn(responseFuture);

        adpConnectorClient.startMppw(mppwStartRequest).onComplete(ar -> assertThat(ar.succeeded()).isTrue());
    }

    @Test
    public void testStartMppwBadResponseCode() {
        Buffer body = new BufferImpl();
        body.appendString("Error message", "UTF-8");
        HttpResponse mppwStartResponse = new HttpResponseImpl(
                HttpVersion.HTTP_2,
                500,
                "Internal Server Error",
                null,
                null,
                Collections.emptyList(),
                body,
                Collections.emptyList()
        );

        Future<HttpResponse> responseFuture = Future.succeededFuture(mppwStartResponse);
        when(httpRequest.sendJsonObject(JsonObject.mapFrom(mppwStartRequest))).thenReturn(responseFuture);

        adpConnectorClient.startMppw(mppwStartRequest).onComplete(ar -> {
            assertThat(ar.failed()).isTrue();
            assertThat(ar.cause()).isInstanceOf(DataSourceException.class);
            assertThat(ar.cause().getMessage()).isEqualToNormalizingNewlines("Error message");
        });
    }

    @Test
    public void testStartMppwFailedRequest() {
        when(httpRequest.sendJsonObject(JsonObject.mapFrom(mppwStartRequest))).thenReturn(Future.failedFuture(new RuntimeException()));

        adpConnectorClient.startMppw(mppwStartRequest).onComplete(ar -> {
            assertThat(ar.failed()).isTrue();
            assertThat(ar.cause()).isInstanceOf(DataSourceException.class);
            assertThat(ar.cause().getMessage()).isEqualToNormalizingNewlines("Request[POST] to [" + START_MPPW_URI + "] failed");
        });
    }

    @Test
    public void testStopMppwSuccess() {
        HttpResponse mppwStopResponse = new HttpResponseImpl(
                HttpVersion.HTTP_2,
                200,
                "OK",
                null,
                null,
                Collections.emptyList(),
                "",
                Collections.emptyList()
        );

        Future<HttpResponse> responseFuture = Future.succeededFuture(mppwStopResponse);
        when(httpRequest.sendJsonObject(JsonObject.mapFrom(mppwStopRequest))).thenReturn(responseFuture);

        adpConnectorClient.stopMppw(mppwStopRequest).onComplete(ar -> assertThat(ar.succeeded()).isTrue());
    }

    @Test
    public void testStopMppwBadResponseCode() {
        Buffer body = new BufferImpl();
        body.appendString("Error message", "UTF-8");
        HttpResponse mppwStopResponse = new HttpResponseImpl(
                HttpVersion.HTTP_2,
                500,
                "Internal Server Error",
                null,
                null,
                Collections.emptyList(),
                body,
                Collections.emptyList()
        );

        Future<HttpResponse> responseFuture = Future.succeededFuture(mppwStopResponse);
        when(httpRequest.sendJsonObject(JsonObject.mapFrom(mppwStopRequest))).thenReturn(responseFuture);

        adpConnectorClient.stopMppw(mppwStopRequest).onComplete(ar -> {
            assertThat(ar.failed()).isTrue();
            assertThat(ar.cause()).isInstanceOf(DataSourceException.class);
            assertThat(ar.cause().getMessage()).isEqualToNormalizingNewlines("Error message");
        });
    }

    @Test
    public void testStopMppwFailedRequest() {
        when(httpRequest.sendJsonObject(JsonObject.mapFrom(mppwStopRequest))).thenReturn(Future.failedFuture(new RuntimeException()));

        adpConnectorClient.stopMppw(mppwStopRequest).onComplete(ar -> {
            assertThat(ar.failed()).isTrue();
            assertThat(ar.cause()).isInstanceOf(DataSourceException.class);
            assertThat(ar.cause().getMessage()).isEqualToNormalizingNewlines("Request[POST] to [" + STOP_MPPW_URI + "] failed");
        });
    }

    @Test
    public void testMpprSuccess() {
        HttpResponse mpprResponse = new HttpResponseImpl(
                HttpVersion.HTTP_2,
                200,
                "OK",
                null,
                null,
                Collections.emptyList(),
                "",
                Collections.emptyList()
        );

        Future<HttpResponse> responseFuture = Future.succeededFuture(mpprResponse);
        when(httpRequest.sendJsonObject(JsonObject.mapFrom(mpprRequest))).thenReturn(responseFuture);

        adpConnectorClient.runMppr(mpprRequest).onComplete(ar -> assertThat(ar.succeeded()).isTrue());
    }

    @Test
    public void testMpprBadResponseCode() {
        Buffer body = new BufferImpl();
        body.appendString("Error message", "UTF-8");
        HttpResponse mpprResponse = new HttpResponseImpl(
                HttpVersion.HTTP_2,
                500,
                "Internal Server Error",
                null,
                null,
                Collections.emptyList(),
                body,
                Collections.emptyList()
        );

        Future<HttpResponse> responseFuture = Future.succeededFuture(mpprResponse);
        when(httpRequest.sendJsonObject(JsonObject.mapFrom(mpprRequest))).thenReturn(responseFuture);

        adpConnectorClient.runMppr(mpprRequest).onComplete(ar -> {
            assertThat(ar.failed()).isTrue();
            assertThat(ar.cause()).isInstanceOf(DataSourceException.class);
            assertThat(ar.cause().getMessage()).isEqualToNormalizingNewlines("Error message");
        });
    }

    @Test
    public void testMpprFailedRequest() {
        when(httpRequest.sendJsonObject(JsonObject.mapFrom(mpprRequest))).thenReturn(Future.failedFuture(new RuntimeException()));

        adpConnectorClient.runMppr(mpprRequest).onComplete(ar -> {
            assertThat(ar.failed()).isTrue();
            assertThat(ar.cause()).isInstanceOf(DataSourceException.class);
            assertThat(ar.cause().getMessage()).isEqualToNormalizingNewlines("Request[POST] to [" + RUN_MPPR_URI + "] failed");
        });
    }

    @Test
    public void testGetMppwVersion() {
        Buffer body = new BufferImpl();
        body.appendString("[{\"name\": \"component-name\", \"version\": \"component-version\"}]", "UTF-8");
        HttpResponse mppwVersionResponse = new HttpResponseImpl(
                HttpVersion.HTTP_2,
                200,
                "OK",
                null,
                null,
                Collections.emptyList(),
                body,
                Collections.emptyList()
        );

        doAnswer(invocation -> {
            Promise promise = invocation.getArgument(0);
            promise.complete(mppwVersionResponse);
            return null;
        }).when(httpRequest).send(any());

        adpConnectorClient.getMppwVersion().onComplete(ar -> {
            assertThat(ar.succeeded()).isTrue();
            List<VersionInfo> versions = ar.result();
            assertThat(versions.size()).isEqualTo(1);
            assertThat(versions.get(0).getName()).isEqualTo("component-name");
            assertThat(versions.get(0).getVersion()).isEqualTo("component-version");
        });
    }

    @Test
    public void testGetMppwVersionFailed() {
        doAnswer(invocation -> {
            Promise promise = invocation.getArgument(0);
            promise.fail(new RuntimeException());
            return null;
        }).when(httpRequest).send(any());

        adpConnectorClient.getMppwVersion().onComplete(ar -> {
            assertThat(ar.failed()).isTrue();
        });
    }

    @Test
    public void testGetMppwVersionBadResponseCode() {
        HttpResponse mppwVersionResponse = new HttpResponseImpl(
                HttpVersion.HTTP_2,
                404,
                "Not Found",
                null,
                null,
                Collections.emptyList(),
                null,
                Collections.emptyList()
        );

        doAnswer(invocation -> {
            Promise promise = invocation.getArgument(0);
            promise.complete(mppwVersionResponse);
            return null;
        }).when(httpRequest).send(any());

        adpConnectorClient.getMppwVersion().onComplete(ar -> {
            assertThat(ar.failed()).isTrue();
            assertThat(ar.cause()).isInstanceOf(DtmException.class);
            assertThat(ar.cause().getMessage()).isEqualTo("Error in receiving version info");
        });
    }

    @Test
    public void testGetMpprVersion() {
        Buffer body = new BufferImpl();
        body.appendString("[{\"name\": \"component-name\", \"version\": \"component-version\"}]", "UTF-8");
        HttpResponse mpprVersionResponse = new HttpResponseImpl(
                HttpVersion.HTTP_2,
                200,
                "OK",
                null,
                null,
                Collections.emptyList(),
                body,
                Collections.emptyList()
        );

        doAnswer(invocation -> {
            Promise promise = invocation.getArgument(0);
            promise.complete(mpprVersionResponse);
            return null;
        }).when(httpRequest).send(any());

        adpConnectorClient.getMpprVersion().onComplete(ar -> {
            assertThat(ar.succeeded()).isTrue();
            List<VersionInfo> versions = ar.result();
            assertThat(versions.size()).isEqualTo(1);
            assertThat(versions.get(0).getName()).isEqualTo("component-name");
            assertThat(versions.get(0).getVersion()).isEqualTo("component-version");
        });
    }

    @Test
    public void testGetMpprVersionFailed() {
        doAnswer(invocation -> {
            Promise promise = invocation.getArgument(0);
            promise.fail(new RuntimeException());
            return null;
        }).when(httpRequest).send(any());

        adpConnectorClient.getMpprVersion().onComplete(ar -> {
            assertThat(ar.failed()).isTrue();
        });
    }

    @Test
    public void testGetMpprVersionBadResponseCode() {
        HttpResponse mpprVersionResponse = new HttpResponseImpl(
                HttpVersion.HTTP_2,
                404,
                "Not Found",
                null,
                null,
                Collections.emptyList(),
                null,
                Collections.emptyList()
        );

        doAnswer(invocation -> {
            Promise promise = invocation.getArgument(0);
            promise.complete(mpprVersionResponse);
            return null;
        }).when(httpRequest).send(any());

        adpConnectorClient.getMpprVersion().onComplete(ar -> {
            assertThat(ar.failed()).isTrue();
            assertThat(ar.cause()).isInstanceOf(DtmException.class);
            assertThat(ar.cause().getMessage()).isEqualTo("Error in receiving version info");
        });
    }

}
