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
package ru.datamart.prostore.query.execution.core.check;

import ru.datamart.prostore.common.exception.DtmException;
import ru.datamart.prostore.common.metrics.RequestMetrics;
import ru.datamart.prostore.common.model.ddl.ColumnType;
import ru.datamart.prostore.common.reader.QueryRequest;
import ru.datamart.prostore.common.reader.QueryResult;
import ru.datamart.prostore.common.reader.SourceType;
import ru.datamart.prostore.common.request.DatamartRequest;
import ru.datamart.prostore.common.version.VersionInfo;
import ru.datamart.prostore.kafka.core.configuration.properties.KafkaProperties;
import ru.datamart.prostore.kafka.core.configuration.properties.KafkaStatusMonitorProperties;
import ru.datamart.prostore.query.calcite.core.extension.check.CheckType;
import ru.datamart.prostore.query.calcite.core.extension.check.SqlCheckVersions;
import ru.datamart.prostore.query.execution.core.check.dto.CheckContext;
import ru.datamart.prostore.query.execution.core.check.factory.CheckVersionQueryResultFactory;
import ru.datamart.prostore.query.execution.core.check.service.impl.CheckVersionsExecutor;
import ru.datamart.prostore.query.execution.core.plugin.configuration.properties.ActivePluginsProperties;
import ru.datamart.prostore.query.execution.core.plugin.service.DataSourcePluginService;
import ru.datamart.prostore.query.execution.core.plugin.service.impl.DataSourcePluginServiceImpl;
import ru.datamart.prostore.query.execution.model.metadata.ColumnMetadata;
import io.vertx.core.AsyncResult;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import io.vertx.core.MultiMap;
import io.vertx.core.buffer.Buffer;
import io.vertx.core.http.HttpMethod;
import io.vertx.core.http.HttpVersion;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import io.vertx.core.streams.ReadStream;
import io.vertx.ext.auth.authentication.Credentials;
import io.vertx.ext.web.client.HttpRequest;
import io.vertx.ext.web.client.HttpResponse;
import io.vertx.ext.web.client.WebClient;
import io.vertx.ext.web.client.predicate.ResponsePredicate;
import io.vertx.ext.web.client.predicate.ResponsePredicateResult;
import io.vertx.ext.web.codec.BodyCodec;
import io.vertx.ext.web.multipart.MultipartForm;
import org.jetbrains.annotations.NotNull;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;
import org.springframework.boot.info.BuildProperties;

import java.util.*;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static ru.datamart.prostore.query.execution.core.check.factory.CheckVersionQueryResultFactory.COMPONENT_NAME_COLUMN;
import static ru.datamart.prostore.query.execution.core.check.factory.CheckVersionQueryResultFactory.VERSION_COLUMN;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class CheckVersionsExecutorTest {
    private final DataSourcePluginService dataSourcePluginService = mock(DataSourcePluginServiceImpl.class);
    private final static Set<SourceType> SOURCE_TYPES = Stream.of(SourceType.ADB, SourceType.ADG, SourceType.ADQM)
            .collect(Collectors.toSet());
    private final CheckVersionQueryResultFactory queryResultFactory = mock(CheckVersionQueryResultFactory.class);
    private final WebClient webClient = mock(WebClient.class);
    private final ActivePluginsProperties activePluginsProperties = new ActivePluginsProperties();
    private final KafkaProperties kafkaProperties = new KafkaProperties();
    private CheckVersionsExecutor versionsExecutor;
    private CheckContext context;

    @BeforeEach
    void setUp() {
        activePluginsProperties.setActive(SOURCE_TYPES);
        kafkaProperties.setStatusMonitor(new KafkaStatusMonitorProperties());
        versionsExecutor = new CheckVersionsExecutor(dataSourcePluginService,
                queryResultFactory,
                webClient,
                activePluginsProperties,
                kafkaProperties, new BuildProperties(new Properties()));
        QueryRequest queryRequest = new QueryRequest();
        queryRequest.setDatamartMnemonic("test");
        SqlCheckVersions sqlCheckVersions = mock(SqlCheckVersions.class);
        context = new CheckContext(new RequestMetrics(), "test",
                new DatamartRequest(queryRequest), CheckType.VERSIONS, sqlCheckVersions);
    }

    @Test
    void executeSuccess() {
        List<Map<String, Object>> resultList = new ArrayList<>();
        List<VersionInfo> adbVersions = Arrays.asList(new VersionInfo("fdw", "1.0"),
                new VersionInfo("pxf", "2.0"));
        List<VersionInfo> adqmVersions = Arrays.asList(new VersionInfo("kafka-writer", "3.3.0"),
                new VersionInfo("kafka-reader", "3.4.0"));
        List<VersionInfo> adgVersions = Arrays.asList(new VersionInfo("adg-connector", "3.4.0"));

        adbVersions.forEach(v -> resultList.add(createRowMap(v)));
        adqmVersions.forEach(v -> resultList.add(createRowMap(v)));
        adgVersions.forEach(v -> resultList.add(createRowMap(v)));
        resultList.add(createRowMap(createStatusMonitorVersionInfo()));

        QueryResult expectedResult = QueryResult.builder()
                .metadata(Arrays.asList(ColumnMetadata.builder().name(COMPONENT_NAME_COLUMN).type(ColumnType.VARCHAR).build(),
                        ColumnMetadata.builder().name(VERSION_COLUMN).type(ColumnType.VARCHAR).build()))
                .result(resultList)
                .build();

        Mockito.doAnswer(invocation -> {
            SourceType st = invocation.getArgument(0);
            if (st == SourceType.ADB) {
                return Future.succeededFuture(adbVersions);
            } else if (st == SourceType.ADQM) {
                return Future.succeededFuture(adqmVersions);
            } else {
                return Future.succeededFuture(adgVersions);
            }
        }).when(dataSourcePluginService).checkVersion(any(), any(), any());
        when(webClient.getAbs(any())).thenReturn(createHttpRequest());
        when(queryResultFactory.create(any())).thenReturn(expectedResult);

        versionsExecutor.execute(context)
                .onComplete(ar -> {
                    assertTrue(ar.succeeded());
                    assertEquals(expectedResult, ar.result());
                });
    }

    @Test
    void executeWithNullVersionInfo() {
        List<Map<String, Object>> resultList = new ArrayList<>();
        List<VersionInfo> adbVersions = Arrays.asList(new VersionInfo("fdw", "1.0"),
                new VersionInfo("pxf", "2.0"));
        List<VersionInfo> adqmVersions = Arrays.asList(new VersionInfo("kafka-writer", "3.3.0"),
                new VersionInfo("kafka-reader", "3.4.0"));
        List<VersionInfo> adgVersions = null;

        adbVersions.forEach(v -> resultList.add(createRowMap(v)));
        adqmVersions.forEach(v -> resultList.add(createRowMap(v)));
        resultList.add(createRowMap(createStatusMonitorVersionInfo()));

        QueryResult expectedResult = QueryResult.builder()
                .metadata(Arrays.asList(ColumnMetadata.builder().name(COMPONENT_NAME_COLUMN).type(ColumnType.VARCHAR).build(),
                        ColumnMetadata.builder().name(VERSION_COLUMN).type(ColumnType.VARCHAR).build()))
                .result(resultList)
                .build();

        Mockito.doAnswer(invocation -> {
            SourceType st = invocation.getArgument(0);
            if (st == SourceType.ADB) {
                return Future.succeededFuture(adbVersions);
            } else if (st == SourceType.ADQM) {
                return Future.succeededFuture(adqmVersions);
            } else {
                return Future.succeededFuture(adgVersions);
            }
        }).when(dataSourcePluginService).checkVersion(any(), any(), any());
        when(webClient.getAbs(any())).thenReturn(createHttpRequest());
        when(queryResultFactory.create(any())).thenReturn(expectedResult);

        versionsExecutor.execute(context)
                .onComplete(ar -> {
                    assertTrue(ar.succeeded());
                    assertEquals(expectedResult, ar.result());
                });
    }

    @Test
    void executeFail() {
        List<Map<String, Object>> resultList = new ArrayList<>();
        List<VersionInfo> adbVersions = Arrays.asList(new VersionInfo("fdw", "1.0"),
                new VersionInfo("pxf", "2.0"));
        List<VersionInfo> adqmVersions = Arrays.asList(new VersionInfo("kafka-writer", "3.3.0"),
                new VersionInfo("kafka-reader", "3.4.0"));

        adbVersions.forEach(v -> resultList.add(createRowMap(v)));
        adqmVersions.forEach(v -> resultList.add(createRowMap(v)));
        resultList.add(createRowMap(createStatusMonitorVersionInfo()));

        QueryResult expectedResult = QueryResult.builder()
                .metadata(Arrays.asList(ColumnMetadata.builder().name(COMPONENT_NAME_COLUMN).type(ColumnType.VARCHAR).build(),
                        ColumnMetadata.builder().name(VERSION_COLUMN).type(ColumnType.VARCHAR).build()))
                .result(resultList)
                .build();

        Mockito.doAnswer(invocation -> {
            SourceType st = invocation.getArgument(0);
            if (st == SourceType.ADB) {
                return Future.succeededFuture(adbVersions);
            } else if (st == SourceType.ADQM) {
                return Future.succeededFuture(adqmVersions);
            } else {
                return Future.failedFuture(new DtmException(""));
            }
        }).when(dataSourcePluginService).checkVersion(any(), any(), any());
        when(webClient.getAbs(any())).thenReturn(createHttpRequest());
        when(queryResultFactory.create(any())).thenReturn(expectedResult);

        versionsExecutor.execute(context)
                .onComplete(ar -> assertTrue(ar.failed()));
    }

    private Map<String, Object> createRowMap(VersionInfo versionInfo) {
        Map<String, Object> rowMap = new HashMap<>();
        rowMap.put(COMPONENT_NAME_COLUMN, versionInfo.getName());
        rowMap.put(VERSION_COLUMN, versionInfo.getVersion());
        return rowMap;
    }

    @NotNull
    private HttpRequest<Buffer> createHttpRequest() {
        return new HttpRequest<Buffer>() {
            @Override
            public HttpRequest<Buffer> method(HttpMethod httpMethod) {
                return null;
            }

            @Override
            public HttpRequest<Buffer> port(int i) {
                return null;
            }

            @Override
            public <U> HttpRequest<U> as(BodyCodec<U> bodyCodec) {
                return null;
            }

            @Override
            public HttpRequest<Buffer> host(String s) {
                return null;
            }

            @Override
            public HttpRequest<Buffer> virtualHost(String s) {
                return null;
            }

            @Override
            public HttpRequest<Buffer> uri(String s) {
                return null;
            }

            @Override
            public HttpRequest<Buffer> putHeaders(MultiMap multiMap) {
                return null;
            }

            @Override
            public HttpRequest<Buffer> putHeader(String s, String s1) {
                return null;
            }

            @Override
            public HttpRequest<Buffer> putHeader(String s, Iterable<String> iterable) {
                return null;
            }

            @Override
            public MultiMap headers() {
                return null;
            }

            @Override
            public HttpRequest<Buffer> authentication(Credentials credentials) {
                return null;
            }

            @Override
            public HttpRequest<Buffer> basicAuthentication(String s, String s1) {
                return null;
            }

            @Override
            public HttpRequest<Buffer> basicAuthentication(Buffer buffer, Buffer buffer1) {
                return null;
            }

            @Override
            public HttpRequest<Buffer> bearerTokenAuthentication(String s) {
                return null;
            }

            @Override
            public HttpRequest<Buffer> ssl(Boolean aBoolean) {
                return null;
            }

            @Override
            public HttpRequest<Buffer> timeout(long l) {
                return null;
            }

            @Override
            public HttpRequest<Buffer> addQueryParam(String s, String s1) {
                return null;
            }

            @Override
            public HttpRequest<Buffer> setQueryParam(String s, String s1) {
                return null;
            }

            @Override
            public HttpRequest<Buffer> followRedirects(boolean b) {
                return null;
            }

            @Override
            public HttpRequest<Buffer> expect(Function<HttpResponse<Void>, ResponsePredicateResult> predicate) {
                return HttpRequest.super.expect(predicate);
            }

            @Override
            public HttpRequest<Buffer> expect(ResponsePredicate responsePredicate) {
                return null;
            }

            @Override
            public MultiMap queryParams() {
                return null;
            }

            @Override
            public HttpRequest<Buffer> copy() {
                return null;
            }

            @Override
            public HttpRequest<Buffer> multipartMixed(boolean b) {
                return null;
            }

            @Override
            public void sendStream(ReadStream<Buffer> readStream, Handler<AsyncResult<HttpResponse<Buffer>>> handler) {

            }

            @Override
            public Future<HttpResponse<Buffer>> sendStream(ReadStream<Buffer> body) {
                return HttpRequest.super.sendStream(body);
            }

            @Override
            public void sendBuffer(Buffer buffer, Handler<AsyncResult<HttpResponse<Buffer>>> handler) {

            }

            @Override
            public Future<HttpResponse<Buffer>> sendBuffer(Buffer body) {
                return HttpRequest.super.sendBuffer(body);
            }

            @Override
            public void sendJsonObject(JsonObject jsonObject, Handler<AsyncResult<HttpResponse<Buffer>>> handler) {

            }

            @Override
            public Future<HttpResponse<Buffer>> sendJsonObject(JsonObject body) {
                return HttpRequest.super.sendJsonObject(body);
            }

            @Override
            public void sendJson(Object o, Handler<AsyncResult<HttpResponse<Buffer>>> handler) {

            }

            @Override
            public Future<HttpResponse<Buffer>> sendJson(Object body) {
                return HttpRequest.super.sendJson(body);
            }

            @Override
            public void sendForm(MultiMap multiMap, Handler<AsyncResult<HttpResponse<Buffer>>> handler) {

            }

            @Override
            public Future<HttpResponse<Buffer>> sendForm(MultiMap body) {
                return HttpRequest.super.sendForm(body);
            }

            @Override
            public void sendMultipartForm(MultipartForm multipartForm, Handler<AsyncResult<HttpResponse<Buffer>>> handler) {

            }

            @Override
            public Future<HttpResponse<Buffer>> sendMultipartForm(MultipartForm body) {
                return HttpRequest.super.sendMultipartForm(body);
            }

            @Override
            public void send(Handler<AsyncResult<HttpResponse<Buffer>>> handler) {
                handler.handle(Future.succeededFuture(createHttpResponse()));
            }

            @Override
            public Future<HttpResponse<Buffer>> send() {
                return HttpRequest.super.send();
            }
        };
    }

    @NotNull
    private HttpResponse<Buffer> createHttpResponse() {
        return new HttpResponse<Buffer>() {
            @Override
            public HttpVersion version() {
                return null;
            }

            @Override
            public int statusCode() {
                return 200;
            }

            @Override
            public String statusMessage() {
                return null;
            }

            @Override
            public MultiMap headers() {
                return null;
            }

            @Override
            public String getHeader(String s) {
                return null;
            }

            @Override
            public MultiMap trailers() {
                return null;
            }

            @Override
            public String getTrailer(String s) {
                return null;
            }

            @Override
            public List<String> cookies() {
                return null;
            }

            @Override
            public Buffer body() {
                return null;
            }

            @Override
            public Buffer bodyAsBuffer() {
                return null;
            }

            @Override
            public List<String> followedRedirects() {
                return null;
            }

            @Override
            public JsonArray bodyAsJsonArray() {
                return null;
            }

            @Override
            public <R> R bodyAsJson(Class<R> type) {
                return (R) createStatusMonitorVersionInfo();
            }
        };
    }

    @NotNull
    private VersionInfo createStatusMonitorVersionInfo() {
        return new VersionInfo("status-monitor", "3.4.0");
    }
}

