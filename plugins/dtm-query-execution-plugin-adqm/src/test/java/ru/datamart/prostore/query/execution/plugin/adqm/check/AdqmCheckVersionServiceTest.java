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
package ru.datamart.prostore.query.execution.plugin.adqm.check;

import ru.datamart.prostore.common.version.VersionInfo;
import ru.datamart.prostore.query.execution.plugin.adqm.check.factory.AdqmVersionInfoFactory;
import ru.datamart.prostore.query.execution.plugin.adqm.check.factory.AdqmVersionQueriesFactory;
import ru.datamart.prostore.query.execution.plugin.adqm.check.service.AdqmCheckVersionService;
import ru.datamart.prostore.query.execution.plugin.adqm.mppr.configuration.properties.AdqmMpprProperties;
import ru.datamart.prostore.query.execution.plugin.adqm.mppw.configuration.properties.AdqmMppwProperties;
import ru.datamart.prostore.query.execution.plugin.adqm.query.service.DatabaseExecutor;
import ru.datamart.prostore.query.execution.plugin.api.check.CheckVersionRequest;
import io.vertx.core.Future;
import io.vertx.core.Promise;
import io.vertx.core.buffer.Buffer;
import io.vertx.core.buffer.impl.BufferImpl;
import io.vertx.core.http.HttpVersion;
import io.vertx.core.json.Json;
import io.vertx.core.json.JsonArray;
import io.vertx.ext.web.client.HttpRequest;
import io.vertx.ext.web.client.HttpResponse;
import io.vertx.ext.web.client.WebClient;
import io.vertx.ext.web.client.impl.HttpResponseImpl;
import lombok.val;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

import static ru.datamart.prostore.query.execution.plugin.adqm.check.factory.AdqmVersionQueriesFactory.COMPONENT_NAME_COLUMN;
import static ru.datamart.prostore.query.execution.plugin.adqm.check.factory.AdqmVersionQueriesFactory.VERSION_COLUMN;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.*;

@ExtendWith(MockitoExtension.class)
class AdqmCheckVersionServiceTest {

    @Mock
    private HttpRequest<Buffer> mpprHttpRequest;

    @Mock
    private HttpRequest<Buffer> mppwHttpRequest;

    @Mock
    private DatabaseExecutor databaseExecutor;

    @Mock
    private AdqmMpprProperties mpprProperties;

    @Mock
    private AdqmMppwProperties mppwProperties;

    @Mock
    private WebClient webClient;

    private AdqmCheckVersionService checkVersionService;
    private final CheckVersionRequest request = new CheckVersionRequest(UUID.randomUUID(),"env", "dtm");

    @BeforeEach
    void setUp() {
        checkVersionService = new AdqmCheckVersionService(databaseExecutor,
                new AdqmVersionQueriesFactory(),
                new AdqmVersionInfoFactory(),
                mpprProperties,
                mppwProperties,
                webClient);
        val mpprVersionUrl = "mpprVersionUrl";
        val mppwVersionUrl = "mppwVersionUrl";
        when(mpprProperties.getVersionUrl()).thenReturn(mpprVersionUrl);
        when(mppwProperties.getVersionUrl()).thenReturn(mppwVersionUrl);
        when(webClient.getAbs(mpprVersionUrl)).thenReturn(mpprHttpRequest);
        when(webClient.getAbs(mppwVersionUrl)).thenReturn(mppwHttpRequest);
    }

    @Test
    void checkVersionSuccess() {
        val adqmComponentName = "ADQM cluster";
        val adqmComponentVersion = "ADQM version";
        val expectedAdqmVersionInfo = new VersionInfo(adqmComponentName, adqmComponentVersion);
        val mpprComponentName = "MPPR Connector";
        val mpprComponentVersion = "MPPR version";
        val expectedMpprVersionInfo = new VersionInfo(mpprComponentName, mpprComponentVersion);
        val mppwComponentName = "MPPW Connector";
        val mppwComponentVersion = "MPPW version";
        val expectedMppwVersionInfo = new VersionInfo(mppwComponentName, mppwComponentVersion);
        Map<String, Object> row = new HashMap<>();
        row.put(COMPONENT_NAME_COLUMN, adqmComponentName);
        row.put(VERSION_COLUMN, adqmComponentVersion);
        when(databaseExecutor.execute(anyString(), any())).thenReturn(Future.succeededFuture(Collections.singletonList(row)));

        HttpResponse<Buffer> mpprVersionResponse = getHttpResponse(expectedMpprVersionInfo);
        doAnswer(invocation -> {
            Promise<HttpResponse<Buffer>> promise = invocation.getArgument(0);
            promise.complete(mpprVersionResponse);
            return null;
        }).when(mpprHttpRequest).send(any());

        HttpResponse<Buffer> mppwVersionResponse = getHttpResponse(expectedMppwVersionInfo);
        doAnswer(invocation -> {
            Promise<HttpResponse<Buffer>> promise = invocation.getArgument(0);
            promise.complete(mppwVersionResponse);
            return null;
        }).when(mppwHttpRequest).send(any());

        checkVersionService.checkVersion(request)
                .onComplete(result -> {
                    assertTrue(result.succeeded());
                    val versions = result.result();
                    assertThat(expectedAdqmVersionInfo).isEqualToComparingFieldByField(versions.get(0));
                    assertThat(expectedMpprVersionInfo).isEqualToComparingFieldByField(versions.get(1));
                    assertThat(expectedMppwVersionInfo).isEqualToComparingFieldByField(versions.get(2));
                });
    }

    @Test
    void checkVersionAdqmVersionFail() {
        String errorMsg = "adqm version error";
        when(databaseExecutor.execute(anyString(), any())).thenReturn(Future.failedFuture(errorMsg));

        val mpprComponentName = "MPPR Connector";
        val mpprComponentVersion = "MPPR version";
        val expectedMpprVersionInfo = new VersionInfo(mpprComponentName, mpprComponentVersion);
        val mppwComponentName = "MPPW Connector";
        val mppwComponentVersion = "MPPW version";
        val expectedMppwVersionInfo = new VersionInfo(mppwComponentName, mppwComponentVersion);

        HttpResponse<Buffer> mpprVersionResponse = getHttpResponse(expectedMpprVersionInfo);
        doAnswer(invocation -> {
            Promise<HttpResponse<Buffer>> promise = invocation.getArgument(0);
            promise.complete(mpprVersionResponse);
            return null;
        }).when(mpprHttpRequest).send(any());

        HttpResponse<Buffer> mppwVersionResponse = getHttpResponse(expectedMppwVersionInfo);
        doAnswer(invocation -> {
            Promise<HttpResponse<Buffer>> promise = invocation.getArgument(0);
            promise.complete(mppwVersionResponse);
            return null;
        }).when(mppwHttpRequest).send(any());

        checkVersionService.checkVersion(request)
                .onComplete(result -> {
                    assertTrue(result.failed());
                    assertEquals(errorMsg, result.cause().getMessage());
                });
    }

    @Test
    void checkVersionMpprVersionFail() {
        val adqmComponentName = "ADQM cluster";
        val adqmComponentVersion = "ADQM version";
        val mppwComponentName = "MPPW Connector";
        val mppwComponentVersion = "MPPW version";
        val expectedMppwVersionInfo = new VersionInfo(mppwComponentName, mppwComponentVersion);
        Map<String, Object> row = new HashMap<>();
        row.put(COMPONENT_NAME_COLUMN, adqmComponentName);
        row.put(VERSION_COLUMN, adqmComponentVersion);
        when(databaseExecutor.execute(anyString(), any())).thenReturn(Future.succeededFuture(Collections.singletonList(row)));

        String errorMsg = "mppr version error";
        doAnswer(invocation -> {
            Promise<HttpResponse<Buffer>> promise = invocation.getArgument(0);
            promise.fail(errorMsg);
            return null;
        }).when(mpprHttpRequest).send(any());

        HttpResponse<Buffer> mppwVersionResponse = getHttpResponse(expectedMppwVersionInfo);
        doAnswer(invocation -> {
            Promise<HttpResponse<Buffer>> promise = invocation.getArgument(0);
            promise.complete(mppwVersionResponse);
            return null;
        }).when(mppwHttpRequest).send(any());

        checkVersionService.checkVersion(request)
                .onComplete(result -> {
                    assertTrue(result.failed());
                    assertEquals(errorMsg, result.cause().getMessage());
                });
    }

    @Test
    void checkVersionMppwVersionFail() {
        val adqmComponentName = "ADQM cluster";
        val adqmComponentVersion = "ADQM version";
        val mpprComponentName = "MPPR Connector";
        val mpprComponentVersion = "MPPR version";
        val expectedMpprVersionInfo = new VersionInfo(mpprComponentName, mpprComponentVersion);
        Map<String, Object> row = new HashMap<>();
        row.put(COMPONENT_NAME_COLUMN, adqmComponentName);
        row.put(VERSION_COLUMN, adqmComponentVersion);
        when(databaseExecutor.execute(anyString(), any())).thenReturn(Future.succeededFuture(Collections.singletonList(row)));

        HttpResponse<Buffer> mpprVersionResponse = getHttpResponse(expectedMpprVersionInfo);
        doAnswer(invocation -> {
            Promise<HttpResponse<Buffer>> promise = invocation.getArgument(0);
            promise.complete(mpprVersionResponse);
            return null;
        }).when(mpprHttpRequest).send(any());

        String errorMsg = "mppw version error";
        doAnswer(invocation -> {
            Promise<HttpResponse<Buffer>> promise = invocation.getArgument(0);
            promise.fail(errorMsg);
            return null;
        }).when(mppwHttpRequest).send(any());

        checkVersionService.checkVersion(request)
                .onComplete(result -> {
                    assertTrue(result.failed());
                    assertEquals(errorMsg, result.cause().getMessage());
                });
    }

    private HttpResponseImpl<Buffer> getHttpResponse(VersionInfo expectedMpprVersionInfo) {
        return new HttpResponseImpl<>(
                HttpVersion.HTTP_2,
                200,
                "OK",
                null,
                null,
                Collections.emptyList(),
                new JsonArray().add(expectedMpprVersionInfo).toBuffer(),
                Collections.emptyList()
        );
    }
}
