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
package ru.datamart.prostore.query.execution.plugin.adp.check;

import ru.datamart.prostore.common.version.VersionInfo;
import ru.datamart.prostore.query.execution.plugin.adp.check.service.AdpCheckVersionService;
import ru.datamart.prostore.query.execution.plugin.adp.connector.service.AdpConnectorClient;
import ru.datamart.prostore.query.execution.plugin.adp.db.service.DatabaseExecutor;
import ru.datamart.prostore.query.execution.plugin.api.check.CheckVersionRequest;
import io.vertx.core.Future;
import lombok.val;
import org.junit.jupiter.api.Test;

import java.util.*;

import static ru.datamart.prostore.query.execution.plugin.adp.check.factory.AdpVersionQueriesFactory.COMPONENT_NAME_COLUMN;
import static ru.datamart.prostore.query.execution.plugin.adp.check.factory.AdpVersionQueriesFactory.VERSION_COLUMN;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.*;

class AdpCheckVersionServiceTest {

    private final DatabaseExecutor databaseExecutor = mock(DatabaseExecutor.class);
    private final AdpConnectorClient connectorClient = mock(AdpConnectorClient.class);
    private final AdpCheckVersionService checkVersionService = new AdpCheckVersionService(databaseExecutor, connectorClient);
    private final CheckVersionRequest request = new CheckVersionRequest(UUID.randomUUID(),"env", "dtm");

    @Test
    void checkVersionSuccess() {
        val adpComponentName = "ADP";
        val adpComponentVersion = "ADP version";
        val expectedAdpVersionInfo = new VersionInfo(adpComponentName, adpComponentVersion);
        val mpprComponentName = "MPPR Connector";
        val mpprComponentVersion = "MPPR version";
        val expectedMpprVersionInfo = new VersionInfo(mpprComponentName, mpprComponentVersion);
        val mppwComponentName = "MPPW Connector";
        val mppwComponentVersion = "MPPW version";
        val expectedMppwVersionInfo = new VersionInfo(mppwComponentName, mppwComponentVersion);
        Map<String, Object> row = new HashMap<>();
        row.put(COMPONENT_NAME_COLUMN, adpComponentName);
        row.put(VERSION_COLUMN, adpComponentVersion);
        when(databaseExecutor.execute(anyString(), any())).thenReturn(Future.succeededFuture(Collections.singletonList(row)));
        when(connectorClient.getMpprVersion()).thenReturn(Future.succeededFuture(Collections.singletonList(expectedMpprVersionInfo)));
        when(connectorClient.getMppwVersion()).thenReturn(Future.succeededFuture(Collections.singletonList(expectedMppwVersionInfo)));

        checkVersionService.checkVersion(request)
                .onComplete(result -> {
                    assertTrue(result.succeeded());
                    val versions = result.result();
                    assertThat(expectedAdpVersionInfo).isEqualToComparingFieldByField(versions.get(0));
                    assertThat(expectedMpprVersionInfo).isEqualToComparingFieldByField(versions.get(1));
                    assertThat(expectedMppwVersionInfo).isEqualToComparingFieldByField(versions.get(2));
                });
        verify(databaseExecutor).execute(anyString(), any());
        verify(connectorClient).getMpprVersion();
        verify(connectorClient).getMppwVersion();
    }

    @Test
    void checkVersionAdpVersionError() {
        testWithError("receiving ADP version failed", 0);
    }

    @Test
    void checkVersionMpprVersionError() {
        testWithError("receiving MPPR version failed", 1);
    }

    @Test
    void checkVersionMppwVersionError() {
        testWithError("receiving MPPW version failed", 2);
    }

    private void testWithError(String error, int failedMockNumber) {
        Future<List<Map<String, Object>>> adpMockResult = Future.succeededFuture(new ArrayList<>());
        Future<List<VersionInfo>> mpprMockResult = Future.succeededFuture(new ArrayList<>());
        Future<List<VersionInfo>> mppwMockResult = Future.succeededFuture(new ArrayList<>());

        switch (failedMockNumber) {
            case 0:
                adpMockResult = Future.failedFuture(error);
                break;
            case 1:
                mpprMockResult = Future.failedFuture(error);
                break;
            default:
                mppwMockResult = Future.failedFuture(error);
                break;
        }

        when(databaseExecutor.execute(anyString(), any())).thenReturn(adpMockResult);
        when(connectorClient.getMpprVersion()).thenReturn(mpprMockResult);
        when(connectorClient.getMppwVersion()).thenReturn(mppwMockResult);

        checkVersionService.checkVersion(request)
                .onComplete(result -> {
                    assertTrue(result.failed());
                    assertEquals(error, result.cause().getMessage());
                });
        verify(databaseExecutor).execute(anyString(), any());
        verify(connectorClient).getMpprVersion();
        verify(connectorClient).getMppwVersion();
    }
}
