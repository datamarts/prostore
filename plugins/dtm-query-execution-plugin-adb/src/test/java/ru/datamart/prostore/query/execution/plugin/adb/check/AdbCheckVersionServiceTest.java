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
package ru.datamart.prostore.query.execution.plugin.adb.check;

import ru.datamart.prostore.common.version.VersionInfo;
import ru.datamart.prostore.query.execution.plugin.adb.check.factory.impl.AdbVersionInfoFactory;
import ru.datamart.prostore.query.execution.plugin.adb.check.factory.impl.AdbVersionQueriesFactory;
import ru.datamart.prostore.query.execution.plugin.adb.check.service.AdbCheckVersionService;
import ru.datamart.prostore.query.execution.plugin.adb.query.service.DatabaseExecutor;
import ru.datamart.prostore.query.execution.plugin.api.check.CheckVersionRequest;
import io.vertx.core.Future;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.*;

import static ru.datamart.prostore.query.execution.plugin.adb.check.factory.impl.AdbVersionQueriesFactory.COMPONENT_NAME_COLUMN;
import static ru.datamart.prostore.query.execution.plugin.adb.check.factory.impl.AdbVersionQueriesFactory.VERSION_COLUMN;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

class AdbCheckVersionServiceTest {

    private final DatabaseExecutor databaseExecutor = mock(DatabaseExecutor.class);
    private final AdbVersionQueriesFactory queriesFactory = mock(AdbVersionQueriesFactory.class);

    private final AdbCheckVersionService checkVersionService = new AdbCheckVersionService(databaseExecutor, queriesFactory, new AdbVersionInfoFactory());
    private final CheckVersionRequest checkVersionRequest = new CheckVersionRequest(UUID.randomUUID(), "env", "dtm");
    private final String adbVersionQuery = "adb_query";
    private final String fdwVersionQuery = "fdw_query";
    private final String pxfVersionQuery = "pxf_query";
    private final String  adbComponentName = "ADB cluster";
    private final String  adbComponentVersion = "ADB version";
    private final VersionInfo expectedAdbVersionInfo = new VersionInfo(adbComponentName, adbComponentVersion);
    private final String fdwComponentName = "FDW Connector";
    private final String fdwComponentVersion = "FDW version";
    private final VersionInfo expectedFdwVersionInfo = new VersionInfo(fdwComponentName, fdwComponentVersion);
    private final String pxfComponentName = "PXF Connector";
    private final String pxfComponentVersion = "PXF version";
    private final VersionInfo expectedPxfVersionInfo = new VersionInfo(pxfComponentName, pxfComponentVersion);

    @BeforeEach
    void setUp() {
        when(queriesFactory.createAdbVersionQuery()).thenReturn(adbVersionQuery);
        when(queriesFactory.createFdwVersionQuery()).thenReturn(fdwVersionQuery);
        when(queriesFactory.createPxfVersionQuery()).thenReturn(pxfVersionQuery);
    }

    @Test
    void checkVersionSuccess() {
        Map<String, Object> row = new HashMap<>();
        row.put(COMPONENT_NAME_COLUMN, adbComponentName);
        row.put(VERSION_COLUMN, adbComponentVersion);
        when(databaseExecutor.execute(eq(adbVersionQuery), any())).thenReturn(Future.succeededFuture(Collections.singletonList(row)));

        row = new HashMap<>();
        row.put(COMPONENT_NAME_COLUMN, fdwComponentName);
        row.put(VERSION_COLUMN, fdwComponentVersion);
        when(databaseExecutor.execute(eq(fdwVersionQuery), any())).thenReturn(Future.succeededFuture(Collections.singletonList(row)));

        row = new HashMap<>();
        row.put(COMPONENT_NAME_COLUMN, pxfComponentName);
        row.put(VERSION_COLUMN, pxfComponentVersion);
        when(databaseExecutor.execute(eq(pxfVersionQuery), any())).thenReturn(Future.succeededFuture(Collections.singletonList(row)));

        checkVersionService.checkVersion(checkVersionRequest)
                .onComplete(ar -> {
                    assertTrue(ar.succeeded());
                    List<VersionInfo> versions = ar.result();
                    assertThat(expectedAdbVersionInfo).isEqualToComparingFieldByField(versions.get(0));
                    assertThat(expectedFdwVersionInfo).isEqualToComparingFieldByField(versions.get(1));
                    assertThat(expectedPxfVersionInfo).isEqualToComparingFieldByField(versions.get(2));
                });
    }


    @Test
    void checkVersionAdbFail() {
        String errorMsg = "adb version error";
        when(databaseExecutor.execute(eq(adbVersionQuery), any())).thenReturn(Future.failedFuture(errorMsg));

        Map<String, Object> row = new HashMap<>();
        row.put(COMPONENT_NAME_COLUMN, fdwComponentName);
        row.put(VERSION_COLUMN, fdwComponentVersion);
        when(databaseExecutor.execute(eq(fdwVersionQuery), any())).thenReturn(Future.succeededFuture(Collections.singletonList(row)));

        row = new HashMap<>();
        row.put(COMPONENT_NAME_COLUMN, pxfComponentName);
        row.put(VERSION_COLUMN, pxfComponentVersion);
        when(databaseExecutor.execute(eq(pxfVersionQuery), any())).thenReturn(Future.succeededFuture(Collections.singletonList(row)));

        checkVersionService.checkVersion(checkVersionRequest)
                .onComplete(ar -> {
                    assertTrue(ar.failed());
                    assertEquals(errorMsg, ar.cause().getMessage());
                });
    }

    @Test
    void checkVersionFdwFail() {
        Map<String, Object> row = new HashMap<>();
        row.put(COMPONENT_NAME_COLUMN, adbComponentName);
        row.put(VERSION_COLUMN, adbComponentVersion);
        when(databaseExecutor.execute(eq(adbVersionQuery), any())).thenReturn(Future.succeededFuture(Collections.singletonList(row)));

        String errorMsg = "fdw version error";
        when(databaseExecutor.execute(eq(fdwVersionQuery), any())).thenReturn(Future.failedFuture(errorMsg));

        row = new HashMap<>();
        row.put(COMPONENT_NAME_COLUMN, pxfComponentName);
        row.put(VERSION_COLUMN, pxfComponentVersion);
        when(databaseExecutor.execute(eq(pxfVersionQuery), any())).thenReturn(Future.succeededFuture(Collections.singletonList(row)));

        checkVersionService.checkVersion(checkVersionRequest)
                .onComplete(ar -> {
                    assertTrue(ar.failed());
                    assertEquals(errorMsg, ar.cause().getMessage());
                });
    }


    @Test
    void checkVersionPxfFail() {
        Map<String, Object> row = new HashMap<>();
        row.put(COMPONENT_NAME_COLUMN, adbComponentName);
        row.put(VERSION_COLUMN, adbComponentVersion);
        when(databaseExecutor.execute(eq(adbVersionQuery), any())).thenReturn(Future.succeededFuture(Collections.singletonList(row)));

        row = new HashMap<>();
        row.put(COMPONENT_NAME_COLUMN, fdwComponentName);
        row.put(VERSION_COLUMN, fdwComponentVersion);
        when(databaseExecutor.execute(eq(fdwVersionQuery), any())).thenReturn(Future.succeededFuture(Collections.singletonList(row)));

        String errorMsg = "pxf version error";
        when(databaseExecutor.execute(eq(pxfVersionQuery), any())).thenReturn(Future.failedFuture(errorMsg));

        checkVersionService.checkVersion(checkVersionRequest)
                .onComplete(ar -> {
                    assertTrue(ar.failed());
                    assertEquals(errorMsg, ar.cause().getMessage());
                });
    }
}
