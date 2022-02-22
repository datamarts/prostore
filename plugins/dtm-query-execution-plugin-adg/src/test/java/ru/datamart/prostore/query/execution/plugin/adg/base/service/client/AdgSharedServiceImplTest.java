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

import ru.datamart.prostore.common.model.ddl.Entity;
import ru.datamart.prostore.query.execution.plugin.adg.base.configuration.properties.TarantoolDatabaseProperties;
import ru.datamart.prostore.query.execution.plugin.adg.base.configuration.properties.TarantoolDatabaseSyncProperties;
import ru.datamart.prostore.query.execution.plugin.adg.base.dto.AdgHelperTableNames;
import ru.datamart.prostore.query.execution.plugin.adg.base.factory.AdgHelperTableNamesFactory;
import ru.datamart.prostore.query.execution.plugin.adg.base.model.cartridge.request.AdgTransferDataEtlRequest;
import ru.datamart.prostore.query.execution.plugin.adg.utils.TestUtils;
import ru.datamart.prostore.query.execution.plugin.api.shared.adg.AdgSharedPrepareStagingRequest;
import ru.datamart.prostore.query.execution.plugin.api.shared.adg.AdgSharedTransferDataRequest;
import io.vertx.core.Future;
import lombok.val;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class AdgSharedServiceImplTest {

    private static final String ENV = "test";
    private static final String DATAMART = "dtm";
    private static final String STAGING = "staging";
    private static final String HISTORY = "history";
    private static final String ACTUAL = "actual";
    private static final String PREFIX = "prefix";
    private static final String ERROR_MESSAGE = "ERROR";

    @Mock
    private AdgCartridgeClient cartridgeClient;
    @Mock
    private AdgHelperTableNamesFactory adgHelperTableNamesFactory;
    @Mock
    private TarantoolDatabaseProperties databaseProperties;
    @Mock
    private TarantoolDatabaseSyncProperties syncProperties;

    @InjectMocks
    private AdgSharedServiceImpl adgSharedService;

    @Captor
    private ArgumentCaptor<String> stringArgumentCaptor;

    @Captor
    private ArgumentCaptor<AdgTransferDataEtlRequest> transferRequestArgumentCaptor;

    private static Entity entity;
    private static AdgSharedPrepareStagingRequest prepareStagingRequest;
    private static AdgSharedTransferDataRequest transferDataRequest;
    private static AdgHelperTableNames adgHelperTableNames;

    @BeforeAll
    static void setUp() {
        entity = TestUtils.getEntity();
        prepareStagingRequest = new AdgSharedPrepareStagingRequest(ENV, DATAMART, entity);
        transferDataRequest = new AdgSharedTransferDataRequest(ENV, DATAMART, entity, 1);
        adgHelperTableNames = new AdgHelperTableNames(STAGING,
                HISTORY,
                ACTUAL,
                PREFIX);
    }

    @Test
    void prepareStagingSuccess() {
        when(cartridgeClient.truncateSpace(anyString())).thenReturn(Future.succeededFuture());

        adgSharedService.prepareStaging(prepareStagingRequest)
                .onComplete(ar -> {
                    assertTrue(ar.succeeded());
                    verify(cartridgeClient).truncateSpace(stringArgumentCaptor.capture());

                    val stagingSpaceName = stringArgumentCaptor.getValue();
                    assertEquals("test__dtm__test_table_staging", stagingSpaceName);
                });
    }

    @Test
    void prepareStagingFail() {
        when(cartridgeClient.truncateSpace(anyString())).thenReturn(Future.failedFuture(ERROR_MESSAGE));

        adgSharedService.prepareStaging(prepareStagingRequest)
                .onComplete(ar -> {
                    assertTrue(ar.failed());
                    assertEquals(ERROR_MESSAGE, ar.cause().getMessage());
                    verify(cartridgeClient).truncateSpace(stringArgumentCaptor.capture());

                    val stagingSpaceName = stringArgumentCaptor.getValue();
                    assertEquals("test__dtm__test_table_staging", stagingSpaceName);
                });
    }

    @Test
    void transferDataSuccess() {
        when(adgHelperTableNamesFactory.create(anyString(), anyString(), any())).thenReturn(adgHelperTableNames);
        when(cartridgeClient.transferDataToScdTable(any())).thenReturn(Future.succeededFuture());

        adgSharedService.transferData(transferDataRequest)
                .onComplete(ar -> {
                    assertTrue(ar.succeeded());

                    verify(adgHelperTableNamesFactory).create(anyString(), anyString(), any());
                    verify(cartridgeClient).transferDataToScdTable(transferRequestArgumentCaptor.capture());

                    val transferRequest = transferRequestArgumentCaptor.getValue();
                    assertEquals(1, transferRequest.getDeltaNumber());
                    val tableNames = transferRequest.getHelperTableNames();
                    assertEquals(STAGING, tableNames.getStaging());
                    assertEquals(HISTORY, tableNames.getHistory());
                    assertEquals(ACTUAL, tableNames.getActual());
                    assertEquals(PREFIX, tableNames.getPrefix());
                });
    }

    @Test
    void transferDataFail() {
        when(adgHelperTableNamesFactory.create(anyString(), anyString(), any())).thenReturn(adgHelperTableNames);
        when(cartridgeClient.transferDataToScdTable(any())).thenReturn(Future.failedFuture(ERROR_MESSAGE));

        adgSharedService.transferData(transferDataRequest)
                .onComplete(ar -> {
                    assertTrue(ar.failed());
                    assertEquals(ERROR_MESSAGE, ar.cause().getMessage());

                    verify(adgHelperTableNamesFactory).create(anyString(), anyString(), any());
                    verify(cartridgeClient).transferDataToScdTable(transferRequestArgumentCaptor.capture());

                    val transferRequest = transferRequestArgumentCaptor.getValue();
                    assertEquals(1, transferRequest.getDeltaNumber());
                    val tableNames = transferRequest.getHelperTableNames();
                    assertEquals(STAGING, tableNames.getStaging());
                    assertEquals(HISTORY, tableNames.getHistory());
                    assertEquals(ACTUAL, tableNames.getActual());
                    assertEquals(PREFIX, tableNames.getPrefix());
                });
    }

    @Test
    void transferDataTableNamesCreateFail() {
        when(adgHelperTableNamesFactory.create(anyString(), anyString(), any())).thenThrow(new RuntimeException(ERROR_MESSAGE));

        adgSharedService.transferData(transferDataRequest)
                .onComplete(ar -> {
                    assertTrue(ar.failed());
                    assertEquals(ERROR_MESSAGE, ar.cause().getMessage());
                });
    }
}
