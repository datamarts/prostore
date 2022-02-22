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
package ru.datamart.prostore.query.execution.core.init;

import ru.datamart.prostore.common.exception.DtmException;
import ru.datamart.prostore.common.reader.SourceType;
import ru.datamart.prostore.query.execution.core.base.service.MaterializedViewSyncService;
import ru.datamart.prostore.query.execution.core.base.service.metadata.InformationSchemaService;
import ru.datamart.prostore.query.execution.core.init.service.CoreInitializationService;
import ru.datamart.prostore.query.execution.core.plugin.service.DataSourcePluginService;
import ru.datamart.prostore.query.execution.core.plugin.service.impl.DataSourcePluginServiceImpl;
import ru.datamart.prostore.query.execution.core.query.verticle.starter.QueryWorkerStarter;
import ru.datamart.prostore.query.execution.core.rollback.service.RestoreStateService;
import io.vertx.core.Future;
import io.vertx.core.Verticle;
import io.vertx.core.Vertx;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.*;

import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;

class CoreInitializationServiceTest {

    private final DataSourcePluginService pluginService = mock(DataSourcePluginServiceImpl.class);
    private final Set<SourceType> sourceTypes = new HashSet<>(Arrays.asList(SourceType.ADB, SourceType.ADG, SourceType.ADQM));
    private CoreInitializationService initializationService;
    private final InformationSchemaService informationSchemaService = mock(InformationSchemaService.class);
    private final MaterializedViewSyncService materializedViewSyncService = mock(MaterializedViewSyncService.class);
    private final RestoreStateService restoreStateService = mock(RestoreStateService.class);
    private final QueryWorkerStarter queryWorkerStarter = mock(QueryWorkerStarter.class);
    private final Vertx vertx = mock(Vertx.class);

    @BeforeEach
    void setUp() {
        List<Verticle> verticles = new ArrayList<>();
        initializationService = new CoreInitializationService(pluginService,
                informationSchemaService,
                materializedViewSyncService,
                restoreStateService,
                vertx,
                queryWorkerStarter,
                verticles);
        when(pluginService.getSourceTypes()).thenReturn(sourceTypes);
        when(restoreStateService.isAutoRestoreState()).thenReturn(true);
        when(materializedViewSyncService.isSyncEnabled()).thenReturn(true);
    }

    @Test
    void executeAllPluginsSucceed() {
        when(informationSchemaService.initInformationSchema()).thenReturn(Future.succeededFuture());
        when(restoreStateService.restoreState()).thenReturn(Future.succeededFuture());
        when(queryWorkerStarter.start(vertx)).thenReturn(Future.succeededFuture());

        when(pluginService.initialize(SourceType.ADB))
                .thenReturn(Future.succeededFuture());
        when(pluginService.initialize(SourceType.ADG))
                .thenReturn(Future.succeededFuture());
        when(pluginService.initialize(SourceType.ADQM))
                .thenReturn(Future.succeededFuture());

        initializationService.execute()
                .onComplete(ar -> {
                    assertTrue(ar.succeeded());
                    verify(pluginService, times(3)).initialize(any());
                    verify(informationSchemaService).initInformationSchema();
                    verify(restoreStateService).restoreState();
                    verify(queryWorkerStarter).start(vertx);
                    verify(materializedViewSyncService).startPeriodicalSync();
                });
    }

    @Test
    void executeWithoutAutoRestore() {
        when(restoreStateService.isAutoRestoreState()).thenReturn(false);

        when(informationSchemaService.initInformationSchema()).thenReturn(Future.succeededFuture());
        when(restoreStateService.restoreState()).thenReturn(Future.succeededFuture());
        when(queryWorkerStarter.start(vertx)).thenReturn(Future.succeededFuture());

        when(pluginService.initialize(SourceType.ADB))
                .thenReturn(Future.succeededFuture());
        when(pluginService.initialize(SourceType.ADG))
                .thenReturn(Future.succeededFuture());
        when(pluginService.initialize(SourceType.ADQM))
                .thenReturn(Future.succeededFuture());

        initializationService.execute()
                .onComplete(ar -> {
                    assertTrue(ar.succeeded());
                    verify(pluginService, times(3)).initialize(any());
                    verify(informationSchemaService).initInformationSchema();
                    verify(restoreStateService, never()).restoreState();
                    verify(queryWorkerStarter).start(vertx);
                    verify(materializedViewSyncService).startPeriodicalSync();
                });
    }

    @Test
    void executeWithoutMatViewSync() {
        when(materializedViewSyncService.isSyncEnabled()).thenReturn(false);

        when(informationSchemaService.initInformationSchema()).thenReturn(Future.succeededFuture());
        when(restoreStateService.restoreState()).thenReturn(Future.succeededFuture());
        when(queryWorkerStarter.start(vertx)).thenReturn(Future.succeededFuture());

        when(pluginService.initialize(SourceType.ADB))
                .thenReturn(Future.succeededFuture());
        when(pluginService.initialize(SourceType.ADG))
                .thenReturn(Future.succeededFuture());
        when(pluginService.initialize(SourceType.ADQM))
                .thenReturn(Future.succeededFuture());

        initializationService.execute()
                .onComplete(ar -> {
                    assertTrue(ar.succeeded());
                    verify(pluginService, times(3)).initialize(any());
                    verify(informationSchemaService).initInformationSchema();
                    verify(restoreStateService).restoreState();
                    verify(queryWorkerStarter).start(vertx);
                    verify(materializedViewSyncService, never()).startPeriodicalSync();
                });
    }

    @Test
    void executeWithRestoreError() {
        when(informationSchemaService.initInformationSchema()).thenReturn(Future.succeededFuture());
        when(restoreStateService.restoreState()).thenReturn(Future.failedFuture(new DtmException("")));
        when(queryWorkerStarter.start(vertx)).thenReturn(Future.succeededFuture());

        when(pluginService.initialize(SourceType.ADB))
                .thenReturn(Future.succeededFuture());
        when(pluginService.initialize(SourceType.ADG))
                .thenReturn(Future.succeededFuture());
        when(pluginService.initialize(SourceType.ADQM))
                .thenReturn(Future.succeededFuture());

        initializationService.execute()
                .onComplete(ar -> {
                    assertTrue(ar.succeeded());
                    verify(pluginService, times(3)).initialize(any());
                    verify(informationSchemaService).initInformationSchema();
                    verify(restoreStateService).restoreState();
                    verify(queryWorkerStarter).start(vertx);
                    verify(materializedViewSyncService).startPeriodicalSync();
                });
    }

    @Test
    void executeWithInformationSchemaInitError() {
        when(informationSchemaService.initInformationSchema())
                .thenReturn(Future.failedFuture(new DtmException("")));
        when(pluginService.initialize(SourceType.ADB))
                .thenReturn(Future.succeededFuture());
        when(pluginService.initialize(SourceType.ADG))
                .thenReturn(Future.succeededFuture());
        when(pluginService.initialize(SourceType.ADQM))
                .thenReturn(Future.succeededFuture());

        initializationService.execute()
                .onComplete(ar -> {
                    assertTrue(ar.failed());
                    verify(informationSchemaService).initInformationSchema();
                    verifyNoInteractions(pluginService);
                    verifyNoInteractions(restoreStateService);
                    verifyNoInteractions(queryWorkerStarter);
                    verifyNoInteractions(materializedViewSyncService);
                });
    }

    @Test
    void executeWithQueryWorkerStartError() {
        when(informationSchemaService.initInformationSchema())
                .thenReturn(Future.succeededFuture());
        when(restoreStateService.restoreState()).thenReturn(Future.succeededFuture());
        when(queryWorkerStarter.start(vertx)).thenReturn(Future.failedFuture(new DtmException("")));

        when(pluginService.initialize(SourceType.ADB))
                .thenReturn(Future.succeededFuture());
        when(pluginService.initialize(SourceType.ADG))
                .thenReturn(Future.succeededFuture());
        when(pluginService.initialize(SourceType.ADQM))
                .thenReturn(Future.succeededFuture());

        initializationService.execute()
                .onComplete(ar -> {
                    assertTrue(ar.failed());
                    verify(informationSchemaService).initInformationSchema();
                    verify(pluginService, times(3)).initialize(any());
                    verify(restoreStateService).restoreState();
                    verify(queryWorkerStarter).start(vertx);
                    verifyNoInteractions(materializedViewSyncService);
                });
    }

    @Test
    void executePluginsError() {
        when(informationSchemaService.initInformationSchema()).thenReturn(Future.succeededFuture());
        when(restoreStateService.restoreState()).thenReturn(Future.succeededFuture());
        when(queryWorkerStarter.start(vertx)).thenReturn(Future.succeededFuture());
        when(pluginService.initialize(SourceType.ADB))
                .thenReturn(Future.failedFuture(new DtmException("")));
        when(pluginService.initialize(SourceType.ADG))
                .thenReturn(Future.succeededFuture());
        when(pluginService.initialize(SourceType.ADQM))
                .thenReturn(Future.succeededFuture());

        initializationService.execute()
                .onComplete(ar -> {
                    assertTrue(ar.failed());
                    verify(informationSchemaService).initInformationSchema();
                    verify(pluginService, times(3)).initialize(any());
                    verifyNoInteractions(restoreStateService);
                    verifyNoInteractions(queryWorkerStarter);
                    verifyNoInteractions(materializedViewSyncService);
                });
    }

    @Test
    void executeOnePluginSuccess() {
        when(pluginService.initialize(SourceType.ADQM))
                .thenReturn(Future.succeededFuture());
        initializationService.execute(SourceType.ADQM)
                .onComplete(ar -> {
                    assertTrue(ar.succeeded());
                    verify(pluginService).initialize(SourceType.ADQM);
                });
    }

    @Test
    void executeOnePluginError() {
        when(pluginService.initialize(SourceType.ADQM))
                .thenReturn(Future.failedFuture(new DtmException("")));
        initializationService.execute(SourceType.ADQM)
                .onComplete(ar -> {
                    assertTrue(ar.failed());
                    verify(pluginService).initialize(SourceType.ADQM);
                });
    }
}