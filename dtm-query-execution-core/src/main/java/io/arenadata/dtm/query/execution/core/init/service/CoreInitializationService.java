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
package io.arenadata.dtm.query.execution.core.init.service;

import io.arenadata.dtm.common.reader.SourceType;
import io.arenadata.dtm.query.execution.core.base.service.MaterializedViewSyncService;
import io.arenadata.dtm.query.execution.core.base.service.metadata.InformationSchemaService;
import io.arenadata.dtm.query.execution.core.plugin.service.DataSourcePluginService;
import io.arenadata.dtm.query.execution.core.query.verticle.starter.QueryWorkerStarter;
import io.arenadata.dtm.query.execution.core.rollback.service.RestoreStateService;
import io.vertx.core.CompositeFuture;
import io.vertx.core.Future;
import io.vertx.core.Verticle;
import io.vertx.core.Vertx;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Service;

import java.util.Collection;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

@Service("coreInitializationService")
@Slf4j
public class CoreInitializationService {

    private final DataSourcePluginService sourcePluginService;
    private final InformationSchemaService informationSchemaService;
    private final MaterializedViewSyncService materializedViewSyncService;
    private final RestoreStateService restoreStateService;
    private final Vertx vertx;
    private final QueryWorkerStarter queryWorkerStarter;
    private final List<Verticle> verticles;

    @Autowired
    public CoreInitializationService(DataSourcePluginService sourcePluginService,
                                     InformationSchemaService informationSchemaService,
                                     MaterializedViewSyncService materializedViewSyncService,
                                     RestoreStateService restoreStateService,
                                     @Qualifier("coreVertx") Vertx vertx,
                                     QueryWorkerStarter queryWorkerStarter,
                                     List<Verticle> verticles) {
        this.sourcePluginService = sourcePluginService;
        this.informationSchemaService = informationSchemaService;
        this.materializedViewSyncService = materializedViewSyncService;
        this.restoreStateService = restoreStateService;
        this.vertx = vertx;
        this.queryWorkerStarter = queryWorkerStarter;
        this.verticles = verticles;
    }

    public Future<Void> execute() {
        return informationSchemaService.initInformationSchema()
                .compose(v -> deployVerticles(vertx, verticles))
                .compose(v -> initPlugins())
                .compose(v -> restoreState())
                .compose(v -> queryWorkerStarter.start(vertx))
                .compose(v -> scheduleMatViewsSync());
    }

    private Future<Void> deployVerticles(Vertx vertx, Collection<Verticle> verticles) {
        log.info("Verticals found: {}", verticles.size());
        return CompositeFuture.join(verticles.stream()
                .map(verticle -> Future.future(p -> vertx.deployVerticle(verticle, ar -> {
                    if (ar.succeeded()) {
                        log.debug("Vertical '{}' deployed successfully", verticle.getClass().getName());
                        p.complete();
                    } else {
                        log.error("Vertical deploy error", ar.cause());
                        p.fail(ar.cause());
                    }
                })))
                .collect(Collectors.toList()))
                .mapEmpty();
    }

    private Future<Void> initPlugins() {
        return Future.future(promise -> {
            Set<SourceType> sourceTypes = sourcePluginService.getSourceTypes();
            CompositeFuture.join(sourceTypes.stream()
                    .map(sourcePluginService::initialize)
                    .collect(Collectors.toList()))
                    .onSuccess(s -> {
                        log.info("Plugins: {} initialized successfully", sourceTypes);
                        promise.complete();
                    })
                    .onFailure(promise::fail);
        });
    }

    private Future<Void> restoreState() {
        if (restoreStateService.isAutoRestoreState()) {
            restoreStateService.restoreState()
                    .onFailure(fail -> log.error("Error in restoring state", fail));
        }
        return Future.succeededFuture();
    }

    private Future<Void> scheduleMatViewsSync() {
        if (materializedViewSyncService.isSyncEnabled()) {
            materializedViewSyncService.startPeriodicalSync();
        }
        return Future.succeededFuture();
    }

    public Future<Void> execute(SourceType sourceType) {
        return Future.future(promise -> sourcePluginService.initialize(sourceType)
                .onSuccess(success -> {
                    log.info("Plugin: {} initialized successfully", sourceType);
                    promise.complete();
                })
                .onFailure(promise::fail));
    }
}
