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
package io.arenadata.dtm.query.execution.core.edml.mppw.service;

import io.arenadata.dtm.cache.service.EvictQueryTemplateCacheService;
import io.arenadata.dtm.common.exception.CrashException;
import io.arenadata.dtm.common.exception.DtmException;
import io.arenadata.dtm.common.model.ddl.Entity;
import io.arenadata.dtm.common.reader.SourceType;
import io.arenadata.dtm.query.execution.core.delta.repository.zookeeper.DeltaServiceDao;
import io.arenadata.dtm.query.execution.core.edml.dto.EdmlRequestContext;
import io.arenadata.dtm.query.execution.core.plugin.service.DataSourcePluginService;
import io.arenadata.dtm.query.execution.core.rollback.dto.RollbackRequestContext;
import io.arenadata.dtm.query.execution.core.rollback.factory.RollbackRequestContextFactory;
import io.arenadata.dtm.query.execution.plugin.api.dto.RollbackRequest;
import io.vertx.core.CompositeFuture;
import io.vertx.core.Future;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

@Slf4j
@Service
public class UploadFailedExecutor {

    private final DeltaServiceDao deltaServiceDao;
    private final RollbackRequestContextFactory rollbackRequestContextFactory;
    private final DataSourcePluginService dataSourcePluginService;
    private final EvictQueryTemplateCacheService evictQueryTemplateCacheService;

    @Autowired
    public UploadFailedExecutor(DeltaServiceDao deltaServiceDao,
                                RollbackRequestContextFactory rollbackRequestContextFactory,
                                DataSourcePluginService dataSourcePluginService,
                                EvictQueryTemplateCacheService evictQueryTemplateCacheService) {
        this.deltaServiceDao = deltaServiceDao;
        this.rollbackRequestContextFactory = rollbackRequestContextFactory;
        this.dataSourcePluginService = dataSourcePluginService;
        this.evictQueryTemplateCacheService = evictQueryTemplateCacheService;
    }

    public Future<Void> execute(EdmlRequestContext context) {
        Entity destinationEntity = context.getDestinationEntity();

        return Future.future(promise -> eraseWriteOp(context)
                .compose(v -> deltaServiceDao.deleteWriteOperation(destinationEntity.getSchema(), context.getSysCn()))
                .onComplete(ar -> {
                    try {
                        evictQueryTemplateCacheService.evictByEntityName(destinationEntity.getSchema(), destinationEntity.getName());
                    } catch (Exception e) {
                        promise.fail(new DtmException(e));
                    }
                    if (ar.succeeded()) {
                        promise.complete();
                    } else {
                        promise.fail(ar.cause());
                    }
                }));
    }

    private Future<Void> eraseWriteOp(EdmlRequestContext context) {
        return Future.future(rbPromise -> {
            final RollbackRequestContext rollbackRequestContext =
                    rollbackRequestContextFactory.create(context);
            eraseWriteOp(rollbackRequestContext)
                    .onSuccess(rbPromise::complete)
                    .onFailure(rbPromise::fail);
        });
    }

    public Future<Void> eraseWriteOp(RollbackRequestContext context) {
        List<Future> futures = new ArrayList<>();
        final Set<SourceType> destination = context.getRequest().getEntity().getDestination().stream()
                .filter(dataSourcePluginService::hasSourceType)
                .collect(Collectors.toSet());
        destination.forEach(sourceType ->
                futures.add(Future.future(p -> dataSourcePluginService.rollback(
                                sourceType,
                                context.getMetrics(),
                                RollbackRequest.builder()
                                        .requestId(context.getRequest().getQueryRequest().getRequestId())
                                        .envName(context.getEnvName())
                                        .datamartMnemonic(context.getRequest().getDatamart())
                                        .destinationTable(context.getRequest().getDestinationTable())
                                        .sysCn(context.getRequest().getSysCn())
                                        .entity(context.getRequest().getEntity())
                                        .build())
                        .onSuccess(result -> {
                            log.debug("Rollback data in plugin [{}], datamart [{}], " +
                                            "table [{}], sysCn [{}] finished successfully",
                                    sourceType,
                                    context.getRequest().getDatamart(),
                                    context.getRequest().getDestinationTable(),
                                    context.getRequest().getSysCn());
                            p.complete();
                        })
                        .onFailure(error -> {
                            log.error("Rollback data in plugin [{}] failed", sourceType, error);
                            p.fail(error);
                        }))));

        return Future.future(rbPromise ->
                CompositeFuture.join(futures)
                        .onSuccess(ar -> {
                            log.debug("Rollback data successfully finished in all plugins");
                            rbPromise.complete();
                        })
                        .onFailure(error -> {
                            log.error("Rollback data failed", error.getCause());
                            rbPromise.fail(
                                    new CrashException("Error in rolling back data → Fatal error. Operation failed on execute and failed on undo.",
                                            error)
                            );
                        }));
    }
}
