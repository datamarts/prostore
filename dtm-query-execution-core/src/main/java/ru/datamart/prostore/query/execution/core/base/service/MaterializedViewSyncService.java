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
package ru.datamart.prostore.query.execution.core.base.service;

import ru.datamart.prostore.cache.service.CacheService;
import ru.datamart.prostore.cache.service.EvictQueryTemplateCacheService;
import ru.datamart.prostore.common.configuration.core.CoreConstants;
import ru.datamart.prostore.common.delta.DeltaData;
import ru.datamart.prostore.common.exception.DtmException;
import ru.datamart.prostore.common.metrics.RequestMetrics;
import ru.datamart.prostore.common.model.RequestStatus;
import ru.datamart.prostore.common.model.ddl.Entity;
import ru.datamart.prostore.query.calcite.core.service.DefinitionService;
import ru.datamart.prostore.query.execution.core.base.configuration.AppConfiguration;
import ru.datamart.prostore.query.execution.core.base.configuration.properties.MatViewSyncProperties;
import ru.datamart.prostore.query.execution.core.base.dto.cache.EntityKey;
import ru.datamart.prostore.query.execution.core.base.dto.cache.MaterializedViewCacheValue;
import ru.datamart.prostore.query.execution.core.base.dto.cache.MaterializedViewSyncStatus;
import ru.datamart.prostore.query.execution.core.base.repository.zookeeper.EntityDao;
import ru.datamart.prostore.query.execution.core.base.service.delta.DeltaInformationService;
import ru.datamart.prostore.query.execution.core.base.service.metadata.LogicalSchemaProvider;
import ru.datamart.prostore.query.execution.core.delta.repository.zookeeper.DeltaServiceDao;
import ru.datamart.prostore.query.execution.core.plugin.exception.SuitablePluginNotExistsException;
import ru.datamart.prostore.query.execution.core.plugin.service.DataSourcePluginService;
import ru.datamart.prostore.query.execution.core.query.utils.LoggerContextUtils;
import ru.datamart.prostore.query.execution.model.metadata.Datamart;
import ru.datamart.prostore.query.execution.plugin.api.synchronize.SynchronizeRequest;
import io.vertx.core.Future;
import io.vertx.core.Vertx;
import lombok.extern.slf4j.Slf4j;
import lombok.val;
import org.apache.calcite.sql.SqlNode;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Service;

import java.time.LocalDateTime;
import java.util.List;
import java.util.Objects;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicInteger;

@Slf4j
@Service
public class MaterializedViewSyncService {

    private final DataSourcePluginService dataSourcePluginService;
    private final CacheService<EntityKey, MaterializedViewCacheValue> materializedViewCacheService;
    private final DeltaServiceDao deltaServiceDao;
    private final DeltaInformationService deltaInformationService;
    private final EntityDao entityDao;
    private final DefinitionService<SqlNode> definitionService;
    private final LogicalSchemaProvider logicalSchemaProvider;
    private final Vertx vertx;
    private final long retryCount;
    private final long periodMs;
    private final long maxConcurrent;
    private final AtomicInteger concurrentSyncCount = new AtomicInteger(0);
    private final AppConfiguration appConfiguration;
    private final EvictQueryTemplateCacheService evictQueryTemplateCacheService;

    public MaterializedViewSyncService(DataSourcePluginService dataSourcePluginService,
                                       CacheService<EntityKey, MaterializedViewCacheValue> materializedViewCacheService,
                                       DeltaServiceDao deltaServiceDao,
                                       DeltaInformationService deltaInformationService,
                                       EntityDao entityDao,
                                       @Qualifier("coreCalciteDefinitionService") DefinitionService<SqlNode> definitionService,
                                       LogicalSchemaProvider logicalSchemaProvider,
                                       @Qualifier("coreVertx") Vertx vertx,
                                       MatViewSyncProperties matViewSyncProperties,
                                       AppConfiguration appConfiguration,
                                       EvictQueryTemplateCacheService evictQueryTemplateCacheService) {
        this.dataSourcePluginService = dataSourcePluginService;
        this.materializedViewCacheService = materializedViewCacheService;
        this.deltaServiceDao = deltaServiceDao;
        this.deltaInformationService = deltaInformationService;
        this.entityDao = entityDao;
        this.definitionService = definitionService;
        this.logicalSchemaProvider = logicalSchemaProvider;
        this.vertx = vertx;
        this.retryCount = matViewSyncProperties.getRetryCount();
        this.periodMs = matViewSyncProperties.getPeriodMs();
        this.maxConcurrent = matViewSyncProperties.getMaxConcurrent();
        this.appConfiguration = appConfiguration;
        this.evictQueryTemplateCacheService = evictQueryTemplateCacheService;
    }

    public boolean isSyncEnabled() {
        return periodMs > 0;
    }

    public long startPeriodicalSync() {
        return vertx.setTimer(periodMs, timerId -> {
            LoggerContextUtils.setRequestId(UUID.randomUUID());
            log.info("Materialized view synchronization tick");
            materializedViewCacheService.forEach(this::startSyncProcess);
            startPeriodicalSync();
        });
    }

    private Future<Void> startSyncProcess(EntityKey key, MaterializedViewCacheValue value) {
        return Future.future(promise -> {
            val datamart = key.getDatamartName();
            val origUUID = value.getUuid();
            val entity = value.getEntity();
            if (value.isMarkedForDeletion()) {
                materializedViewCacheService.remove(key);
                promise.complete();
                return;
            }
            isReadyForSync(datamart, value.getStatus(), entity.getMaterializedDeltaNum(), value.getFailsCount())
                    .onSuccess(isReady -> {
                        if (isReady) {
                            log.info("Started sync process for {}", value);
                            runSync(datamart, value, origUUID)
                                    .onComplete(event -> value.setNotInSync())
                                    .onSuccess(v -> {
                                        value.resetLastSyncError();
                                        concurrentSyncCount.decrementAndGet();
                                        log.info("Materialized view {} synchronized", entity.getNameWithSchema());
                                        promise.complete();
                                    })
                                    .onFailure(error -> {
                                        value.setLastSyncError(error);
                                        concurrentSyncCount.decrementAndGet();
                                        log.error("Failed to sync materialized view {}, fails count {}/{}", entity.getNameWithSchema(), value.getFailsCount() + 1, retryCount, error);
                                        if (origUUID.equals(value.getUuid())) {
                                            value.incrementFailsCount();
                                            value.setStatus(MaterializedViewSyncStatus.READY);
                                        }
                                        promise.complete();
                                    });
                        } else {
                            promise.complete();
                        }
                    })
                    .onFailure(error -> {
                        log.warn("Can't start materialized view sync cause can't get delta ok for datamart {}", datamart, error);
                        promise.complete();
                    });
        });
    }

    private Future<Boolean> isReadyForSync(String datamart, MaterializedViewSyncStatus matViewStatus, Long matViewDeltaNum, long matViewRetryCount) {
        if (concurrentSyncCount.get() >= maxConcurrent || MaterializedViewSyncStatus.READY != matViewStatus || matViewRetryCount >= retryCount) {
            return Future.succeededFuture(false);
        }
        return deltaServiceDao.getDeltaOk(datamart)
                .map(okDelta -> ((okDelta != null && okDelta.getDeltaNum() >= 0L)
                        && (matViewDeltaNum == null || matViewDeltaNum < okDelta.getDeltaNum())));
    }

    private Future<Void> runSync(String datamart, MaterializedViewCacheValue value, UUID origUUID) {
        value.setInSync();
        value.setLastSyncTime(LocalDateTime.now());
        concurrentSyncCount.incrementAndGet();
        value.setStatus(MaterializedViewSyncStatus.RUN);
        return synchronize(datamart, value.getEntity())
                .compose(deltaNum -> origUUID.equals(value.getUuid()) ? updateEntity(deltaNum, value) : Future.succeededFuture());
    }

    private Future<Long> synchronize(String datamart, Entity matViewEntity) {
        return Future.future(promise -> {
            if (!dataSourcePluginService.hasSourceType(matViewEntity.getMaterializedDataSource())) {
                throw new SuitablePluginNotExistsException();
            }

            val uuid = UUID.randomUUID();
            preparePluginContext(datamart, matViewEntity)
                    .compose(context -> dataSourcePluginService.synchronize(matViewEntity.getMaterializedDataSource(),
                            createRequestMetrics(uuid), prepareRequest(uuid, datamart, matViewEntity, context)))
                    .onComplete(promise);
        });
    }

    private SynchronizeRequest prepareRequest(UUID uuid, String datamart, Entity matViewEntity, SynchronizePluginContext context) {
        return new SynchronizeRequest(uuid, appConfiguration.getEnvName(), datamart, context.querySchema,
                matViewEntity, context.viewQuery, context.deltaToBe, context.previousDeltaCnTo);
    }

    private Future<SynchronizePluginContext> preparePluginContext(String datamart, Entity matViewEntity) {
        return Future.future(promise -> {
            val sqlNode = definitionService.processingQuery(matViewEntity.getViewQuery());
            val synchronizeRequest = new SynchronizePluginContext(sqlNode);
            long deltaNumToBe = getDeltaNumToBe(matViewEntity);
            long deltaNumBefore = deltaNumToBe - 1;
            logicalSchemaProvider.getSchemaFromQuery(sqlNode, datamart)
                    .compose(datamarts -> {
                        synchronizeRequest.querySchema = datamarts;
                        return deltaServiceDao.getDeltaByNum(datamart, deltaNumToBe);
                    })
                    .compose(okDelta -> {
                        synchronizeRequest.deltaToBe = new DeltaData(deltaNumToBe, okDelta.getCnFrom(), okDelta.getCnTo());
                        return deltaInformationService.getCnToByDeltaNum(datamart, deltaNumBefore);
                    })
                    .onSuccess(beforeDeltaNumToBeCnTo -> {
                        synchronizeRequest.previousDeltaCnTo = beforeDeltaNumToBeCnTo;
                        promise.complete(synchronizeRequest);
                    })
                    .onFailure(promise::fail);
        });
    }

    private Long getDeltaNumToBe(Entity matViewEntity) {
        if (matViewEntity.getMaterializedDeltaNum() == null) {
            return 0L;
        }

        return matViewEntity.getMaterializedDeltaNum() + 1L;
    }

    private Future<Void> updateEntity(long deltaNum, MaterializedViewCacheValue cacheValue) {
        val entity = cacheValue.getEntity();
        val oldDeltaNum = entity.getMaterializedDeltaNum();
        if (!Objects.equals(oldDeltaNum, deltaNum)) {
            entity.setMaterializedDeltaNum(deltaNum);
            return entityDao.updateEntity(entity)
                    .map(v -> {
                        evictQueryTemplateCacheService.evictByDatamartName(entity.getSchema());
                        cacheValue.resetFailsCount();
                        cacheValue.setStatus(MaterializedViewSyncStatus.READY);
                        return v;
                    });
        }
        return Future.failedFuture(new DtmException(String.format("DeltaNum for materialized view %s has not been changed; old value [%d], new value [%d]",
                entity.getNameWithSchema(), oldDeltaNum, deltaNum)));
    }

    private RequestMetrics createRequestMetrics(UUID uuid) {
        return RequestMetrics.builder()
                .startTime(LocalDateTime.now(CoreConstants.CORE_ZONE_ID))
                .requestId(uuid)
                .status(RequestStatus.IN_PROCESS)
                .isActive(true)
                .build();
    }

    private static class SynchronizePluginContext {
        private final SqlNode viewQuery;
        private List<Datamart> querySchema;
        private DeltaData deltaToBe;
        private Long previousDeltaCnTo;

        private SynchronizePluginContext(SqlNode viewQuery) {
            this.viewQuery = viewQuery;
        }
    }
}
