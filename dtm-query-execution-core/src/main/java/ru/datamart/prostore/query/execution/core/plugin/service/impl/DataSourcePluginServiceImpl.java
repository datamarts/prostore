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
package ru.datamart.prostore.query.execution.core.plugin.service.impl;

import io.vertx.core.Future;
import io.vertx.core.Handler;
import io.vertx.core.Promise;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.plugin.core.PluginRegistry;
import org.springframework.plugin.core.config.EnablePluginRegistries;
import org.springframework.stereotype.Service;
import ru.datamart.prostore.common.metrics.RequestMetrics;
import ru.datamart.prostore.common.model.SqlProcessingType;
import ru.datamart.prostore.common.plugin.status.StatusQueryResult;
import ru.datamart.prostore.common.reader.QueryResult;
import ru.datamart.prostore.common.reader.SourceType;
import ru.datamart.prostore.common.version.VersionInfo;
import ru.datamart.prostore.query.execution.core.base.verticle.TaskVerticleExecutor;
import ru.datamart.prostore.query.execution.core.metrics.service.MetricsService;
import ru.datamart.prostore.query.execution.core.plugin.service.DataSourcePluginService;
import ru.datamart.prostore.query.execution.plugin.api.DtmDataSourcePlugin;
import ru.datamart.prostore.query.execution.plugin.api.check.CheckDataByCountRequest;
import ru.datamart.prostore.query.execution.plugin.api.check.CheckDataByHashInt32Request;
import ru.datamart.prostore.query.execution.plugin.api.check.CheckTableRequest;
import ru.datamart.prostore.query.execution.plugin.api.check.CheckVersionRequest;
import ru.datamart.prostore.query.execution.plugin.api.dto.RollbackRequest;
import ru.datamart.prostore.query.execution.plugin.api.dto.TruncateHistoryRequest;
import ru.datamart.prostore.query.execution.plugin.api.mppr.MpprRequest;
import ru.datamart.prostore.query.execution.plugin.api.mppw.MppwRequest;
import ru.datamart.prostore.query.execution.plugin.api.request.*;
import ru.datamart.prostore.query.execution.plugin.api.synchronize.SynchronizeRequest;

import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;

@Slf4j
@Service
@EnablePluginRegistries({DtmDataSourcePlugin.class})
public class DataSourcePluginServiceImpl implements DataSourcePluginService {

    private final PluginRegistry<DtmDataSourcePlugin, SourceType> pluginRegistry;
    private final TaskVerticleExecutor taskVerticleExecutor;
    private final Set<SourceType> sourceTypes;
    private final Set<String> activeCaches;
    private final MetricsService metricsService;

    @Autowired
    public DataSourcePluginServiceImpl(
            PluginRegistry<DtmDataSourcePlugin, SourceType> pluginRegistry,
            TaskVerticleExecutor taskVerticleExecutor,
            @Qualifier("coreMetricsService") MetricsService metricsService) {
        this.taskVerticleExecutor = taskVerticleExecutor;
        this.pluginRegistry = pluginRegistry;
        this.sourceTypes = pluginRegistry.getPlugins().stream()
                .map(DtmDataSourcePlugin::getSourceType)
                .collect(Collectors.toSet());
        this.activeCaches = pluginRegistry.getPlugins().stream()
                .flatMap(plugin -> plugin.getActiveCaches().stream())
                .collect(Collectors.toSet());
        this.metricsService = metricsService;
        log.info("Active Plugins: {}", sourceTypes.toString());
    }

    @Override
    public Set<SourceType> getSourceTypes() {
        return new HashSet<>(sourceTypes);
    }

    @Override
    public boolean hasSourceType(SourceType sourceType) {
        return sourceTypes.contains(sourceType);
    }

    @Override
    public Future<Void> ddl(SourceType sourceType, RequestMetrics metrics, DdlRequest request) {
        return executeWithMetrics(sourceType,
                SqlProcessingType.DDL,
                metrics,
                plugin -> plugin.ddl(request));
    }

    @Override
    public Future<QueryResult> llr(SourceType sourceType, RequestMetrics metrics, LlrRequest request) {
        return executeWithMetrics(sourceType,
                SqlProcessingType.LLR,
                metrics,
                plugin -> plugin.llr(request));
    }

    @Override
    public Future<QueryResult> llrEstimate(SourceType sourceType, RequestMetrics metrics, LlrRequest request) {
        return executeWithMetrics(sourceType,
                SqlProcessingType.LLR_ESTIMATE,
                metrics,
                plugin -> plugin.llrEstimate(request));
    }

    @Override
    public Future<Void> insert(SourceType sourceType, RequestMetrics metrics, InsertValuesRequest request) {
        return executeWithMetrics(sourceType,
                SqlProcessingType.LLW,
                metrics,
                plugin -> plugin.insert(request));
    }

    @Override
    public Future<Void> insert(SourceType sourceType, RequestMetrics metrics, InsertSelectRequest request) {
        return executeWithMetrics(sourceType,
                SqlProcessingType.LLW,
                metrics,
                plugin -> plugin.insert(request));
    }

    @Override
    public Future<Void> upsert(SourceType sourceType, RequestMetrics metrics, UpsertValuesRequest request) {
        return executeWithMetrics(sourceType,
                SqlProcessingType.LLW,
                metrics,
                plugin -> plugin.upsert(request));
    }

    @Override
    public Future<Void> delete(SourceType sourceType, RequestMetrics metrics, DeleteRequest request) {
        return executeWithMetrics(sourceType,
                SqlProcessingType.LLW,
                metrics,
                plugin -> plugin.delete(request));
    }

    @Override
    public Future<QueryResult> mppr(SourceType sourceType, RequestMetrics metrics, MpprRequest request) {
        return executeWithMetrics(sourceType,
                SqlProcessingType.MPPR,
                metrics,
                plugin -> plugin.mppr(request));
    }

    @Override
    public Future<String> mppw(SourceType sourceType, RequestMetrics metrics, MppwRequest request) {
        return executeWithMetrics(sourceType,
                SqlProcessingType.MPPW,
                metrics,
                plugin -> plugin.mppw(request));
    }

    @Override
    public Future<StatusQueryResult> status(SourceType sourceType, RequestMetrics metrics, String topic, String consumerGroup) {
        return executeWithMetrics(sourceType,
                SqlProcessingType.STATUS,
                metrics,
                plugin -> plugin.status(topic, consumerGroup));
    }

    @Override
    public Future<Void> rollback(SourceType sourceType, RequestMetrics metrics, RollbackRequest request) {
        return executeWithMetrics(sourceType,
                SqlProcessingType.ROLLBACK,
                metrics,
                plugin -> plugin.rollback(request));
    }

    @Override
    public DtmDataSourcePlugin getPlugin(SourceType sourceType) {
        return pluginRegistry.getRequiredPluginFor(sourceType);
    }

    @Override
    public Set<String> getActiveCaches() {
        return activeCaches;
    }

    @Override
    public Future<Void> checkTable(SourceType sourceType,
                                   RequestMetrics metrics,
                                   CheckTableRequest checkTableRequest) {
        return executeWithMetrics(sourceType,
                SqlProcessingType.CHECK,
                metrics,
                plugin -> plugin.checkTable(checkTableRequest));
    }

    @Override
    public Future<Long> checkDataByCount(SourceType sourceType,
                                         RequestMetrics metrics,
                                         CheckDataByCountRequest request) {
        return executeWithMetrics(sourceType,
                SqlProcessingType.CHECK,
                metrics,
                plugin -> plugin.checkDataByCount(request));
    }

    @Override
    public Future<Long> checkDataByHashInt32(SourceType sourceType,
                                             RequestMetrics metrics,
                                             CheckDataByHashInt32Request request) {
        return executeWithMetrics(
                sourceType,
                SqlProcessingType.CHECK,
                metrics,
                plugin -> plugin.checkDataByHashInt32(request));
    }

    @Override
    public Future<Long> checkDataSnapshotByHashInt32(SourceType sourceType, RequestMetrics metrics, CheckDataByHashInt32Request request) {
        return executeWithMetrics(
                sourceType,
                SqlProcessingType.CHECK,
                metrics,
                plugin -> plugin.checkDataSnapshotByHashInt32(request));
    }

    @Override
    public Future<List<VersionInfo>> checkVersion(SourceType sourceType, RequestMetrics metrics, CheckVersionRequest request) {
        return executeWithMetrics(
                sourceType,
                SqlProcessingType.CHECK,
                metrics,
                plugin -> plugin.checkVersion(request));
    }

    @Override
    public Future<Void> truncateHistory(SourceType sourceType,
                                        RequestMetrics metrics,
                                        TruncateHistoryRequest request) {
        return executeWithMetrics(
                sourceType,
                SqlProcessingType.TRUNCATE,
                metrics,
                plugin -> plugin.truncateHistory(request));
    }

    @Override
    public Future<Long> synchronize(SourceType sourceType,
                                    RequestMetrics metrics,
                                    SynchronizeRequest request) {
        return executeWithMetrics(
                sourceType,
                SqlProcessingType.SYNCHRONIZE,
                metrics,
                plugin -> plugin.synchronize(request));
    }

    @Override
    public Future<Void> initialize(SourceType sourceType) {
        return taskVerticleExecutor.execute(promise -> getPlugin(sourceType).initialize().onComplete(promise));
    }

    private <T> Future<T> executeWithMetrics(SourceType sourceType,
                                             SqlProcessingType sqlProcessingType,
                                             RequestMetrics requestMetrics,
                                             Function<DtmDataSourcePlugin, Future<T>> func) {
        return Future.future((Promise<T> promise) ->
                metricsService.sendMetrics(sourceType,
                        sqlProcessingType,
                        requestMetrics)
                        .compose(result -> taskVerticleExecutor.execute((Handler<Promise<T>>) p -> func.apply(getPlugin(sourceType)).onComplete(p)))
                        .onComplete(metricsService.sendMetrics(sourceType,
                                sqlProcessingType,
                                requestMetrics,
                                promise)));
    }

}
