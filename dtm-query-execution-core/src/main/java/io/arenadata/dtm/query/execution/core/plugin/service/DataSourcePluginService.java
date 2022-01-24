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
package io.arenadata.dtm.query.execution.core.plugin.service;

import io.arenadata.dtm.common.metrics.RequestMetrics;
import io.arenadata.dtm.common.plugin.status.StatusQueryResult;
import io.arenadata.dtm.common.reader.QueryResult;
import io.arenadata.dtm.common.reader.SourceType;
import io.arenadata.dtm.common.version.VersionInfo;
import io.arenadata.dtm.query.execution.plugin.api.DtmDataSourcePlugin;
import io.arenadata.dtm.query.execution.plugin.api.check.CheckDataByCountRequest;
import io.arenadata.dtm.query.execution.plugin.api.check.CheckDataByHashInt32Request;
import io.arenadata.dtm.query.execution.plugin.api.check.CheckTableRequest;
import io.arenadata.dtm.query.execution.plugin.api.check.CheckVersionRequest;
import io.arenadata.dtm.query.execution.plugin.api.dto.RollbackRequest;
import io.arenadata.dtm.query.execution.plugin.api.dto.TruncateHistoryRequest;
import io.arenadata.dtm.query.execution.plugin.api.mppr.MpprRequest;
import io.arenadata.dtm.query.execution.plugin.api.mppw.MppwRequest;
import io.arenadata.dtm.query.execution.plugin.api.request.*;
import io.arenadata.dtm.query.execution.plugin.api.synchronize.SynchronizeRequest;
import io.vertx.core.Future;

import java.util.List;
import java.util.Set;

/**
 * Service for interaction of the core with data source plugins
 */
public interface DataSourcePluginService {

    /**
     * <p>Get data source type</p>
     *
     * @return Data source type
     */
    Set<SourceType> getSourceTypes();

    /**
     * <p>Checks sourceType exists</p>
     *
     * @return true if exist, false otherwise
     */
    boolean hasSourceType(SourceType sourceType);

    /**
     * <p>execute DDL operation</p>
     *
     * @param sourceType Data source type
     * @param request    DDL context
     * @return future object
     */
    Future<Void> ddl(SourceType sourceType, RequestMetrics metrics, DdlRequest request);

    /**
     * <p>execute Low Latency Reading request</p>
     *
     * @param sourceType Data source type
     * @param metrics    metrics
     * @param llrRequest llr request
     * @return future object
     */
    Future<QueryResult> llr(SourceType sourceType, RequestMetrics metrics, LlrRequest llrRequest);

    /**
     * <p>execute Low Latency Reading Estimate request</p>
     *
     * @param sourceType Data source type
     * @param metrics    metrics
     * @param llrRequest llr request
     * @return future object
     */
    Future<QueryResult> llrEstimate(SourceType sourceType, RequestMetrics metrics, LlrRequest llrRequest);

    /**
     * <p>execute Low Latency Write Upsert Values request</p>
     *
     * @param sourceType    Data source type
     * @param metrics       metrics
     * @param upsertRequest llw request
     * @return future object
     */
    Future<Void> upsert(SourceType sourceType, RequestMetrics metrics, UpsertValuesRequest upsertRequest);

    /**
     * <p>execute Low Latency Write Upsert Select request</p>
     *
     * @param sourceType    Data source type
     * @param metrics       metrics
     * @param upsertRequest llw request
     * @return future object
     */
    Future<Void> upsert(SourceType sourceType, RequestMetrics metrics, UpsertSelectRequest upsertRequest);

    /**
     * <p>execute Low Latency Write Delete request</p>
     *
     * @param sourceType    Data source type
     * @param metrics       metrics
     * @param deleteRequest llw request
     * @return future object
     */
    Future<Void> delete(SourceType sourceType, RequestMetrics metrics, DeleteRequest deleteRequest);

    /**
     * <p>execute Massively Parallel Processing Reading</p>
     *
     * @param sourceType Data source type
     * @param request    MPPR plugin request
     * @return future object
     */
    Future<QueryResult> mppr(SourceType sourceType, RequestMetrics metrics, MpprRequest request);

    /**
     * <p>execute Massively Parallel Processing Writing</p>
     *
     * @param sourceType Data source type
     * @param request    MPPW plugin request
     * @return Consumer group if it's start load request, null otherwise
     */
    Future<String> mppw(SourceType sourceType, RequestMetrics metrics, MppwRequest request);

    /**
     * <p>Get plugin status information</p>
     *
     * @param sourceType Data source type
     * @param topic      Topic
     * @return future object
     */
    Future<StatusQueryResult> status(SourceType sourceType, RequestMetrics metrics, String topic, String consumerGroup);

    /**
     * @param sourceType Data source type
     * @param request    Rollback request
     * @return future object
     */
    Future<Void> rollback(SourceType sourceType, RequestMetrics metrics, RollbackRequest request);

    /**
     * Get plugin by source type
     *
     * @param sourceType Data source type
     * @return plugin
     */
    DtmDataSourcePlugin getPlugin(SourceType sourceType);

    /**
     * <p>Get name set of active caches</p>
     *
     * @return set of caches names
     */
    Set<String> getActiveCaches();

    /**
     * @param sourceType        SourceType
     * @param checkTableRequest
     * @return failed future with errors if check failed
     */
    Future<Void> checkTable(SourceType sourceType, RequestMetrics metrics, CheckTableRequest checkTableRequest);

    /**
     * @param request CheckDataByCountParams
     * @return count of records
     */
    Future<Long> checkDataByCount(SourceType sourceType, RequestMetrics metrics, CheckDataByCountRequest request);

    /**
     * @param request CheckDataByHashInt32Params
     * @return checksum
     */
    Future<Long> checkDataByHashInt32(SourceType sourceType, RequestMetrics metrics, CheckDataByHashInt32Request request);

    /**
     * @param request CheckVersionRequest
     * @return checksum
     */
    Future<List<VersionInfo>> checkVersion(SourceType sourceType, RequestMetrics metrics, CheckVersionRequest request);

    /**
     * @param request TruncateHistoryRequest
     * @return void
     */
    Future<Void> truncateHistory(SourceType sourceType, RequestMetrics metrics, TruncateHistoryRequest request);

    /**
     * @param request SynchronizeRequest
     * @return deltaNum
     */
    Future<Long> synchronize(SourceType sourceType, RequestMetrics metrics, SynchronizeRequest request);

    /**
     * <p>initialize plugin by source type</p>
     *
     * @param sourceType
     * @return void
     */
    Future<Void> initialize(SourceType sourceType);
}
