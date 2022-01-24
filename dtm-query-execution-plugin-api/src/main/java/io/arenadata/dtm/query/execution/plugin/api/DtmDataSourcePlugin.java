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
package io.arenadata.dtm.query.execution.plugin.api;

import io.arenadata.dtm.common.plugin.status.StatusQueryResult;
import io.arenadata.dtm.common.reader.QueryResult;
import io.arenadata.dtm.common.reader.SourceType;
import io.arenadata.dtm.common.version.VersionInfo;
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
import org.springframework.plugin.core.Plugin;

import java.util.List;
import java.util.Set;

/**
 * Data source plugin interface
 */
public interface DtmDataSourcePlugin extends Plugin<SourceType> {

    /**
     * <p>Data source type support</p>
     *
     * @param sourceType data source type
     * @return is support
     */
    default boolean supports(SourceType sourceType) {
        return getSourceType() == sourceType;
    }

    /**
     * <p>Get data source type</p>
     *
     * @return data source type
     */
    SourceType getSourceType();

    /**
     * <p>execute DDL operation</p>
     *
     * @param request DDL context
     * @return void
     */
    Future<Void> ddl(DdlRequest request);

    /**
     * <p>execute Low Latency Reading</p>
     *
     * @param request LLR context
     * @return query result
     */
    Future<QueryResult> llr(LlrRequest request);

    /**
     * <p>execute Low Latency Reading Estimate</p>
     *
     * @param request LLR context
     * @return query result
     */
    Future<QueryResult> llrEstimate(LlrRequest request);

    /**
     * <p>execute Low Latency Write Upsert Values</p>
     *
     * @param request LLW context
     * @return void
     */
    Future<Void> upsert(UpsertValuesRequest request);

    /**
     * <p>execute Low Latency Write Upsert Select</p>
     *
     * @param request LLW context
     * @return void
     */
    Future<Void> upsert(UpsertSelectRequest request);

    /**
     * <p>execute Low Latency Write Delete</p>
     *
     * @param request LLW context
     * @return void
     */
    Future<Void> delete(DeleteRequest request);

    /**
     * <p>execute Massively Parallel Processing Reading</p>
     *
     * @param request MPPR context
     * @return query result
     */
    Future<QueryResult> mppr(MpprRequest request);

    /**
     * <p>execute Massively Parallel Processing Writing</p>
     *
     * @param request MPPW context
     * @return Consumer group if it's start request, else empty string
     */
    Future<String> mppw(MppwRequest request);

    /**
     * <p>Get plugin status information</p>
     *
     * @param topic Topic
     * @param consumerGroup consumer group
     * @return query status
     */
    Future<StatusQueryResult> status(String topic, String consumerGroup);

    /**
     * @param request Rollback request
     * @return void
     */
    Future<Void> rollback(RollbackRequest request);

    /**
     * <p>Get name set of active caches</p>
     *
     * @return set of caches names
     */
    Set<String> getActiveCaches();

    /**
     * @param request check table request
     * @return error if check failed
     */
    Future<Void> checkTable(CheckTableRequest request);

    /**
     * @param request CheckDataByCountParams
     * @return count of records
     */
    Future<Long> checkDataByCount(CheckDataByCountRequest request);

    /**
     * @param request CheckDataByHashInt32Params
     * @return checksum
     */
    Future<Long> checkDataByHashInt32(CheckDataByHashInt32Request request);

    /**
     * @param request CheckVersionRequest
     * @return list of version info
     */
    Future<List<VersionInfo>> checkVersion(CheckVersionRequest request);

    /**
     * @param request truncate params
     * @return future object
     */
    Future<Void> truncateHistory(TruncateHistoryRequest request);

    /**
     * @param request synchronize params
     * @return deltaNum
     */
    Future<Long> synchronize(SynchronizeRequest request);

    /**
     * <p>initialize plugin by source type</p>
     * @return void
     */
    Future<Void> initialize();
}
