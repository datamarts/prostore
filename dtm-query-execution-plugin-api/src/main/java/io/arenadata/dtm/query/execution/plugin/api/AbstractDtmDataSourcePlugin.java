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
import io.arenadata.dtm.query.execution.plugin.api.service.*;
import io.arenadata.dtm.query.execution.plugin.api.service.check.CheckDataService;
import io.arenadata.dtm.query.execution.plugin.api.service.check.CheckTableService;
import io.arenadata.dtm.query.execution.plugin.api.service.check.CheckVersionService;
import io.arenadata.dtm.query.execution.plugin.api.service.ddl.TruncateHistoryService;
import io.arenadata.dtm.query.execution.plugin.api.service.mppr.MpprService;
import io.arenadata.dtm.query.execution.plugin.api.service.mppw.MppwService;
import io.arenadata.dtm.query.execution.plugin.api.synchronize.SynchronizeRequest;
import io.vertx.core.Future;

import java.util.List;

public abstract class AbstractDtmDataSourcePlugin implements DtmDataSourcePlugin {

    protected final DdlService<Void> ddlService;
    protected final LlrService<QueryResult> llrService;
    protected final UpsertValuesService upsertValuesService;
    protected final UpsertSelectService upsertSelectService;
    protected final DeleteService deleteService;
    protected final MpprService mpprService;
    protected final MppwService mppwService;
    protected final StatusService statusService;
    protected final RollbackService<Void> rollbackService;
    protected final CheckTableService checkTableService;
    protected final CheckDataService checkDataService;
    protected final CheckVersionService checkVersionService;
    protected final TruncateHistoryService truncateService;
    protected final PluginInitializationService initializationService;
    protected final SynchronizeService synchronizeService;

    protected AbstractDtmDataSourcePlugin(DdlService<Void> ddlService,
                                       LlrService<QueryResult> llrService,
                                       UpsertValuesService upsertValuesService,
                                       UpsertSelectService upsertSelectService,
                                       DeleteService deleteService,
                                       MpprService mpprService,
                                       MppwService mppwService,
                                       StatusService statusService,
                                       RollbackService<Void> rollbackService,
                                       CheckTableService checkTableService,
                                       CheckDataService checkDataService,
                                       CheckVersionService checkVersionService,
                                       TruncateHistoryService truncateService,
                                       PluginInitializationService initializationService,
                                       SynchronizeService synchronizeService) {
        this.ddlService = ddlService;
        this.llrService = llrService;
        this.upsertValuesService = upsertValuesService;
        this.upsertSelectService = upsertSelectService;
        this.deleteService = deleteService;
        this.mpprService = mpprService;
        this.mppwService = mppwService;
        this.statusService = statusService;
        this.rollbackService = rollbackService;
        this.checkTableService = checkTableService;
        this.checkDataService = checkDataService;
        this.checkVersionService = checkVersionService;
        this.truncateService = truncateService;
        this.initializationService = initializationService;
        this.synchronizeService = synchronizeService;
    }

    @Override
    public Future<Void> ddl(DdlRequest request) {
        return ddlService.execute(request);
    }

    @Override
    public Future<QueryResult> llr(LlrRequest request) {
        return llrService.execute(request);
    }

    @Override
    public Future<QueryResult> llrEstimate(LlrRequest request) {
        return llrService.execute(request.toBuilder()
                .estimate(true)
                .build());
    }

    @Override
    public Future<Void> upsert(UpsertValuesRequest request) {
        return upsertValuesService.execute(request);
    }

    @Override
    public Future<Void> upsert(UpsertSelectRequest request) {
        return upsertSelectService.execute(request);
    }

    @Override
    public Future<Void> delete(DeleteRequest request) {
        return deleteService.execute(request);
    }

    @Override
    public Future<QueryResult> mppr(MpprRequest request) {
        return mpprService.execute(request);
    }

    @Override
    public Future<String> mppw(MppwRequest request) {
        return mppwService.execute(request);
    }

    @Override
    public Future<StatusQueryResult> status(String topic, String consumerGroup) {
        return statusService.execute(topic, consumerGroup);
    }

    @Override
    public Future<Void> rollback(RollbackRequest request) {
        return rollbackService.execute(request);
    }

    @Override
    public Future<Void> checkTable(CheckTableRequest request) {
        return checkTableService.check(request);
    }

    @Override
    public Future<Long> checkDataByCount(CheckDataByCountRequest request) {
        return checkDataService.checkDataByCount(request);
    }

    @Override
    public Future<Long> checkDataByHashInt32(CheckDataByHashInt32Request params) {
        return checkDataService.checkDataByHashInt32(params);
    }

    @Override
    public Future<List<VersionInfo>> checkVersion(CheckVersionRequest request) {
        return checkVersionService.checkVersion(request);
    }

    @Override
    public Future<Void> truncateHistory(TruncateHistoryRequest request) {
        return truncateService.truncateHistory(request);
    }

    @Override
    public Future<Long> synchronize(SynchronizeRequest request) {
        return synchronizeService.execute(request);
    }

    @Override
    public Future<Void> initialize() {
        return initializationService.execute();
    }
}
