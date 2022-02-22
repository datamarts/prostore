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
package ru.datamart.prostore.query.execution.plugin.api;

import io.vertx.core.Future;
import ru.datamart.prostore.common.plugin.status.StatusQueryResult;
import ru.datamart.prostore.common.reader.QueryResult;
import ru.datamart.prostore.common.version.VersionInfo;
import ru.datamart.prostore.query.execution.plugin.api.check.CheckDataByCountRequest;
import ru.datamart.prostore.query.execution.plugin.api.check.CheckDataByHashInt32Request;
import ru.datamart.prostore.query.execution.plugin.api.check.CheckTableRequest;
import ru.datamart.prostore.query.execution.plugin.api.check.CheckVersionRequest;
import ru.datamart.prostore.query.execution.plugin.api.dto.RollbackRequest;
import ru.datamart.prostore.query.execution.plugin.api.dto.TruncateHistoryRequest;
import ru.datamart.prostore.query.execution.plugin.api.mppr.MpprRequest;
import ru.datamart.prostore.query.execution.plugin.api.mppw.MppwRequest;
import ru.datamart.prostore.query.execution.plugin.api.request.*;
import ru.datamart.prostore.query.execution.plugin.api.service.*;
import ru.datamart.prostore.query.execution.plugin.api.service.check.CheckDataService;
import ru.datamart.prostore.query.execution.plugin.api.service.check.CheckTableService;
import ru.datamart.prostore.query.execution.plugin.api.service.check.CheckVersionService;
import ru.datamart.prostore.query.execution.plugin.api.service.ddl.TruncateHistoryService;
import ru.datamart.prostore.query.execution.plugin.api.service.mppr.MpprService;
import ru.datamart.prostore.query.execution.plugin.api.service.mppw.MppwService;
import ru.datamart.prostore.query.execution.plugin.api.synchronize.SynchronizeRequest;

import java.util.List;

public abstract class AbstractDtmDataSourcePlugin implements DtmDataSourcePlugin {

    protected final DdlService<Void> ddlService;
    protected final LlrService<QueryResult> llrService;
    protected final InsertValuesService insertValuesService;
    protected final InsertSelectService insertSelectService;
    protected final UpsertValuesService upsertValuesService;
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
                                          InsertValuesService insertValuesService,
                                          InsertSelectService insertSelectService,
                                          UpsertValuesService upsertValuesService,
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
        this.insertValuesService = insertValuesService;
        this.insertSelectService = insertSelectService;
        this.upsertValuesService = upsertValuesService;
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
    public Future<Void> insert(InsertValuesRequest request) {
        return insertValuesService.execute(request);
    }

    @Override
    public Future<Void> insert(InsertSelectRequest request) {
        return insertSelectService.execute(request);
    }

    @Override
    public Future<Void> upsert(UpsertValuesRequest request) {
        return upsertValuesService.execute(request);
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
    public Future<Long> checkDataSnapshotByHashInt32(CheckDataByHashInt32Request params) {
        return checkDataService.checkDataSnapshotByHashInt32(params);
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
