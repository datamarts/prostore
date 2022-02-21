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
package ru.datamart.prostore.query.execution.plugin.adb.base.service;

import ru.datamart.prostore.common.reader.QueryResult;
import ru.datamart.prostore.common.reader.SourceType;
import ru.datamart.prostore.query.execution.plugin.api.AbstractDtmDataSourcePlugin;
import ru.datamart.prostore.query.execution.plugin.api.service.*;
import ru.datamart.prostore.query.execution.plugin.api.service.check.CheckDataService;
import ru.datamart.prostore.query.execution.plugin.api.service.check.CheckTableService;
import ru.datamart.prostore.query.execution.plugin.api.service.check.CheckVersionService;
import ru.datamart.prostore.query.execution.plugin.api.service.ddl.TruncateHistoryService;
import ru.datamart.prostore.query.execution.plugin.api.service.mppr.MpprService;
import ru.datamart.prostore.query.execution.plugin.api.service.mppw.MppwService;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;

public class AdbDtmDataSourcePlugin extends AbstractDtmDataSourcePlugin {

    public static final String ADB_QUERY_TEMPLATE_CACHE = "adbQueryTemplateCache";
    public static final String ADB_DATAMART_CACHE = "adb_datamart";

    public AdbDtmDataSourcePlugin(
            DdlService<Void> ddlService,
            LlrService<QueryResult> adbLlrService,
            InsertValuesService insertValuesService,
            InsertSelectService insertSelectService,
            UpsertValuesService upsertValuesService,
            DeleteService deleteService,
            MpprService adbMpprService,
            MppwService adbMppwService,
            StatusService statusService,
            RollbackService<Void> rollbackService,
            CheckTableService checkTableService,
            CheckDataService checkDataService,
            TruncateHistoryService truncateService,
            CheckVersionService checkVersionService,
            PluginInitializationService initializationService,
            SynchronizeService synchronizeService) {
        super(ddlService,
                adbLlrService,
                insertValuesService,
                insertSelectService,
                upsertValuesService,
                deleteService,
                adbMpprService,
                adbMppwService,
                statusService,
                rollbackService,
                checkTableService,
                checkDataService,
                checkVersionService,
                truncateService,
                initializationService,
                synchronizeService);
    }

    @Override
    public SourceType getSourceType() {
        return SourceType.ADB;
    }

    @Override
    public Set<String> getActiveCaches() {
        return new HashSet<>(Arrays.asList(ADB_DATAMART_CACHE, ADB_QUERY_TEMPLATE_CACHE));
    }
}
