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
package ru.datamart.prostore.query.execution.plugin.adqm.base.configuration;

import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import ru.datamart.prostore.common.reader.QueryResult;
import ru.datamart.prostore.query.execution.plugin.adqm.base.service.AdqmDtmDataSourcePlugin;
import ru.datamart.prostore.query.execution.plugin.api.service.*;
import ru.datamart.prostore.query.execution.plugin.api.service.check.CheckDataService;
import ru.datamart.prostore.query.execution.plugin.api.service.check.CheckTableService;
import ru.datamart.prostore.query.execution.plugin.api.service.check.CheckVersionService;
import ru.datamart.prostore.query.execution.plugin.api.service.ddl.TruncateHistoryService;
import ru.datamart.prostore.query.execution.plugin.api.service.mppr.MpprService;
import ru.datamart.prostore.query.execution.plugin.api.service.mppw.MppwService;

@Configuration
public class AdqmDataSourcePluginConfig {

    @Bean("adqmDtmDataSourcePlugin")
    public AdqmDtmDataSourcePlugin adqmDataSourcePlugin(
            @Qualifier("adqmDdlService") DdlService<Void> ddlService,
            @Qualifier("adqmLlrService") LlrService<QueryResult> llrService,
            @Qualifier("adqmInsertValuesService") InsertValuesService insertValuesService,
            @Qualifier("adqmInsertSelectService") InsertSelectService insertSelectService,
            @Qualifier("adqmUpsertValuesService") UpsertValuesService upsertValuesService,
            @Qualifier("adqmDeleteService") DeleteService deleteService,
            @Qualifier("adqmMpprService") MpprService mpprService,
            @Qualifier("adqmMppwService") MppwService mppwService,
            @Qualifier("adqmStatusService") StatusService statusService,
            @Qualifier("adqmRollbackService") RollbackService<Void> rollbackService,
            @Qualifier("adqmCheckTableService") CheckTableService checkTableService,
            @Qualifier("adqmCheckDataService") CheckDataService checkDataService,
            @Qualifier("adqmTruncateHistoryService") TruncateHistoryService truncateHistoryService,
            @Qualifier("adqmCheckVersionService") CheckVersionService checkVersionService,
            @Qualifier("adqmInitializationService") PluginInitializationService initializationService,
            @Qualifier("adqmSynchronizeService") SynchronizeService synchronizeService) {
        return new AdqmDtmDataSourcePlugin(
                ddlService,
                llrService,
                insertValuesService,
                insertSelectService,
                upsertValuesService,
                deleteService,
                mpprService,
                mppwService,
                statusService,
                rollbackService,
                checkTableService,
                checkDataService,
                truncateHistoryService,
                checkVersionService,
                initializationService,
                synchronizeService);
    }
}
