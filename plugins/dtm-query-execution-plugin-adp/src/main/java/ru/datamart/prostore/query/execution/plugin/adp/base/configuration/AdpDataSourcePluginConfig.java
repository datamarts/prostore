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
package ru.datamart.prostore.query.execution.plugin.adp.base.configuration;

import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import ru.datamart.prostore.common.reader.QueryResult;
import ru.datamart.prostore.query.execution.plugin.adp.base.service.AdpDtmDataSourcePlugin;
import ru.datamart.prostore.query.execution.plugin.api.service.*;
import ru.datamart.prostore.query.execution.plugin.api.service.check.CheckDataService;
import ru.datamart.prostore.query.execution.plugin.api.service.check.CheckTableService;
import ru.datamart.prostore.query.execution.plugin.api.service.check.CheckVersionService;
import ru.datamart.prostore.query.execution.plugin.api.service.ddl.TruncateHistoryService;
import ru.datamart.prostore.query.execution.plugin.api.service.mppr.MpprService;
import ru.datamart.prostore.query.execution.plugin.api.service.mppw.MppwService;

@Configuration
public class AdpDataSourcePluginConfig {

    @Bean("adpDtmDataSourcePlugin")
    public AdpDtmDataSourcePlugin adpDataSourcePlugin(
            @Qualifier("adpDdlService") DdlService<Void> ddlService,
            @Qualifier("adpLlrService") LlrService<QueryResult> llrService,
            @Qualifier("adpInsertValuesService") InsertValuesService insertValuesService,
            @Qualifier("adpInsertSelectService") InsertSelectService insertSelectService,
            @Qualifier("adpUpsertValuesService") UpsertValuesService upsertValuesService,
            @Qualifier("adpDeleteService") DeleteService deleteService,
            @Qualifier("adpMpprService") MpprService mpprService,
            @Qualifier("adpMppwService") MppwService mppwService,
            @Qualifier("adpStatusService") StatusService statusService,
            @Qualifier("adpRollbackService") RollbackService<Void> rollbackService,
            @Qualifier("adpCheckTableService") CheckTableService checkTableService,
            @Qualifier("adpCheckDataService") CheckDataService checkDataService,
            @Qualifier("adpTruncateHistoryService") TruncateHistoryService truncateHistoryService,
            @Qualifier("adpCheckVersionService") CheckVersionService checkVersionService,
            @Qualifier("adpInitializationService") PluginInitializationService initializationService,
            @Qualifier("adpSynchronizeService") SynchronizeService synchronizeService) {
        return new AdpDtmDataSourcePlugin(
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
