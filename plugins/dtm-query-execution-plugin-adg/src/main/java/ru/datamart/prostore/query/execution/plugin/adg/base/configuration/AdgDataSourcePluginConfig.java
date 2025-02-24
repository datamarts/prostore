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
package ru.datamart.prostore.query.execution.plugin.adg.base.configuration;

import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import ru.datamart.prostore.common.reader.QueryResult;
import ru.datamart.prostore.query.execution.plugin.adg.base.service.AdgDataSourcePlugin;
import ru.datamart.prostore.query.execution.plugin.api.service.*;
import ru.datamart.prostore.query.execution.plugin.api.service.check.CheckDataService;
import ru.datamart.prostore.query.execution.plugin.api.service.check.CheckTableService;
import ru.datamart.prostore.query.execution.plugin.api.service.check.CheckVersionService;
import ru.datamart.prostore.query.execution.plugin.api.service.ddl.TruncateHistoryService;
import ru.datamart.prostore.query.execution.plugin.api.service.mppr.MpprService;
import ru.datamart.prostore.query.execution.plugin.api.service.mppw.MppwService;

@Configuration
public class AdgDataSourcePluginConfig {

    @Bean("adgDtmDataSourcePlugin")
    public AdgDataSourcePlugin adgDataSourcePlugin(
            @Qualifier("adgDdlService") DdlService<Void> ddlService,
            @Qualifier("adgEddlService") EddlService eddlService,
            @Qualifier("adgLlrService") LlrService<QueryResult> llrService,
            @Qualifier("adgInsertValuesService") InsertValuesService insertValuesService,
            @Qualifier("adgInsertSelectService") InsertSelectService insertSelectService,
            @Qualifier("adgUpsertValuesService") UpsertValuesService upsertValuesService,
            @Qualifier("adgDeleteService") DeleteService deleteService,
            @Qualifier("adgMpprService") MpprService mpprService,
            @Qualifier("adgMppwService") MppwService mppwService,
            @Qualifier("adgStatusService") StatusService statusService,
            @Qualifier("adgRollbackService") RollbackService<Void> rollbackService,
            @Qualifier("adgCheckTableService") CheckTableService checkTableService,
            @Qualifier("adgCheckDataService") CheckDataService checkDataService,
            @Qualifier("adgTruncateHistoryService") TruncateHistoryService truncateHistoryService,
            @Qualifier("adgCheckVersionService") CheckVersionService checkVersionService,
            @Qualifier("adgInitializationService") PluginInitializationService initializationService,
            @Qualifier("adgSynchronizeService") SynchronizeService synchronizeService) {
        return new AdgDataSourcePlugin(
                ddlService,
                eddlService,
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
