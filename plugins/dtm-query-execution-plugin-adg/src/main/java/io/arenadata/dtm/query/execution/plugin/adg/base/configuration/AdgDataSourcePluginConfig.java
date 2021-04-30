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
package io.arenadata.dtm.query.execution.plugin.adg.base.configuration;

import io.arenadata.dtm.common.reader.QueryResult;
import io.arenadata.dtm.query.execution.plugin.adg.base.service.AdgDataSourcePlugin;
import io.arenadata.dtm.query.execution.plugin.api.service.*;
import io.arenadata.dtm.query.execution.plugin.api.service.check.CheckDataService;
import io.arenadata.dtm.query.execution.plugin.api.service.check.CheckTableService;
import io.arenadata.dtm.query.execution.plugin.api.service.check.CheckVersionService;
import io.arenadata.dtm.query.execution.plugin.api.service.ddl.TruncateHistoryService;
import io.arenadata.dtm.query.execution.plugin.api.service.mppr.MpprService;
import io.arenadata.dtm.query.execution.plugin.api.service.mppw.MppwService;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class AdgDataSourcePluginConfig {

    @Bean("adgDtmDataSourcePlugin")
    public AdgDataSourcePlugin adbDataSourcePlugin(
            @Qualifier("adgDdlService") DdlService<Void> ddlService,
            @Qualifier("adgLlrService") LlrService<QueryResult> llrService,
            @Qualifier("adgMpprService") MpprService mpprService,
            @Qualifier("adgMppwService") MppwService mppwService,
            @Qualifier("adgStatusService") StatusService statusService,
            @Qualifier("adgRollbackService") RollbackService<Void> rollbackService,
            @Qualifier("adgCheckTableService") CheckTableService checkTableService,
            @Qualifier("adgCheckDataService") CheckDataService checkDataService,
            @Qualifier("adgTruncateHistoryService") TruncateHistoryService truncateHistoryService,
            @Qualifier("adgCheckVersionService") CheckVersionService checkVersionService,
            @Qualifier("adgInitializationService") PluginInitializationService initializationService) {
        return new AdgDataSourcePlugin(
                ddlService,
                llrService,
                mpprService,
                mppwService,
                statusService,
                rollbackService,
                checkTableService,
                checkDataService,
                truncateHistoryService,
                checkVersionService,
                initializationService);
    }
}
