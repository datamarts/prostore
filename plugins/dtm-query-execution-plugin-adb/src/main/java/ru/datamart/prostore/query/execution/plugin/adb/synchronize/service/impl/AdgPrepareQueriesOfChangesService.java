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
package ru.datamart.prostore.query.execution.plugin.adb.synchronize.service.impl;

import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Service;
import ru.datamart.prostore.query.calcite.core.service.QueryParserService;
import ru.datamart.prostore.query.execution.plugin.adb.base.service.castservice.ColumnsCastService;
import ru.datamart.prostore.query.execution.plugin.adb.synchronize.service.PrepareQueriesOfChangesServiceBase;
import ru.datamart.prostore.query.execution.plugin.api.service.enrichment.service.QueryEnrichmentService;

@Service("adgPrepareQueriesOfChangesService")
public class AdgPrepareQueriesOfChangesService extends PrepareQueriesOfChangesServiceBase {
    public AdgPrepareQueriesOfChangesService(@Qualifier("adbCalciteDMLQueryParserService") QueryParserService parserService,
                                             @Qualifier("adgColumnsCastService") ColumnsCastService columnsCastService,
                                             QueryEnrichmentService adbQueryEnrichmentService) {
        super(parserService, columnsCastService, adbQueryEnrichmentService, false);
    }
}
