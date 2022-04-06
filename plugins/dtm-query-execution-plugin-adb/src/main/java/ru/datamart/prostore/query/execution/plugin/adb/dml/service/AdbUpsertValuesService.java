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
package ru.datamart.prostore.query.execution.plugin.adb.dml.service;

import io.vertx.core.Future;
import org.springframework.stereotype.Service;
import ru.datamart.prostore.common.model.ddl.EntityType;
import ru.datamart.prostore.query.execution.plugin.adb.dml.service.upsert.values.AdbLogicalUpsertValuesService;
import ru.datamart.prostore.query.execution.plugin.adb.dml.service.upsert.values.AdbStandaloneUpsertValuesService;
import ru.datamart.prostore.query.execution.plugin.api.request.UpsertValuesRequest;
import ru.datamart.prostore.query.execution.plugin.api.service.UpsertValuesService;

@Service("adbUpsertValuesService")
public class AdbUpsertValuesService implements UpsertValuesService {

    private final AdbLogicalUpsertValuesService logicalService;
    private final AdbStandaloneUpsertValuesService standaloneService;

    public AdbUpsertValuesService(AdbLogicalUpsertValuesService logicalService, AdbStandaloneUpsertValuesService standaloneService) {
        this.logicalService = logicalService;
        this.standaloneService = standaloneService;
    }

    @Override
    public Future<Void> execute(UpsertValuesRequest request) {
        if (request.getEntity().getEntityType() == EntityType.TABLE) {
            return logicalService.execute(request);
        }

        return standaloneService.execute(request);
    }
}
