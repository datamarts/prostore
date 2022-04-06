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
import ru.datamart.prostore.query.execution.plugin.adb.dml.service.insert.values.AdbLogicalInsertValuesService;
import ru.datamart.prostore.query.execution.plugin.adb.dml.service.insert.values.AdbStandaloneInsertValuesService;
import ru.datamart.prostore.query.execution.plugin.api.request.InsertValuesRequest;
import ru.datamart.prostore.query.execution.plugin.api.service.InsertValuesService;

@Service("adbInsertValuesService")
public class AdbInsertValuesService implements InsertValuesService {
    private final AdbLogicalInsertValuesService logicalService;
    private final AdbStandaloneInsertValuesService standaloneService;

    public AdbInsertValuesService(AdbLogicalInsertValuesService logicalService,
                                  AdbStandaloneInsertValuesService standaloneService) {
        this.logicalService = logicalService;
        this.standaloneService = standaloneService;
    }

    @Override
    public Future<Void> execute(InsertValuesRequest request) {
        if (request.getEntity().getEntityType() == EntityType.WRITEABLE_EXTERNAL_TABLE) {
            return standaloneService.execute(request);
        }
        return logicalService.execute(request);
    }
}
