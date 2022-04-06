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
package ru.datamart.prostore.query.execution.plugin.adg.dml.service;

import io.vertx.core.Future;
import org.springframework.stereotype.Service;
import ru.datamart.prostore.common.model.ddl.EntityType;
import ru.datamart.prostore.query.execution.plugin.adg.dml.service.delete.AdgLogicalDeleteService;
import ru.datamart.prostore.query.execution.plugin.adg.dml.service.delete.AdgStandaloneDeleteService;
import ru.datamart.prostore.query.execution.plugin.api.request.DeleteRequest;
import ru.datamart.prostore.query.execution.plugin.api.service.DeleteService;

@Service("adgDeleteService")
public class AdgDeleteService implements DeleteService {
    private final AdgLogicalDeleteService logicalDeleteService;
    private final AdgStandaloneDeleteService standaloneDeleteService;

    public AdgDeleteService(AdgLogicalDeleteService logicalDeleteService, AdgStandaloneDeleteService standaloneDeleteService) {
        this.logicalDeleteService = logicalDeleteService;
        this.standaloneDeleteService = standaloneDeleteService;
    }

    @Override
    public Future<Void> execute(DeleteRequest request) {
        if (request.getEntity().getEntityType() == EntityType.WRITEABLE_EXTERNAL_TABLE) {
            return standaloneDeleteService.execute(request);
        }

        return logicalDeleteService.execute(request);
    }
}
