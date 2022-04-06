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
package ru.datamart.prostore.query.execution.plugin.adb.eddl.service;

import io.vertx.core.Future;
import org.springframework.cache.annotation.CacheEvict;
import org.springframework.stereotype.Service;
import ru.datamart.prostore.query.execution.plugin.adb.base.service.AdbDtmDataSourcePlugin;
import ru.datamart.prostore.query.execution.plugin.api.request.EddlRequest;
import ru.datamart.prostore.query.execution.plugin.api.service.EddlService;

@Service("adbEddlService")
public class AdbEddlService implements EddlService {

    private final CreateStandaloneTableExecutor createExecutor;
    private final DropStandaloneTableExecutor dropExecutor;

    public AdbEddlService(CreateStandaloneTableExecutor createExecutor,
                          DropStandaloneTableExecutor dropExecutor) {
        this.createExecutor = createExecutor;
        this.dropExecutor = dropExecutor;
    }

    @Override
    @CacheEvict(value = AdbDtmDataSourcePlugin.ADB_DATAMART_CACHE, key = "#request.getDatamartMnemonic()")
    public Future<Void> execute(EddlRequest request) {
        if (request.isCreateRequest()) {
            return createExecutor.execute(request);
        }

        return dropExecutor.execute(request);
    }
}
