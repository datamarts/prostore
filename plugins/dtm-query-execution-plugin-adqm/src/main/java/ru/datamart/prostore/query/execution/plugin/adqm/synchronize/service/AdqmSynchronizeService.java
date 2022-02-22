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
package ru.datamart.prostore.query.execution.plugin.adqm.synchronize.service;

import ru.datamart.prostore.query.execution.plugin.api.exception.SynchronizeDatasourceException;
import ru.datamart.prostore.query.execution.plugin.api.service.SynchronizeService;
import ru.datamart.prostore.query.execution.plugin.api.synchronize.SynchronizeRequest;
import io.vertx.core.Future;
import org.springframework.stereotype.Service;

@Service("adqmSynchronizeService")
public class AdqmSynchronizeService implements SynchronizeService {
    @Override
    public Future<Long> execute(SynchronizeRequest request) {
        return Future.failedFuture(new SynchronizeDatasourceException("Synchronize[ADQM] is not implemented"));
    }
}
