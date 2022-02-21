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
package ru.datamart.prostore.query.execution.plugin.api.service.shared.adqm;

import ru.datamart.prostore.common.exception.DtmException;
import ru.datamart.prostore.common.model.ddl.Entity;
import ru.datamart.prostore.query.execution.plugin.api.shared.adqm.AdqmSharedProperties;
import io.vertx.core.Future;
import org.springframework.stereotype.Service;

@Service
public class AdqmSharedServiceStub implements AdqmSharedService {

    public static final String STUBBED_EXCEPTION = "Disabled plugin [ADQM]";

    @Override
    public Future<Void> recreateBufferTables(String env, String datamart, Entity entity) {
        throw new DtmException(STUBBED_EXCEPTION);
    }

    @Override
    public Future<Void> dropBufferTables(String env, String datamart, Entity entity) {
        throw new DtmException(STUBBED_EXCEPTION);
    }

    @Override
    public Future<Void> flushActualTable(String env, String datamart, Entity entity) {
        throw new DtmException(STUBBED_EXCEPTION);
    }

    @Override
    public Future<Void> closeVersionSqlByTableActual(String env, String datamart, Entity entity, long sysCn) {
        throw new DtmException(STUBBED_EXCEPTION);
    }

    @Override
    public Future<Void> closeVersionSqlByTableBuffer(String env, String datamart, Entity entity, long sysCn) {
        throw new DtmException(STUBBED_EXCEPTION);
    }

    @Override
    public AdqmSharedProperties getSharedProperties() {
        throw new DtmException(STUBBED_EXCEPTION);
    }
}
