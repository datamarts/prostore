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
package ru.datamart.prostore.query.execution.plugin.api.service.shared.adqm;

import ru.datamart.prostore.common.model.ddl.Entity;
import ru.datamart.prostore.query.execution.plugin.api.shared.adqm.AdqmSharedProperties;
import io.vertx.core.Future;

public interface AdqmSharedService {

    Future<Void> recreateBufferTables(String env, String datamart, Entity entity);

    Future<Void> dropBufferTables(String env, String datamart, Entity entity);

    Future<Void> flushTable(String table);

    Future<Void> flushActualTable(String env, String datamart, Entity entity);

    Future<Void> closeVersionSqlByTableActual(String env, String datamart, Entity entity, long sysCn);

    Future<Void> closeVersionSqlByTableBuffer(String env, String datamart, Entity entity, long sysCn);

    AdqmSharedProperties getSharedProperties();
}
