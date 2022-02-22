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
package ru.datamart.prostore.query.execution.plugin.adp.init.service;

import ru.datamart.prostore.query.execution.plugin.adp.db.service.DatabaseExecutor;
import ru.datamart.prostore.query.execution.plugin.api.service.PluginInitializationService;
import io.vertx.core.Future;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Service;

import static ru.datamart.prostore.query.execution.plugin.adp.base.factory.hash.AdpFunctionFactory.createInt32HashFunction;

@Service("adpInitializationService")
public class AdpInitializationService implements PluginInitializationService {

    private final DatabaseExecutor databaseExecutor;

    public AdpInitializationService(@Qualifier("adpQueryExecutor") DatabaseExecutor databaseExecutor) {
        this.databaseExecutor = databaseExecutor;
    }

    @Override
    public Future<Void> execute() {
        return databaseExecutor.executeUpdate(createInt32HashFunction());
    }
}
