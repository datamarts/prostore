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
package ru.datamart.prostore.query.execution.plugin.adb.init.service;

import ru.datamart.prostore.query.execution.plugin.adb.base.factory.hash.AdbHashFunctionFactory;
import ru.datamart.prostore.query.execution.plugin.adb.query.service.DatabaseExecutor;
import ru.datamart.prostore.query.execution.plugin.api.service.PluginInitializationService;
import io.vertx.core.Future;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

@Service("adbInitializationService")
public class AdbInitializationService implements PluginInitializationService {

    private final DatabaseExecutor databaseExecutor;
    private final AdbHashFunctionFactory hashFunctionFactory;

    @Autowired
    public AdbInitializationService(DatabaseExecutor databaseExecutor,
                                    AdbHashFunctionFactory hashFunctionFactory) {
        this.databaseExecutor = databaseExecutor;
        this.hashFunctionFactory = hashFunctionFactory;
    }

    @Override
    public Future<Void> execute() {
        return databaseExecutor.executeUpdate(hashFunctionFactory.createInt32HashFunction());
    }
}
