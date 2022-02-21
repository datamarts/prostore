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
package ru.datamart.prostore.query.execution.plugin.adb.check.service;

import io.vertx.core.Future;
import ru.datamart.prostore.query.execution.plugin.api.check.CheckDataByCountRequest;
import ru.datamart.prostore.query.execution.plugin.api.check.CheckDataByHashInt32Request;
import ru.datamart.prostore.query.execution.plugin.api.exception.FeatureNotImplementedException;
import ru.datamart.prostore.query.execution.plugin.api.service.check.CheckDataService;

public class AdbCheckDataServiceWithHistory implements CheckDataService {

    @Override
    public Future<Long> checkDataByCount(CheckDataByCountRequest request) {
        return Future.failedFuture(new FeatureNotImplementedException("adb with history check data by count"));
    }

    @Override
    public Future<Long> checkDataByHashInt32(CheckDataByHashInt32Request request) {
        return Future.failedFuture(new FeatureNotImplementedException("adb with history check data"));
    }

    @Override
    public Future<Long> checkDataSnapshotByHashInt32(CheckDataByHashInt32Request request) {
        return Future.failedFuture(new FeatureNotImplementedException("adb with history check data snapshot"));
    }
}
