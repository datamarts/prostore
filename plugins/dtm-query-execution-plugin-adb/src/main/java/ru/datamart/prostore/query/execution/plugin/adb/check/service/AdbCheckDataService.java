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
import lombok.val;
import ru.datamart.prostore.common.model.ddl.ColumnType;
import ru.datamart.prostore.query.execution.model.metadata.ColumnMetadata;
import ru.datamart.prostore.query.execution.plugin.adb.check.factory.impl.AdbCheckDataQueryFactory;
import ru.datamart.prostore.query.execution.plugin.adb.query.service.DatabaseExecutor;
import ru.datamart.prostore.query.execution.plugin.api.check.CheckDataByCountRequest;
import ru.datamart.prostore.query.execution.plugin.api.check.CheckDataByHashInt32Request;
import ru.datamart.prostore.query.execution.plugin.api.service.check.CheckDataService;

import java.util.Collections;
import java.util.List;
import java.util.Map;

public class AdbCheckDataService implements CheckDataService {

    private static final String COUNT_COLUMN_NAME = "cnt";
    private static final String HASH_SUM_COLUMN_NAME = "hash_sum";

    private final AdbCheckDataQueryFactory checkDataFactory;
    private final DatabaseExecutor queryExecutor;

    public AdbCheckDataService(AdbCheckDataQueryFactory checkDataFactory,
                               DatabaseExecutor queryExecutor) {
        this.checkDataFactory = checkDataFactory;
        this.queryExecutor = queryExecutor;
    }

    @Override
    public Future<Long> checkDataByCount(CheckDataByCountRequest request) {
        val metadata = new ColumnMetadata(COUNT_COLUMN_NAME, ColumnType.BIGINT);
        return queryExecutor.execute(checkDataFactory.createCheckDataByCountQuery(request, COUNT_COLUMN_NAME),
                        Collections.singletonList(metadata))
                .map(result -> Long.valueOf(result.get(0).get(COUNT_COLUMN_NAME).toString()));
    }

    @Override
    public Future<Long> checkDataByHashInt32(CheckDataByHashInt32Request request) {
        val columnMetadata = new ColumnMetadata(HASH_SUM_COLUMN_NAME, ColumnType.BIGINT);
        return queryExecutor.execute(checkDataFactory.createCheckDataByHashInt32Query(request, HASH_SUM_COLUMN_NAME),
                        Collections.singletonList(columnMetadata))
                .map(this::processResult);
    }

    @Override
    public Future<Long> checkDataSnapshotByHashInt32(CheckDataByHashInt32Request request) {
        val columnMetadata = new ColumnMetadata(HASH_SUM_COLUMN_NAME, ColumnType.BIGINT);
        return queryExecutor.execute(checkDataFactory.createCheckDataSnapshotByHashInt32Query(request, HASH_SUM_COLUMN_NAME),
                        Collections.singletonList(columnMetadata))
                .map(this::processResult);
    }

    private long processResult(List<Map<String, Object>> result) {
        if (result.isEmpty()) {
            return 0L;
        }

        val res = result.get(0).get(HASH_SUM_COLUMN_NAME);
        if (res == null) {
            return 0L;
        }

        return Long.parseLong(res.toString());
    }
}
