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
package ru.datamart.prostore.query.execution.plugin.adqm.check.service;

import io.vertx.core.Future;
import lombok.val;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import ru.datamart.prostore.query.execution.plugin.adqm.check.factory.AdqmCheckDataQueryFactory;
import ru.datamart.prostore.query.execution.plugin.adqm.query.service.DatabaseExecutor;
import ru.datamart.prostore.query.execution.plugin.api.check.CheckDataByCountRequest;
import ru.datamart.prostore.query.execution.plugin.api.check.CheckDataByHashInt32Request;
import ru.datamart.prostore.query.execution.plugin.api.service.check.CheckDataService;

import java.util.List;
import java.util.Map;

@Service("adqmCheckDataService")
public class AdqmCheckDataService implements CheckDataService {
    private static final String COUNT = "count";
    private static final String SUM = "sum";

    private final DatabaseExecutor adqmQueryExecutor;
    private final AdqmCheckDataQueryFactory queryFactory;

    @Autowired
    public AdqmCheckDataService(DatabaseExecutor adqmQueryExecutor, AdqmCheckDataQueryFactory queryFactory) {
        this.adqmQueryExecutor = adqmQueryExecutor;
        this.queryFactory = queryFactory;
    }

    @Override
    public Future<Long> checkDataByCount(CheckDataByCountRequest request) {
        return adqmQueryExecutor.execute(queryFactory.createCheckDataByCountQuery(request, COUNT))
                .map(result -> Long.parseLong(result.get(0).get(COUNT).toString()));
    }

    @Override
    public Future<Long> checkDataByHashInt32(CheckDataByHashInt32Request request) {
        return adqmQueryExecutor.execute(queryFactory.createCheckDataByHashInt32Query(request, SUM))
                .map(this::processResult);
    }

    @Override
    public Future<Long> checkDataSnapshotByHashInt32(CheckDataByHashInt32Request request) {
        return adqmQueryExecutor.execute(queryFactory.createCheckDataSnapshotByHashInt32Query(request, SUM))
                .map(this::processResult);
    }

    private long processResult(List<Map<String, Object>> result) {
        if (result.isEmpty()) {
            return 0L;
        }

        val res = result.get(0).get(SUM);
        if (res == null) {
            return 0L;
        }

        return Long.parseLong(res.toString());
    }
}
