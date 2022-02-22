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
package ru.datamart.prostore.query.execution.plugin.adg.check.service;

import io.vertx.core.CompositeFuture;
import io.vertx.core.Future;
import lombok.val;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import ru.datamart.prostore.query.execution.plugin.adg.base.service.client.AdgCartridgeClient;
import ru.datamart.prostore.query.execution.plugin.adg.base.utils.AdgUtils;
import ru.datamart.prostore.query.execution.plugin.adg.base.utils.ColumnFields;
import ru.datamart.prostore.query.execution.plugin.api.check.CheckDataByCountRequest;
import ru.datamart.prostore.query.execution.plugin.api.check.CheckDataByHashInt32Request;
import ru.datamart.prostore.query.execution.plugin.api.exception.FeatureNotImplementedException;
import ru.datamart.prostore.query.execution.plugin.api.service.check.CheckDataService;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;

@Service("adgCheckDataService")
public class AdgCheckDataService implements CheckDataService {
    private final AdgCartridgeClient adgCartridgeClient;

    @Autowired
    public AdgCheckDataService(AdgCartridgeClient adgCartridgeClient) {
        this.adgCartridgeClient = adgCartridgeClient;
    }

    @Override
    public Future<Long> checkDataByCount(CheckDataByCountRequest request) {
        val entity = request.getEntity();
        return getCheckSum(request.getEnvName(),
                entity.getSchema(),
                entity.getName(),
                request.getCnFrom(),
                request.getCnTo(),
                null,
                null);
    }

    @Override
    public Future<Long> checkDataByHashInt32(CheckDataByHashInt32Request request) {
        val entity = request.getEntity();
        return getCheckSum(request.getEnvName(),
                entity.getSchema(),
                entity.getName(),
                request.getCnFrom(),
                request.getCnTo(),
                request.getColumns(),
                request.getNormalization());
    }

    @Override
    public Future<Long> checkDataSnapshotByHashInt32(CheckDataByHashInt32Request request) {
        return Future.failedFuture(new FeatureNotImplementedException("adg check data snapshot"));
    }

    private Future<Long> getCheckSum(String env,
                                     String schema,
                                     String table,
                                     Long cnFrom,
                                     Long cnTo,
                                     Set<String> columns,
                                     Long normalization) {
        List<Future> checkSumFutures = new ArrayList<>();
        for (long cn = cnFrom; cn <= cnTo; cn++) {
            checkSumFutures.add(adgCartridgeClient.getCheckSumByInt32Hash(
                    AdgUtils.getSpaceName(env, schema, table, ColumnFields.ACTUAL_POSTFIX),
                    AdgUtils.getSpaceName(env, schema, table, ColumnFields.HISTORY_POSTFIX),
                    cn,
                    columns,
                    normalization
            ));
        }
        return CompositeFuture.join(checkSumFutures)
                .map(result -> result.list().stream()
                        .map(Long.class::cast)
                        .reduce(0L, Long::sum));
    }
}
