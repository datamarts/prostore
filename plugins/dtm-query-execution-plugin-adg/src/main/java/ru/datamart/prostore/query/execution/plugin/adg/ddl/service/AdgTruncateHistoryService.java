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
package ru.datamart.prostore.query.execution.plugin.adg.ddl.service;

import ru.datamart.prostore.query.execution.plugin.adg.base.service.client.AdgCartridgeClient;
import ru.datamart.prostore.query.execution.plugin.adg.base.utils.AdgUtils;
import ru.datamart.prostore.query.execution.plugin.adg.ddl.factory.AdgTruncateHistoryConditionFactory;
import ru.datamart.prostore.query.execution.plugin.api.dto.TruncateHistoryRequest;
import ru.datamart.prostore.query.execution.plugin.api.service.ddl.TruncateHistoryService;
import io.vertx.core.CompositeFuture;
import io.vertx.core.Future;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.Arrays;

import static ru.datamart.prostore.query.execution.plugin.adg.base.utils.ColumnFields.ACTUAL_POSTFIX;
import static ru.datamart.prostore.query.execution.plugin.adg.base.utils.ColumnFields.HISTORY_POSTFIX;

@Service("adgTruncateHistoryService")
public class AdgTruncateHistoryService implements TruncateHistoryService {
    private final AdgCartridgeClient adgCartridgeClient;
    private final AdgTruncateHistoryConditionFactory conditionFactory;

    @Autowired
    public AdgTruncateHistoryService(AdgCartridgeClient adgCartridgeClient,
                                     AdgTruncateHistoryConditionFactory adgTruncateHistoryConditionFactory) {
        this.adgCartridgeClient = adgCartridgeClient;
        this.conditionFactory = adgTruncateHistoryConditionFactory;
    }

    @Override
    public Future<Void> truncateHistory(TruncateHistoryRequest request) {
        String conditions = conditionFactory.create(request);
        return request.getSysCn() != null
                ? deleteSpaceTuples(request, HISTORY_POSTFIX, conditions)
                : deleteSpaceTuplesWithoutSysCn(request, conditions);
    }

    private Future<Void> deleteSpaceTuples(TruncateHistoryRequest request, String postfix, String conditions) {
        String spaceName = AdgUtils.getSpaceName(request.getEnvName(), request.getEntity().getSchema(),
                request.getEntity().getName(), postfix);
        return adgCartridgeClient.deleteSpaceTuples(spaceName, conditions.isEmpty() ? null : conditions);
    }

    private Future<Void> deleteSpaceTuplesWithoutSysCn(TruncateHistoryRequest request,
                                                       String conditions) {
        return Future.future(promise -> CompositeFuture.join(Arrays.asList(
                        deleteSpaceTuples(request, ACTUAL_POSTFIX, conditions),
                        deleteSpaceTuples(request, HISTORY_POSTFIX, conditions)
                ))
                .onSuccess(result -> promise.complete())
                .onFailure(promise::fail));
    }
}
