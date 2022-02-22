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
package ru.datamart.prostore.query.execution.core.check.service.impl;

import ru.datamart.prostore.common.model.ddl.Entity;
import ru.datamart.prostore.common.reader.SourceType;
import ru.datamart.prostore.query.execution.core.check.dto.CheckContext;
import ru.datamart.prostore.query.execution.core.plugin.service.DataSourcePluginService;
import ru.datamart.prostore.query.execution.plugin.api.check.CheckException;
import ru.datamart.prostore.query.execution.plugin.api.check.CheckTableRequest;
import io.vertx.core.CompositeFuture;
import io.vertx.core.Future;
import org.apache.calcite.util.Pair;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

@Service("coreCheckTableService")
public class CheckTableService {

    private final DataSourcePluginService dataSourcePluginService;

    @Autowired
    public CheckTableService(DataSourcePluginService dataSourcePluginService) {
        this.dataSourcePluginService = dataSourcePluginService;
    }

    public Future<String> checkEntity(Entity entity, CheckContext context) {
        return Future.future(promise -> CompositeFuture.join(entity.getDestination()
                .stream()
                .map(type -> checkEntityByType(entity, context, type))
                .collect(Collectors.toList()))
                .onSuccess(result -> {
                    List<Pair<SourceType, Optional<String>>> list = result.list();
                    if (list.stream().map(Pair::getValue).noneMatch(Optional::isPresent)) {
                        promise.complete(String.format("Table %s.%s (%s) is ok",
                            entity.getSchema(), entity.getName(),
                            list.stream()
                                .map(pair -> pair.getKey().name())
                                .sorted()
                                .collect(Collectors.joining(", "))));
                    } else {
                        String errors = list.stream()
                                .map(pair -> String.format("%s : %s", pair.getKey(), pair.getValue().orElse("ok")))
                                .collect(Collectors.joining("\n"));
                        promise.complete(String.format("Table '%s.%s' check failed!%n%s", entity.getSchema(),
                                entity.getName(), errors));
                    }
                })
                .onFailure(promise::fail));
    }

    private Future<Pair<SourceType, Optional<String>>> checkEntityByType(Entity entity, CheckContext context, SourceType type) {
        return Future.future(promise -> dataSourcePluginService
                .checkTable(type,
                        context.getMetrics(),
                        new CheckTableRequest(context.getRequest().getQueryRequest().getRequestId(),
                                context.getEnvName(),
                                context.getRequest().getQueryRequest().getDatamartMnemonic(),
                                entity))
                .onSuccess(result -> promise.complete(new Pair<>(type, Optional.empty())))
                .onFailure(fail -> {
                    if (fail instanceof CheckException) {
                        promise.complete(new Pair<>(type, Optional.of(fail.getMessage())));
                    } else {
                        promise.fail(fail);
                    }
                }));
    }

}
