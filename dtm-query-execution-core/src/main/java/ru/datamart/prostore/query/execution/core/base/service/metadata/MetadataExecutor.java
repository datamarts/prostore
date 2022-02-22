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
package ru.datamart.prostore.query.execution.core.base.service.metadata;

import ru.datamart.prostore.common.model.ddl.Entity;
import ru.datamart.prostore.common.reader.SourceType;
import ru.datamart.prostore.query.execution.core.ddl.dto.DdlRequestContext;
import ru.datamart.prostore.query.execution.core.plugin.service.DataSourcePluginService;
import ru.datamart.prostore.query.execution.plugin.api.request.DdlRequest;
import io.vertx.core.CompositeFuture;
import io.vertx.core.Future;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlNode;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.Set;

@Component
public class MetadataExecutor {

    private final DataSourcePluginService dataSourcePluginService;

    @Autowired
    public MetadataExecutor(DataSourcePluginService dataSourcePluginService) {
        this.dataSourcePluginService = dataSourcePluginService;
    }

    public Future<Void> execute(DdlRequestContext context) {
        return Future.future(promise -> {
            List<Future> futures = new ArrayList<>();
            Set<SourceType> destination = Optional.ofNullable(context.getEntity())
                    .map(Entity::getDestination)
                    .filter(set -> !set.isEmpty())
                    .orElse(dataSourcePluginService.getSourceTypes());
            destination.forEach(sourceType ->
                    futures.add(dataSourcePluginService.ddl(
                            sourceType,
                            context.getMetrics(),
                            DdlRequest.builder()
                                    .datamartMnemonic(context.getDatamartName())
                                    .entity(context.getEntity())
                                    .envName(context.getEnvName())
                                    .requestId(context.getRequest().getQueryRequest().getRequestId())
                                    .sqlKind(getKind(context.getSqlNode()))
                                    .build())
                    ));
            CompositeFuture.join(futures).onComplete(ar -> {
                if (ar.succeeded()) {
                    promise.complete();
                } else {
                    promise.fail(ar.cause());
                }
            });
        });
    }

    private SqlKind getKind(SqlNode node) {
        SqlKind kind = node.getKind();
        switch (kind) {
            case CREATE_MATERIALIZED_VIEW:
                return SqlKind.CREATE_TABLE;
            case DROP_MATERIALIZED_VIEW:
                return SqlKind.DROP_TABLE;
            default:
                return kind;
        }
    }
}
