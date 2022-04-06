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
package ru.datamart.prostore.query.execution.core.edml.mppw.service.impl;

import io.vertx.core.Future;
import lombok.extern.slf4j.Slf4j;
import lombok.val;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import ru.datamart.prostore.common.exception.DtmException;
import ru.datamart.prostore.common.model.ddl.Entity;
import ru.datamart.prostore.common.model.ddl.ExternalTableLocationType;
import ru.datamart.prostore.common.reader.QueryResult;
import ru.datamart.prostore.query.execution.core.base.service.metadata.LogicalSchemaProvider;
import ru.datamart.prostore.query.execution.core.edml.dto.EdmlAction;
import ru.datamart.prostore.query.execution.core.edml.dto.EdmlRequestContext;
import ru.datamart.prostore.query.execution.core.edml.mppw.service.EdmlUploadExecutor;
import ru.datamart.prostore.query.execution.core.edml.service.EdmlExecutor;
import ru.datamart.prostore.query.execution.core.plugin.service.DataSourcePluginService;
import ru.datamart.prostore.query.execution.model.metadata.Datamart;

import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static ru.datamart.prostore.query.execution.core.edml.dto.EdmlAction.UPLOAD_STANDALONE;

@Service
@Slf4j
public class UploadStandaloneExternalTableExecutor implements EdmlExecutor {

    private final Map<ExternalTableLocationType, EdmlUploadExecutor> executors;
    private final DataSourcePluginService pluginService;
    private final LogicalSchemaProvider logicalSchemaProvider;

    @Autowired
    public UploadStandaloneExternalTableExecutor(List<EdmlUploadExecutor> uploadExecutors,
                                                 DataSourcePluginService pluginService,
                                                 LogicalSchemaProvider logicalSchemaProvider) {
        this.executors = uploadExecutors.stream()
                .collect(Collectors.toMap(EdmlUploadExecutor::getUploadType, it -> it));
        this.pluginService = pluginService;
        this.logicalSchemaProvider = logicalSchemaProvider;
    }

    @Override
    public Future<QueryResult> execute(EdmlRequestContext context) {
        return isEntitySourceTypesExistsInConfiguration(context.getDestinationEntity())
                .compose(v -> initLogicalSchema(context))
                .compose(v -> executeInternal(context))
                .onComplete(f -> {
                    BreakMppwContext.removeTask(
                            context.getDestinationEntity().getSchema(),
                            context.getSysCn());
                });
    }

    private Future<Void> isEntitySourceTypesExistsInConfiguration(Entity destination) {
        val nonExistDestinationTypes = destination.getDestination().stream()
                .filter(type -> !pluginService.hasSourceType(type))
                .collect(Collectors.toSet());
        if (!nonExistDestinationTypes.isEmpty()) {
            final String failureMessage = String.format("Plugins: %s for the table [%s] datamart [%s] are not configured",
                    nonExistDestinationTypes,
                    destination.getName(),
                    destination.getSchema());
            return Future.failedFuture(new DtmException(failureMessage));
        }

        return Future.succeededFuture();
    }

    private Future<QueryResult> executeInternal(EdmlRequestContext context) {
        return Future.future(promise -> {
            if (ExternalTableLocationType.KAFKA == context.getSourceEntity().getExternalTableLocationType()) {
                executors.get(context.getSourceEntity().getExternalTableLocationType())
                        .execute(context)
                        .onComplete(promise);
            } else {
                promise.fail(new DtmException("Other download types are not yet implemented"));
            }
        });
    }

    private Future<Void> initLogicalSchema(EdmlRequestContext context) {
        return Future.future(promise -> {
            String datamartMnemonic = context.getRequest().getQueryRequest().getDatamartMnemonic();
            logicalSchemaProvider.getSchemaFromQuery(context.getSqlNode(), datamartMnemonic)
                    .onComplete(ar -> {
                        if (ar.succeeded()) {
                            final List<Datamart> logicalSchema = ar.result();
                            context.setLogicalSchema(logicalSchema);
                            promise.complete();
                        } else {
                            promise.fail(ar.cause());
                        }
                    });
        });
    }

    @Override
    public EdmlAction getAction() {
        return UPLOAD_STANDALONE;
    }
}
