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
package ru.datamart.prostore.query.execution.plugin.adb.synchronize.executors.impl;

import ru.datamart.prostore.common.exception.DtmException;
import ru.datamart.prostore.common.model.ddl.Entity;
import ru.datamart.prostore.common.reader.SourceType;
import ru.datamart.prostore.query.execution.plugin.adb.base.factory.adqm.AdqmConnectorSqlFactory;
import ru.datamart.prostore.query.execution.plugin.adb.query.service.DatabaseExecutor;
import ru.datamart.prostore.query.execution.plugin.adb.synchronize.executors.SynchronizeDestinationExecutor;
import ru.datamart.prostore.query.execution.plugin.adb.synchronize.service.PrepareQueriesOfChangesService;
import ru.datamart.prostore.query.execution.plugin.adb.synchronize.service.PrepareRequestOfChangesRequest;
import ru.datamart.prostore.query.execution.plugin.adb.synchronize.service.PrepareRequestOfChangesResult;
import ru.datamart.prostore.query.execution.plugin.api.service.shared.adqm.AdqmSharedService;
import ru.datamart.prostore.query.execution.plugin.api.synchronize.SynchronizeRequest;
import io.vertx.core.CompositeFuture;
import io.vertx.core.Future;
import lombok.extern.slf4j.Slf4j;
import lombok.val;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Component;

@Component
@Slf4j
public class AdqmSynchronizeDestinationExecutor implements SynchronizeDestinationExecutor {
    private final PrepareQueriesOfChangesService prepareQueriesOfChangesService;
    private final DatabaseExecutor databaseExecutor;
    private final AdqmConnectorSqlFactory connectorSqlFactory;
    private final AdqmSharedService adqmSharedService;

    public AdqmSynchronizeDestinationExecutor(@Qualifier("adqmPrepareQueriesOfChangesService") PrepareQueriesOfChangesService prepareQueriesOfChangesService,
                                              DatabaseExecutor databaseExecutor,
                                              AdqmConnectorSqlFactory connectorSqlFactory,
                                              AdqmSharedService adqmSharedService) {
        this.prepareQueriesOfChangesService = prepareQueriesOfChangesService;
        this.databaseExecutor = databaseExecutor;
        this.connectorSqlFactory = connectorSqlFactory;
        this.adqmSharedService = adqmSharedService;
    }

    @Override
    public Future<Long> execute(SynchronizeRequest request) {
        return Future.future(promise -> {
            log.info("Started [ADB->ADQM][{}] synchronization, deltaNum: {}", request.getRequestId(), request.getDeltaToBe());
            if (request.getDatamarts().size() > 1) {
                promise.fail(new DtmException(String.format("Can't synchronize [ADB->ADQM][%s] with multiple datamarts: %s",
                        request.getEntity().getName(), request.getDatamarts())));
                return;
            }


            adqmSharedService.recreateBufferTables(request.getEnvName(), request.getDatamartMnemonic(), request.getEntity())
                    .compose(v -> prepareQueriesOfChangesService.prepare(new PrepareRequestOfChangesRequest(request.getDatamarts(), request.getEnvName(),
                            request.getDeltaToBe(), request.getBeforeDeltaCnTo(), request.getViewQuery(), request.getEntity())))
                    .compose(requestOfChanges -> synchronize(requestOfChanges, request))
                    .onComplete(result -> CompositeFuture.join(executeDropExternalTable(request.getEntity()),
                                    adqmSharedService.dropBufferTables(request.getEnvName(), request.getDatamartMnemonic(), request.getEntity()))
                            .onComplete(dropResult -> {
                                if (dropResult.failed()) {
                                    log.error("Could not drop external table/buffer [{}]", request.getEntity().getNameWithSchema(), dropResult.cause());
                                }

                                if (result.succeeded()) {
                                    promise.complete(request.getDeltaToBe().getNum());
                                } else {
                                    promise.fail(result.cause());
                                }
                            }));
        });
    }

    private Future<Void> synchronize(PrepareRequestOfChangesResult requestOfChanges, SynchronizeRequest request) {
        return executeDropExternalTable(request.getEntity())
                .compose(r -> executeCreateExternalTable(request.getEnvName(), request.getDatamartMnemonic(), request.getEntity()))
                .compose(r -> insertChanges(requestOfChanges, request))
                .compose(r -> adqmSharedService.flushActualTable(request.getEnvName(), request.getDatamartMnemonic(), request.getEntity()))
                .compose(r -> adqmSharedService.closeVersionSqlByTableBuffer(request.getEnvName(), request.getDatamartMnemonic(), request.getEntity(), request.getDeltaToBe().getCnTo()))
                .compose(r -> adqmSharedService.closeVersionSqlByTableActual(request.getEnvName(), request.getDatamartMnemonic(), request.getEntity(), request.getDeltaToBe().getCnTo()))
                .compose(r -> adqmSharedService.flushActualTable(request.getEnvName(), request.getDatamartMnemonic(), request.getEntity()));
    }


    private Future<Void> insertChanges(PrepareRequestOfChangesResult requestOfChanges, SynchronizeRequest request) {
        return executeInsertIntoExternalTable(request.getDatamartMnemonic(), request.getEntity(), requestOfChanges.getDeletedRecordsQuery())
                .compose(ar -> executeInsertIntoExternalTable(request.getDatamartMnemonic(), request.getEntity(), requestOfChanges.getNewRecordsQuery(), request.getDeltaToBe().getCnTo()));
    }

    private Future<Void> executeDropExternalTable(Entity entity) {
        return Future.future(p -> {
            val extTableName = connectorSqlFactory.extTableName(entity);
            val pkExtTableName = connectorSqlFactory.extTablePkOnlyName(entity);
            val dropExtTable = connectorSqlFactory.dropExternalTable(extTableName);
            val dropPkExtTable = connectorSqlFactory.dropExternalTable(pkExtTableName);
            databaseExecutor.executeUpdate(dropExtTable)
                    .compose(v -> databaseExecutor.executeUpdate(dropPkExtTable))
                    .onComplete(p);
        });
    }

    private Future<Void> executeCreateExternalTable(String env, String datamart, Entity entity) {
        return Future.future(p -> {
            val createExtTable = connectorSqlFactory.createExternalTable(env, datamart, entity);
            val createPkExtTable = connectorSqlFactory.createExternalPkOnlyTable(env, datamart, entity);
            databaseExecutor.executeUpdate(createExtTable)
                    .compose(v -> databaseExecutor.executeUpdate(createPkExtTable))
                    .onComplete(p);
        });
    }

    private Future<Void> executeInsertIntoExternalTable(String datamart, Entity entity, String query, long sysCn) {
        return Future.future(event -> {
            val insertIntoSql = connectorSqlFactory.insertIntoExternalTable(datamart, entity, query, sysCn);
            databaseExecutor.executeUpdate(insertIntoSql).onComplete(event);
        });
    }

    private Future<Void> executeInsertIntoExternalTable(String datamart, Entity entity, String query) {
        return Future.future(event -> {
            val insertIntoSql = connectorSqlFactory.insertIntoExternalTable(datamart, entity, query);
            databaseExecutor.executeUpdate(insertIntoSql).onComplete(event);
        });
    }

    @Override
    public SourceType getDestination() {
        return SourceType.ADQM;
    }
}
