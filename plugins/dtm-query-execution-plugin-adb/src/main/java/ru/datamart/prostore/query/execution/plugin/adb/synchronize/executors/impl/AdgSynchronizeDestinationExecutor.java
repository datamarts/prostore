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

import io.vertx.core.Future;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Component;
import ru.datamart.prostore.common.exception.DtmException;
import ru.datamart.prostore.common.model.ddl.Entity;
import ru.datamart.prostore.common.reader.SourceType;
import ru.datamart.prostore.query.execution.plugin.adb.base.factory.adg.AdgConnectorSqlFactory;
import ru.datamart.prostore.query.execution.plugin.adb.query.service.DatabaseExecutor;
import ru.datamart.prostore.query.execution.plugin.adb.synchronize.executors.SynchronizeDestinationExecutor;
import ru.datamart.prostore.query.execution.plugin.adb.synchronize.service.PrepareQueriesOfChangesService;
import ru.datamart.prostore.query.execution.plugin.adb.synchronize.service.PrepareRequestOfChangesRequest;
import ru.datamart.prostore.query.execution.plugin.adb.synchronize.service.PrepareRequestOfChangesResult;
import ru.datamart.prostore.query.execution.plugin.api.service.shared.adg.AdgSharedService;
import ru.datamart.prostore.query.execution.plugin.api.shared.adg.AdgSharedPrepareStagingRequest;
import ru.datamart.prostore.query.execution.plugin.api.shared.adg.AdgSharedTransferDataRequest;
import ru.datamart.prostore.query.execution.plugin.api.synchronize.SynchronizeRequest;

import java.util.List;
import java.util.Map;

@Component
@Slf4j
public class AdgSynchronizeDestinationExecutor implements SynchronizeDestinationExecutor {
    private static final boolean ONLY_NOT_NULLABLE_KEYS = true;
    private static final boolean ALL_COLUMNS = false;
    private final PrepareQueriesOfChangesService prepareQueriesOfChangesService;
    private final DatabaseExecutor databaseExecutor;
    private final AdgConnectorSqlFactory connectorSqlFactory;
    private final AdgSharedService adgSharedService;

    public AdgSynchronizeDestinationExecutor(@Qualifier("adgPrepareQueriesOfChangesService") PrepareQueriesOfChangesService prepareQueriesOfChangesService,
                                             DatabaseExecutor databaseExecutor,
                                             AdgConnectorSqlFactory connectorSqlFactory,
                                             AdgSharedService adgSharedService) {
        this.prepareQueriesOfChangesService = prepareQueriesOfChangesService;
        this.databaseExecutor = databaseExecutor;
        this.connectorSqlFactory = connectorSqlFactory;
        this.adgSharedService = adgSharedService;
    }

    @Override
    public Future<Long> execute(SynchronizeRequest request) {
        return Future.future(promise -> {
            log.info("Started [ADB->ADG][{}] synchronization, deltaNum: {}", request.getRequestId(), request.getDeltaToBe());
            if (request.getDatamarts().size() > 1) {
                promise.fail(new DtmException(String.format("Can't synchronize [ADB->ADG][%s] with multiple datamarts: %s",
                        request.getEntity().getName(), request.getDatamarts())));
                return;
            }

            prepareQueriesOfChangesService.prepare(new PrepareRequestOfChangesRequest(request.getDatamarts(), request.getEnvName(),
                            request.getDeltaToBe(), request.getBeforeDeltaCnTo(), request.getViewQuery(), request.getEntity()))
                    .compose(requestOfChanges -> synchronize(requestOfChanges, request))
                    .onComplete(result -> executeDropExternalTable(request.getDatamartMnemonic(), request.getEntity())
                            .onComplete(dropResult -> {
                                if (dropResult.failed()) {
                                    log.error("Could not drop external table [{}]", request.getEntity().getNameWithSchema(), dropResult.cause());
                                }

                                if (result.succeeded()) {
                                    promise.complete(request.getDeltaToBe().getNum());
                                } else {
                                    promise.fail(result.cause());
                                }
                            }));
        });
    }

    private Future<Void> synchronize(PrepareRequestOfChangesResult requestOfChanges, SynchronizeRequest synchronizeRequest) {
        return executeDropExternalTable(synchronizeRequest.getDatamartMnemonic(), synchronizeRequest.getEntity())
                .compose(r -> executeCreateExternalTable(synchronizeRequest.getEnvName(), synchronizeRequest.getDatamartMnemonic(), synchronizeRequest.getEntity()))
                .compose(r -> truncateSpace(synchronizeRequest))
                .compose(r -> insertChanges(requestOfChanges, synchronizeRequest))
                .compose(r -> transferSpaceChanges(synchronizeRequest));
    }

    private Future<Void> truncateSpace(SynchronizeRequest synchronizeRequest) {
        return adgSharedService.prepareStaging(new AdgSharedPrepareStagingRequest(synchronizeRequest.getEnvName(), synchronizeRequest.getDatamartMnemonic(),
                synchronizeRequest.getEntity()));
    }

    private Future<List<Map<String, Object>>> insertChanges(PrepareRequestOfChangesResult requestOfChanges, SynchronizeRequest synchronizeRequest) {
        return executeInsertIntoExternalTable(synchronizeRequest.getDatamartMnemonic(), synchronizeRequest.getEntity(), requestOfChanges.getDeletedRecordsQuery(), ONLY_NOT_NULLABLE_KEYS)
                .compose(ar -> executeInsertIntoExternalTable(synchronizeRequest.getDatamartMnemonic(), synchronizeRequest.getEntity(), requestOfChanges.getNewRecordsQuery(), ALL_COLUMNS));
    }

    private Future<Void> transferSpaceChanges(SynchronizeRequest synchronizeRequest) {
        return adgSharedService.transferData(new AdgSharedTransferDataRequest(synchronizeRequest.getEnvName(), synchronizeRequest.getDatamartMnemonic(),
                synchronizeRequest.getEntity(), synchronizeRequest.getDeltaToBe().getCnTo()));
    }

    private Future<List<Map<String, Object>>> executeDropExternalTable(String datamart, Entity entity) {
        return Future.future(event -> {
            String dropSql = connectorSqlFactory.dropExternalTable(datamart, entity);
            databaseExecutor.execute(dropSql).onComplete(event);
        });
    }

    private Future<List<Map<String, Object>>> executeCreateExternalTable(String env, String datamart, Entity entity) {
        return Future.future(event -> {
            String createSql = connectorSqlFactory.createExternalTable(env, datamart, entity);
            databaseExecutor.execute(createSql).onComplete(event);
        });
    }

    private Future<List<Map<String, Object>>> executeInsertIntoExternalTable(String datamart, Entity entity, String query, boolean onlyNotNullableKeys) {
        return Future.future(event -> {
            String insertIntoSql = connectorSqlFactory.insertIntoExternalTable(datamart, entity, query, onlyNotNullableKeys);
            databaseExecutor.execute(insertIntoSql).onComplete(event);
        });
    }

    @Override
    public SourceType getDestination() {
        return SourceType.ADG;
    }
}
