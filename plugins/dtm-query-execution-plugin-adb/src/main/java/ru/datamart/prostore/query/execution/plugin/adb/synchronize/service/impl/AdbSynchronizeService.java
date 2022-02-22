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
package ru.datamart.prostore.query.execution.plugin.adb.synchronize.service.impl;

import ru.datamart.prostore.common.reader.SourceType;
import ru.datamart.prostore.query.execution.plugin.adb.synchronize.executors.SynchronizeDestinationExecutorDelegate;
import ru.datamart.prostore.query.execution.plugin.api.exception.SynchronizeDatasourceException;
import ru.datamart.prostore.query.execution.plugin.api.service.SynchronizeService;
import ru.datamart.prostore.query.execution.plugin.api.synchronize.SynchronizeRequest;
import io.vertx.core.CompositeFuture;
import io.vertx.core.Future;
import io.vertx.core.Vertx;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.ToString;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Service;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.function.Supplier;

@Slf4j
@Service("adbSynchronizeService")
public class AdbSynchronizeService implements SynchronizeService {
    private final SynchronizeDestinationExecutorDelegate synchronizeDestinationExecutorDelegate;
    private final Vertx vertx;

    public AdbSynchronizeService(SynchronizeDestinationExecutorDelegate synchronizeDestinationExecutorDelegate,
                                 @Qualifier("coreVertx") Vertx vertx) {
        this.synchronizeDestinationExecutorDelegate = synchronizeDestinationExecutorDelegate;
        this.vertx = vertx;
    }

    @Override
    public Future<Long> execute(SynchronizeRequest request) {
        return Future.future(promise -> {
            Set<SourceType> destinations = request.getEntity().getDestination();

            List<Future> futures = new ArrayList<>();
            for (SourceType destination : destinations) {
                futures.add(execute(destination, () -> synchronizeDestinationExecutorDelegate.execute(destination, request))
                        .onComplete(ar -> {
                            if (ar.succeeded()) {
                                log.info("Synchronization [ADB->{}}][{}] succeeded, matView: {}, deltaNum: {}",
                                        destination, request.getRequestId(), request.getEntity().getNameWithSchema(), request.getDeltaToBe());
                            } else {
                                log.error("Synchronization [ADB->{}}][{}] failed, matView: {}, deltaNum: {}", destination, request.getRequestId(),
                                        request.getEntity().getNameWithSchema(), request.getDeltaToBe(), ar.cause());
                            }
                        }));
            }

            CompositeFuture.join(futures)
                    .onSuccess(event -> {
                        List<SynchronizeResult> deltaNumResults = event.result().list();
                        long uniqueCount = deltaNumResults.stream()
                                .map(SynchronizeResult::getDeltaNum)
                                .distinct().count();
                        if (uniqueCount != 1) {
                            promise.fail(new SynchronizeDatasourceException(String.format("Materialized view %s failed to synchronize ADB, result deltaNum not equal: %s",
                                    request.getEntity().getName(), deltaNumResults)));
                            return;
                        }

                        promise.complete(deltaNumResults.get(0).deltaNum);
                    })
                    .onFailure(promise::fail);
        });
    }

    private Future<SynchronizeResult> execute(SourceType destination, Supplier<Future<Long>> executorCall) {
        return Future.future(promise -> vertx.executeBlocking(vertxPromise -> {
            executorCall.get()
                    .onSuccess(deltaNum -> vertxPromise.complete(new SynchronizeResult(destination, deltaNum)))
                    .onFailure(vertxPromise::fail);
        }, promise));
    }

    @Data
    @AllArgsConstructor
    @ToString
    private static class SynchronizeResult {
        private final SourceType destination;
        private final Long deltaNum;
    }
}
