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
package ru.datamart.prostore.query.execution.core.delta.repository.executor;

import ru.datamart.prostore.query.execution.core.base.service.zookeeper.ZookeeperExecutor;
import ru.datamart.prostore.query.execution.core.delta.exception.DeltaException;
import io.vertx.core.Future;
import io.vertx.core.Promise;
import lombok.extern.slf4j.Slf4j;
import lombok.val;
import org.apache.zookeeper.data.Stat;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

@Slf4j
@Component
public class DeleteDeltaHotExecutor extends DeltaServiceDaoExecutorHelper implements DeltaDaoExecutor {

    public DeleteDeltaHotExecutor(ZookeeperExecutor executor,
                                  @Value("${core.env.name}") String envName) {
        super(executor, envName);
    }

    public Future<Void> execute(String datamart) {
        val deltaStat = new Stat();
        Promise<Void> resultPromise = Promise.promise();
        executor.getData(getDeltaPath(datamart), null, deltaStat)
            .map(this::deserializedDelta)
            .map(delta -> {
                delta.setHot(null);
                return serializedDelta(delta);
            })
            .compose(deltaData -> executor.setData(getDeltaPath(datamart), deltaData, deltaStat.getVersion()))
            .onSuccess(r -> {
                log.debug("Deletion delta hot by datamart[{}] completed successfully", datamart);
                resultPromise.complete();
            })
            .onFailure(error -> {
                val errMsg = String.format("Can't delete delta hot on datamart[%s]",
                    datamart);
                if (error instanceof DeltaException) {
                    resultPromise.fail(error);
                } else {
                    resultPromise.fail(new DeltaException(errMsg, error));
                }
            });
        return resultPromise.future();
    }

    @Override
    public Class<? extends DeltaDaoExecutor> getExecutorInterface() {
        return DeleteDeltaHotExecutor.class;
    }
}
