/*
 * Copyright © 2021 ProStore
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

import ru.datamart.prostore.common.reader.SourceType;
import ru.datamart.prostore.query.execution.plugin.adb.synchronize.executors.SynchronizeDestinationExecutor;
import ru.datamart.prostore.query.execution.plugin.adb.synchronize.executors.SynchronizeDestinationExecutorDelegate;
import ru.datamart.prostore.query.execution.plugin.api.exception.SynchronizeDatasourceException;
import ru.datamart.prostore.query.execution.plugin.api.synchronize.SynchronizeRequest;
import io.vertx.core.Future;
import org.springframework.stereotype.Service;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static java.lang.String.format;

@Service
public class SynchronizeDestinationExecutorDelegateImpl implements SynchronizeDestinationExecutorDelegate {
    private final Map<SourceType, SynchronizeDestinationExecutor> executorMap;

    public SynchronizeDestinationExecutorDelegateImpl(List<SynchronizeDestinationExecutor> synchronizeDestinationExecutors) {
        executorMap = new HashMap<>(synchronizeDestinationExecutors.size());
        for (SynchronizeDestinationExecutor executor : synchronizeDestinationExecutors) {
            SynchronizeDestinationExecutor previous = executorMap.put(executor.getDestination(), executor);
            if (previous != null) {
                throw new IllegalArgumentException(format("SynchronizeDestinationExecutor of %s already exists, exist: %s, new: %s",
                        executor.getDestination(), previous.getClass().getSimpleName(), executor.getClass().getSimpleName()));
            }
        }
    }

    @Override
    public Future<Long> execute(SourceType sourceType, SynchronizeRequest synchronizeRequest) {
        if (!executorMap.containsKey(sourceType)) {
            return Future.failedFuture(new SynchronizeDatasourceException(format("Synchronize[ADB->%s] is not implemented", sourceType)));
        }

        return executorMap.get(sourceType).execute(synchronizeRequest);
    }
}
