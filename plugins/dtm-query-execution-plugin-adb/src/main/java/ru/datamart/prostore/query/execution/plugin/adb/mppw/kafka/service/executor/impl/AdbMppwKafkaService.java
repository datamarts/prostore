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
package ru.datamart.prostore.query.execution.plugin.adb.mppw.kafka.service.executor.impl;

import ru.datamart.prostore.common.model.ddl.ExternalTableFormat;
import ru.datamart.prostore.common.model.ddl.ExternalTableLocationType;
import ru.datamart.prostore.query.execution.plugin.adb.mppw.kafka.service.executor.AdbMppwExecutor;
import ru.datamart.prostore.query.execution.plugin.adb.mppw.kafka.service.executor.AdbMppwRequestExecutor;
import ru.datamart.prostore.query.execution.plugin.api.exception.MppwDatasourceException;
import ru.datamart.prostore.query.execution.plugin.api.mppw.MppwRequest;
import ru.datamart.prostore.query.execution.plugin.api.mppw.kafka.MppwKafkaRequest;
import io.vertx.core.Future;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Component;

import java.util.EnumMap;
import java.util.Map;

@Slf4j
@Component("adbMppwKafkaService")
public class AdbMppwKafkaService implements AdbMppwExecutor {

    private static final Map<LoadType, AdbMppwRequestExecutor> mppwExecutors = new EnumMap<>(LoadType.class);

    @Autowired
    public AdbMppwKafkaService(@Qualifier("adbMppwStartRequestExecutor") AdbMppwRequestExecutor mppwStartExecutor,
                               @Qualifier("adbMppwStopRequestExecutor") AdbMppwRequestExecutor mppwStopExecutor) {
        mppwExecutors.put(LoadType.START, mppwStartExecutor);
        mppwExecutors.put(LoadType.STOP, mppwStopExecutor);
    }

    @Override
    public Future<String> execute(MppwRequest request) {
        if (request == null) {
            return Future.failedFuture(new MppwDatasourceException("MppwRequest should not be null"));
        }

        if (request.getUploadMetadata().getFormat() != ExternalTableFormat.AVRO) {
            return Future.failedFuture(new MppwDatasourceException(String.format("Format %s not implemented",
                    request.getUploadMetadata().getFormat())));
        }

        return Future.future(promise -> {
            final LoadType loadType = LoadType.valueOf(request.isLoadStart());
            mppwExecutors.get(loadType).execute((MppwKafkaRequest) request)
                    .onComplete(promise);
        });
    }

    @Override
    public ExternalTableLocationType getType() {
        return ExternalTableLocationType.KAFKA;
    }

    private enum LoadType {
        START(true),
        STOP(false);

        LoadType(boolean b) {
            this.load = b;
        }

        static LoadType valueOf(boolean b) {
            return b ? START : STOP;
        }

        private final boolean load;
    }
}
