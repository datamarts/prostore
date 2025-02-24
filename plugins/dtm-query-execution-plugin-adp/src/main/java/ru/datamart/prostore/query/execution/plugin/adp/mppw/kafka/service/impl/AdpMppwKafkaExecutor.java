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
package ru.datamart.prostore.query.execution.plugin.adp.mppw.kafka.service.impl;

import io.vertx.core.Future;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Service;
import ru.datamart.prostore.common.model.ddl.ExternalTableFormat;
import ru.datamart.prostore.common.model.ddl.ExternalTableLocationType;
import ru.datamart.prostore.query.execution.plugin.adp.mppw.AdpMppwExecutor;
import ru.datamart.prostore.query.execution.plugin.adp.mppw.kafka.service.AdpMppwRequestExecutor;
import ru.datamart.prostore.query.execution.plugin.api.exception.MppwDatasourceException;
import ru.datamart.prostore.query.execution.plugin.api.mppw.MppwRequest;
import ru.datamart.prostore.query.execution.plugin.api.mppw.kafka.MppwKafkaRequest;

@Service
public class AdpMppwKafkaExecutor implements AdpMppwExecutor {

    private final AdpMppwRequestExecutor adpStartMppwRequestExecutor;
    private final AdpMppwRequestExecutor adpStopMppwRequestExecutor;

    public AdpMppwKafkaExecutor(@Qualifier("adpStartMppwRequestExecutor") AdpMppwRequestExecutor adpStartMppwRequestExecutor,
                                @Qualifier("adpStopMppwRequestExecutor") AdpMppwRequestExecutor adpStopMppwRequestExecutor) {
        this.adpStartMppwRequestExecutor = adpStartMppwRequestExecutor;
        this.adpStopMppwRequestExecutor = adpStopMppwRequestExecutor;
    }

    @Override
    public Future<String> execute(MppwRequest request) {
        return Future.future(promise -> {
            if (request.getUploadMetadata().getFormat() != ExternalTableFormat.AVRO) {
                promise.fail(new MppwDatasourceException(String.format("Format %s not implemented",
                        request.getUploadMetadata().getFormat())));
                return;
            }

            if (request.isLoadStart()) {
                adpStartMppwRequestExecutor.execute((MppwKafkaRequest) request)
                        .onComplete(promise);
            } else {
                adpStopMppwRequestExecutor.execute((MppwKafkaRequest) request)
                        .onComplete(promise);
            }
        });
    }

    @Override
    public ExternalTableLocationType getType() {
        return ExternalTableLocationType.KAFKA;
    }
}
