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
package ru.datamart.prostore.query.execution.plugin.adb.mppw.kafka.factory;

import org.apache.avro.Schema;
import org.springframework.stereotype.Component;
import ru.datamart.prostore.query.execution.plugin.adb.mppw.kafka.dto.TransferDataRequest;
import ru.datamart.prostore.query.execution.plugin.api.mppw.MppwRequest;
import ru.datamart.prostore.query.execution.plugin.api.mppw.kafka.MppwKafkaRequest;

import java.util.List;
import java.util.stream.Collectors;

import static ru.datamart.prostore.query.execution.plugin.adb.base.factory.Constants.SYS_FROM_ATTR;
import static ru.datamart.prostore.query.execution.plugin.adb.base.factory.Constants.SYS_TO_ATTR;

@Component
public class MppwTransferRequestFactory {

    public TransferDataRequest create(MppwKafkaRequest request, List<String> keyColumns) {
        return TransferDataRequest.builder()
                .datamart(request.getDatamartMnemonic())
                .tableName(request.getDestinationEntity().getName())
                .hotDelta(request.getSysCn())
                .columnList(getColumnList(request))
                .keyColumnList(keyColumns)
                .build();
    }

    private List<String> getColumnList(MppwRequest request) {
        final List<String> columns = new Schema.Parser().parse(request.getUploadMetadata().getExternalSchema())
                .getFields().stream().map(Schema.Field::name).collect(Collectors.toList());
        columns.add(SYS_FROM_ATTR);
        columns.add(SYS_TO_ATTR);
        return columns;
    }
}
