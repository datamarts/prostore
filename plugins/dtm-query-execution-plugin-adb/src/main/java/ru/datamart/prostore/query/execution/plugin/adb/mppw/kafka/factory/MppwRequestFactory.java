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
package ru.datamart.prostore.query.execution.plugin.adb.mppw.kafka.factory;

import ru.datamart.prostore.query.execution.plugin.adb.mppw.kafka.dto.AdbKafkaMppwTransferRequest;
import ru.datamart.prostore.query.execution.plugin.adb.mppw.kafka.dto.TransferDataRequest;

import java.util.List;
import java.util.stream.Collectors;

import static ru.datamart.prostore.query.execution.plugin.adb.base.factory.Constants.SYS_FROM_ATTR;

public abstract class MppwRequestFactory {

    public abstract AdbKafkaMppwTransferRequest create(TransferDataRequest request);

    protected List<String> getStagingColumnList(TransferDataRequest request) {
        return request.getColumnList().stream()
                .map(fieldName -> SYS_FROM_ATTR.equals(fieldName) ? String.valueOf(request.getHotDelta()) : fieldName)
                .collect(Collectors.toList());
    }
}
