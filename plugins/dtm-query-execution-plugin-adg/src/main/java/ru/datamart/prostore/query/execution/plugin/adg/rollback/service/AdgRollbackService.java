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
package ru.datamart.prostore.query.execution.plugin.adg.rollback.service;

import ru.datamart.prostore.query.execution.plugin.adg.rollback.factory.ReverseHistoryTransferRequestFactory;
import ru.datamart.prostore.query.execution.plugin.adg.base.service.client.AdgCartridgeClient;
import ru.datamart.prostore.query.execution.plugin.api.dto.RollbackRequest;
import ru.datamart.prostore.query.execution.plugin.api.service.RollbackService;
import io.vertx.core.Future;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;

@Slf4j
@RequiredArgsConstructor
@Service("adgRollbackService")
public class AdgRollbackService implements RollbackService<Void> {

    private final ReverseHistoryTransferRequestFactory requestFactory;
    private final AdgCartridgeClient cartridgeClient;

    @Override
    public Future<Void> execute(RollbackRequest request) {
        return cartridgeClient.reverseHistoryTransfer(requestFactory.create(request));
    }
}
