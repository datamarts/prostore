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
package ru.datamart.prostore.query.execution.plugin.adg.eddl.service;

import io.vertx.core.Future;
import lombok.extern.slf4j.Slf4j;
import lombok.val;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;
import ru.datamart.prostore.query.execution.plugin.adg.base.model.cartridge.request.AdgDeleteTablesRequest;
import ru.datamart.prostore.query.execution.plugin.adg.base.service.client.AdgCartridgeClient;
import ru.datamart.prostore.query.execution.plugin.api.request.EddlRequest;
import ru.datamart.prostore.query.execution.plugin.api.service.EddlExecutor;

import java.util.Collections;

@Component
@Slf4j
public class DropStandaloneTableExecutor implements EddlExecutor {

    private final AdgCartridgeClient cartridgeClient;

    @Autowired
    public DropStandaloneTableExecutor(AdgCartridgeClient cartridgeClient) {
        this.cartridgeClient = cartridgeClient;
    }

    @Override
    public Future<Void> execute(EddlRequest request) {
        val deleteTablesRequest = new AdgDeleteTablesRequest(Collections.singletonList(request.getEntity().getExternalTableLocationPath()));
        return cartridgeClient.executeDeleteSpacesQueued(deleteTablesRequest);
    }
}
