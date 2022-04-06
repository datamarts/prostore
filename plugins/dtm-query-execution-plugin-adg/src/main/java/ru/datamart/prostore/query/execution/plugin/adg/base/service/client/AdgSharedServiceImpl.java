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
package ru.datamart.prostore.query.execution.plugin.adg.base.service.client;

import io.vertx.core.Future;
import lombok.val;
import org.springframework.context.annotation.Primary;
import org.springframework.stereotype.Service;
import ru.datamart.prostore.query.execution.plugin.adg.base.configuration.properties.TarantoolDatabaseProperties;
import ru.datamart.prostore.query.execution.plugin.adg.base.configuration.properties.TarantoolDatabaseSyncProperties;
import ru.datamart.prostore.query.execution.plugin.adg.base.factory.AdgHelperTableNamesFactory;
import ru.datamart.prostore.query.execution.plugin.adg.base.model.cartridge.request.AdgTransferDataEtlRequest;
import ru.datamart.prostore.query.execution.plugin.adg.base.utils.AdgUtils;
import ru.datamart.prostore.query.execution.plugin.adg.base.utils.ColumnFields;
import ru.datamart.prostore.query.execution.plugin.api.service.shared.adg.AdgSharedService;
import ru.datamart.prostore.query.execution.plugin.api.shared.adg.AdgSharedPrepareStagingRequest;
import ru.datamart.prostore.query.execution.plugin.api.shared.adg.AdgSharedProperties;
import ru.datamart.prostore.query.execution.plugin.api.shared.adg.AdgSharedTransferDataRequest;

@Primary
@Service
public class AdgSharedServiceImpl implements AdgSharedService {
    private final AdgCartridgeClient cartridgeClient;
    private final AdgHelperTableNamesFactory adgHelperTableNamesFactory;
    private final TarantoolDatabaseProperties databaseProperties;
    private final TarantoolDatabaseSyncProperties syncProperties;

    public AdgSharedServiceImpl(AdgCartridgeClient cartridgeClient,
                                AdgHelperTableNamesFactory adgHelperTableNamesFactory,
                                TarantoolDatabaseProperties databaseProperties,
                                TarantoolDatabaseSyncProperties syncProperties) {
        this.cartridgeClient = cartridgeClient;
        this.adgHelperTableNamesFactory = adgHelperTableNamesFactory;
        this.databaseProperties = databaseProperties;
        this.syncProperties = syncProperties;
    }

    @Override
    public Future<Void> prepareStaging(AdgSharedPrepareStagingRequest request) {
        return Future.future(promise -> {
            val stagingSpaceName = AdgUtils.getSpaceName(request.getEnv(), request.getDatamart(), request.getEntity().getName(), ColumnFields.STAGING_POSTFIX);
            cartridgeClient.truncateSpace(stagingSpaceName)
                    .onComplete(promise);
        });
    }

    @Override
    public Future<Void> transferData(AdgSharedTransferDataRequest request) {
        return Future.future(promise -> {
            val tableNames = adgHelperTableNamesFactory.create(request.getEnv(), request.getDatamart(), request.getEntity().getName());
            cartridgeClient.transferDataToScdTable(new AdgTransferDataEtlRequest(tableNames, request.getCnTo()))
                    .onComplete(promise);
        });
    }

    @Override
    public AdgSharedProperties getSharedProperties() {
        val server = databaseProperties.getHost() + ":" + databaseProperties.getPort();
        return new AdgSharedProperties(server, databaseProperties.getUser(), databaseProperties.getPassword(),
                syncProperties.getTimeoutConnect(), syncProperties.getTimeoutRead(), syncProperties.getTimeoutRequest(),
                syncProperties.getBufferSize());
    }
}
