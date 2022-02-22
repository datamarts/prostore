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
package ru.datamart.prostore.query.execution.plugin.adp.check.service;

import ru.datamart.prostore.common.model.ddl.ColumnType;
import ru.datamart.prostore.common.version.VersionInfo;
import ru.datamart.prostore.query.execution.model.metadata.ColumnMetadata;
import ru.datamart.prostore.query.execution.plugin.adp.check.factory.AdpVersionQueriesFactory;
import ru.datamart.prostore.query.execution.plugin.adp.connector.service.AdpConnectorClient;
import ru.datamart.prostore.query.execution.plugin.adp.db.service.DatabaseExecutor;
import ru.datamart.prostore.query.execution.plugin.api.check.CheckVersionRequest;
import ru.datamart.prostore.query.execution.plugin.api.service.check.CheckVersionService;
import io.vertx.core.CompositeFuture;
import io.vertx.core.Future;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static ru.datamart.prostore.query.execution.plugin.adp.check.factory.AdpVersionQueriesFactory.*;

@Service("adpCheckVersionService")
public class AdpCheckVersionService implements CheckVersionService {

    private final DatabaseExecutor databaseExecutor;
    private final AdpConnectorClient connectorClient;
    private final List<ColumnMetadata> metadata;

    @Autowired
    public AdpCheckVersionService(DatabaseExecutor databaseExecutor,
                                  AdpConnectorClient connectorClient) {
        this.databaseExecutor = databaseExecutor;
        this.connectorClient = connectorClient;
        metadata = createColumnMetadata();
    }

    @Override
    public Future<List<VersionInfo>> checkVersion(CheckVersionRequest request) {
        return CompositeFuture.join(databaseExecutor.execute(createAdpVersionQuery(), metadata)
                        .map(this::createResult),
                getConnectorVersions())
                .map(result -> {
                    List<List<VersionInfo>> list = result.list();
                    return list.stream()
                            .flatMap(List::stream)
                            .collect(Collectors.toList());
                });
    }

    private Future<List<VersionInfo>> getConnectorVersions() {
        return CompositeFuture.join(connectorClient.getMpprVersion(), connectorClient.getMppwVersion())
                .map(result -> {
                    List<List<VersionInfo>> list = result.list();
                    return list.stream()
                            .flatMap(List::stream)
                            .collect(Collectors.toList());
                });
    }

    private List<ColumnMetadata> createColumnMetadata() {
        return Arrays.asList(ColumnMetadata.builder()
                        .name(AdpVersionQueriesFactory.COMPONENT_NAME_COLUMN)
                        .type(ColumnType.VARCHAR)
                        .build(),
                ColumnMetadata.builder()
                        .name(AdpVersionQueriesFactory.VERSION_COLUMN)
                        .type(ColumnType.VARCHAR)
                        .build()
        );
    }

    private List<VersionInfo> createResult(List<Map<String, Object>> rows) {
        return rows.stream()
                .map(row -> new VersionInfo(row.get(COMPONENT_NAME_COLUMN).toString(),
                        row.get(VERSION_COLUMN).toString()))
                .collect(Collectors.toList());
    }
}
