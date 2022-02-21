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
package ru.datamart.prostore.query.execution.plugin.adb.check.service;

import ru.datamart.prostore.common.model.ddl.ColumnType;
import ru.datamart.prostore.common.version.VersionInfo;
import ru.datamart.prostore.query.execution.model.metadata.ColumnMetadata;
import ru.datamart.prostore.query.execution.plugin.adb.check.factory.impl.AdbVersionInfoFactory;
import ru.datamart.prostore.query.execution.plugin.adb.check.factory.impl.AdbVersionQueriesFactory;
import ru.datamart.prostore.query.execution.plugin.adb.query.service.DatabaseExecutor;
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

@Service("adbCheckVersionService")
public class AdbCheckVersionService implements CheckVersionService {

    private final DatabaseExecutor databaseExecutor;
    private final AdbVersionQueriesFactory versionQueriesFactory;
    private final AdbVersionInfoFactory versionInfoFactory;
    private final List<ColumnMetadata> metadata;

    @Autowired
    public AdbCheckVersionService(DatabaseExecutor databaseExecutor,
                                  AdbVersionQueriesFactory versionQueriesFactory,
                                  AdbVersionInfoFactory versionInfoFactory) {
        this.databaseExecutor = databaseExecutor;
        this.versionQueriesFactory = versionQueriesFactory;
        this.versionInfoFactory = versionInfoFactory;
        metadata = createColumnMetadata();
    }

    @Override
    public Future<List<VersionInfo>> checkVersion(CheckVersionRequest request) {
        return CompositeFuture.join(Arrays.asList(databaseExecutor.execute(versionQueriesFactory.createAdbVersionQuery(), metadata),
                databaseExecutor.execute(versionQueriesFactory.createFdwVersionQuery(), metadata),
                databaseExecutor.execute(versionQueriesFactory.createPxfVersionQuery(), metadata)))
                .map(result -> {
                    List<List<Map<String, Object>>> list = result.list();
                    List<Map<String, Object>> resultList = list.stream()
                            .flatMap(List::stream)
                            .collect(Collectors.toList());
                    return versionInfoFactory.create(resultList);
                });
    }

    private List<ColumnMetadata> createColumnMetadata() {
        return Arrays.asList(ColumnMetadata.builder()
                        .name(AdbVersionQueriesFactory.COMPONENT_NAME_COLUMN)
                        .type(ColumnType.VARCHAR)
                        .build(),
                ColumnMetadata.builder()
                        .name(AdbVersionQueriesFactory.VERSION_COLUMN)
                        .type(ColumnType.VARCHAR)
                        .build()
        );
    }
}
