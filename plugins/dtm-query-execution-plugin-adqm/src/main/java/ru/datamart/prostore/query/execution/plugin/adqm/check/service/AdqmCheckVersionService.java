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
package ru.datamart.prostore.query.execution.plugin.adqm.check.service;

import ru.datamart.prostore.common.exception.DtmException;
import ru.datamart.prostore.common.model.ddl.ColumnType;
import ru.datamart.prostore.common.version.VersionInfo;
import ru.datamart.prostore.query.execution.model.metadata.ColumnMetadata;
import ru.datamart.prostore.query.execution.plugin.adqm.check.factory.AdqmVersionInfoFactory;
import ru.datamart.prostore.query.execution.plugin.adqm.check.factory.AdqmVersionQueriesFactory;
import ru.datamart.prostore.query.execution.plugin.adqm.mppr.configuration.properties.AdqmMpprProperties;
import ru.datamart.prostore.query.execution.plugin.adqm.mppw.configuration.properties.AdqmMppwProperties;
import ru.datamart.prostore.query.execution.plugin.adqm.query.service.DatabaseExecutor;
import ru.datamart.prostore.query.execution.plugin.api.check.CheckVersionRequest;
import ru.datamart.prostore.query.execution.plugin.api.service.check.CheckVersionService;
import io.vertx.core.CompositeFuture;
import io.vertx.core.Future;
import io.vertx.core.buffer.Buffer;
import io.vertx.core.json.Json;
import io.vertx.ext.web.client.HttpResponse;
import io.vertx.ext.web.client.WebClient;
import lombok.extern.slf4j.Slf4j;
import lombok.val;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Service;

import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

@Service("adqmCheckVersionService")
@Slf4j
public class AdqmCheckVersionService implements CheckVersionService {

    private final DatabaseExecutor databaseExecutor;
    private final AdqmVersionQueriesFactory versionQueriesFactory;
    private final AdqmVersionInfoFactory versionInfoFactory;
    private final AdqmMpprProperties mpprProperties;
    private final AdqmMppwProperties mppwProperties;
    private final WebClient webClient;
    private final List<ColumnMetadata> metadata;

    @Autowired
    public AdqmCheckVersionService(@Qualifier("adqmQueryExecutor") DatabaseExecutor databaseExecutor,
                                   AdqmVersionQueriesFactory versionQueriesFactory,
                                   AdqmVersionInfoFactory versionInfoFactory,
                                   AdqmMpprProperties mpprProperties,
                                   AdqmMppwProperties mppwProperties,
                                   @Qualifier("adqmWebClient") WebClient webClient) {
        this.databaseExecutor = databaseExecutor;
        this.versionQueriesFactory = versionQueriesFactory;
        this.versionInfoFactory = versionInfoFactory;
        this.mpprProperties = mpprProperties;
        this.mppwProperties = mppwProperties;
        this.webClient = webClient;
        metadata = createColumnMetadata();
    }

    @Override
    public Future<List<VersionInfo>> checkVersion(CheckVersionRequest request) {
        return CompositeFuture.join(databaseExecutor.execute(versionQueriesFactory.createAdqmVersionQuery(), metadata)
                        .map(versionInfoFactory::create),
                getConnectorVersions())
                .map(result -> {
                    List<List<VersionInfo>> list = result.list();
                    return list.stream()
                            .flatMap(List::stream)
                            .collect(Collectors.toList());
                });
    }

    private Future<List<VersionInfo>> getConnectorVersions() {
        return CompositeFuture.join(executeGetVersionRequest(mpprProperties.getVersionUrl()).compose(this::handleResponse),
                executeGetVersionRequest(mppwProperties.getVersionUrl()).compose(this::handleResponse))
                .map(result -> {
                    List<List<VersionInfo>> list = result.list();
                    return list.stream()
                            .flatMap(List::stream)
                            .collect(Collectors.toList());
                });
    }

    private Future<HttpResponse<Buffer>> executeGetVersionRequest(String uri) {
        return Future.future(promise -> {
            log.debug("Send request to [{}]", uri);
            webClient.getAbs(uri)
                    .send(promise);
        });
    }

    private Future<List<VersionInfo>> handleResponse(HttpResponse<Buffer> response) {
        return Future.future(promise -> {
            log.trace("Handle response [{}]", response);
            val statusCode = response.statusCode();
            if (statusCode == 200) {
                List<VersionInfo> successResponse = response.bodyAsJsonArray().stream()
                        .map(o -> Json.decodeValue(o.toString(), VersionInfo.class))
                        .collect(Collectors.toList());
                promise.complete(successResponse);
            } else {
                promise.fail(new DtmException("Error in receiving version info"));
            }
        });
    }

    private List<ColumnMetadata> createColumnMetadata() {
        return Arrays.asList(ColumnMetadata.builder()
                        .name(AdqmVersionQueriesFactory.COMPONENT_NAME_COLUMN)
                        .type(ColumnType.VARCHAR)
                        .build(),
                ColumnMetadata.builder()
                        .name(AdqmVersionQueriesFactory.VERSION_COLUMN)
                        .type(ColumnType.VARCHAR)
                        .build()
        );
    }
}
