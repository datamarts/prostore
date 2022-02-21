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
package ru.datamart.prostore.query.execution.core.config.service.impl;

import ru.datamart.prostore.common.exception.DtmException;
import ru.datamart.prostore.common.reader.QueryResult;
import ru.datamart.prostore.common.reader.SourceType;
import ru.datamart.prostore.query.calcite.core.extension.config.SqlConfigType;
import ru.datamart.prostore.query.calcite.core.extension.config.function.SqlConfigStorageAdd;
import ru.datamart.prostore.query.execution.core.base.repository.zookeeper.DatamartDao;
import ru.datamart.prostore.query.execution.core.config.dto.ConfigRequestContext;
import ru.datamart.prostore.query.execution.core.config.service.ConfigExecutor;
import ru.datamart.prostore.query.execution.core.init.service.CoreInitializationService;
import ru.datamart.prostore.query.execution.core.plugin.service.DataSourcePluginService;
import ru.datamart.prostore.query.execution.plugin.api.request.DdlRequest;
import io.vertx.core.CompositeFuture;
import io.vertx.core.Future;
import lombok.extern.slf4j.Slf4j;
import lombok.val;
import org.apache.calcite.sql.SqlKind;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.util.List;
import java.util.stream.Collectors;

@Slf4j
@Component
public class ConfigStorageAddDdlExecutor implements ConfigExecutor {

    public static final String STORAGE_IS_NOT_ACTIVE = "Storage [%s] is not active!";
    public static final String SOURCE_TYPE_IS_NOT_SET = "Source type is not set!";
    private final DataSourcePluginService dataSourcePluginService;
    private final DatamartDao datamartDao;
    private final CoreInitializationService initializationService;

    @Autowired
    public ConfigStorageAddDdlExecutor(DataSourcePluginService dataSourcePluginService,
                                       DatamartDao datamartDao, CoreInitializationService initializationService) {
        this.dataSourcePluginService = dataSourcePluginService;
        this.datamartDao = datamartDao;
        this.initializationService = initializationService;
    }

    @Override
    public Future<QueryResult> execute(ConfigRequestContext context) {
        SqlConfigStorageAdd configStorageAdd = (SqlConfigStorageAdd) context.getSqlNode();
        if (configStorageAdd.getSourceType() == null) {
            return Future.failedFuture(new DtmException(SOURCE_TYPE_IS_NOT_SET));
        }
        val sourceType = configStorageAdd.getSourceType();
        if (!dataSourcePluginService.getSourceTypes().contains(sourceType)) {
            return Future.failedFuture(new DtmException(String.format(STORAGE_IS_NOT_ACTIVE, sourceType.name())));
        }
        return datamartDao.getDatamarts()
                .compose(datamarts -> joinCreateDatamartFutures(sourceType, datamarts, context))
                .compose(v -> initializationService.execute(sourceType))
                .map(v -> QueryResult.emptyResult());
    }

    private CompositeFuture joinCreateDatamartFutures(SourceType sourceType,
                                                      List<String> datamarts,
                                                      ConfigRequestContext context) {
        return CompositeFuture.join(
                datamarts.stream()
                        .map(datamart -> createDatamartFuture(sourceType, datamart, context))
                        .collect(Collectors.toList()));
    }

    private Future<Void> createDatamartFuture(SourceType sourceType,
                                              String datamart,
                                              ConfigRequestContext context) {
        return dataSourcePluginService.ddl(sourceType, context.getMetrics(), DdlRequest.builder()
                .requestId(context.getRequest().getQueryRequest().getRequestId())
                .datamartMnemonic(datamart)
                .envName(context.getEnvName())
                .sqlKind(SqlKind.CREATE_SCHEMA)
                .build());
    }

    @Override
    public SqlConfigType getConfigType() {
        return SqlConfigType.CONFIG_STORAGE_ADD;
    }

}
