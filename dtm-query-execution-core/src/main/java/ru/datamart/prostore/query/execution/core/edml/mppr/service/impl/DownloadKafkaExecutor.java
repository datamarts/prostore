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
package ru.datamart.prostore.query.execution.core.edml.mppr.service.impl;

import ru.datamart.prostore.common.dto.QueryParserRequest;
import ru.datamart.prostore.common.exception.DtmException;
import ru.datamart.prostore.common.model.ddl.ExternalTableLocationType;
import ru.datamart.prostore.common.reader.QueryResult;
import ru.datamart.prostore.common.reader.SourceType;
import ru.datamart.prostore.query.calcite.core.extension.dml.SqlDataSourceTypeGetter;
import ru.datamart.prostore.query.calcite.core.service.QueryParserService;
import ru.datamart.prostore.query.execution.core.base.service.column.CheckColumnTypesService;
import ru.datamart.prostore.query.execution.core.dml.dto.PluginDeterminationRequest;
import ru.datamart.prostore.query.execution.core.dml.dto.PluginDeterminationResult;
import ru.datamart.prostore.query.execution.core.dml.service.ColumnMetadataService;
import ru.datamart.prostore.query.execution.core.dml.service.PluginDeterminationService;
import ru.datamart.prostore.query.execution.core.edml.dto.EdmlRequestContext;
import ru.datamart.prostore.query.execution.core.edml.mppr.factory.MpprKafkaRequestFactory;
import ru.datamart.prostore.query.execution.core.edml.mppr.service.EdmlDownloadExecutor;
import ru.datamart.prostore.query.execution.core.plugin.service.DataSourcePluginService;
import ru.datamart.prostore.query.execution.plugin.api.mppr.MpprRequest;
import io.vertx.core.Future;
import lombok.extern.slf4j.Slf4j;
import lombok.val;
import org.apache.calcite.rel.RelRoot;
import org.apache.calcite.sql.SqlDialect;
import org.apache.calcite.sql.SqlInsert;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Service;

@Service
@Slf4j
public class DownloadKafkaExecutor implements EdmlDownloadExecutor {

    private final QueryParserService queryParserService;
    private final CheckColumnTypesService checkColumnTypesService;
    private final MpprKafkaRequestFactory mpprKafkaRequestFactory;
    private final ColumnMetadataService columnMetadataService;
    private final DataSourcePluginService pluginService;
    private final SqlDialect sqlDialect;
    private final PluginDeterminationService pluginDeterminationService;

    @Autowired
    public DownloadKafkaExecutor(@Qualifier("coreCalciteDMLQueryParserService") QueryParserService coreCalciteDMLQueryParserService,
                                 CheckColumnTypesService checkColumnTypesService,
                                 MpprKafkaRequestFactory mpprKafkaRequestFactory,
                                 ColumnMetadataService columnMetadataService,
                                 DataSourcePluginService pluginService,
                                 @Qualifier("coreSqlDialect") SqlDialect sqlDialect,
                                 PluginDeterminationService pluginDeterminationService) {
        this.queryParserService = coreCalciteDMLQueryParserService;
        this.checkColumnTypesService = checkColumnTypesService;
        this.mpprKafkaRequestFactory = mpprKafkaRequestFactory;
        this.columnMetadataService = columnMetadataService;
        this.pluginService = pluginService;
        this.sqlDialect = sqlDialect;
        this.pluginDeterminationService = pluginDeterminationService;
    }

    @Override
    public Future<QueryResult> execute(EdmlRequestContext context) {
        return getActualDatasourceType(context)
                .compose(actualDatasourceType -> queryParserService.parse(new QueryParserRequest(context.getDmlSubQuery(), context.getLogicalSchema()))
                        .map(parserResponse -> {
                            if (!checkColumnTypesService.check(context.getDestinationEntity().getFields(), parserResponse.getRelNode())) {
                                throw getFailCheckColumnsException(context);
                            }
                            return parserResponse.getRelNode();
                        })
                        .compose(relNode -> initColumnMetadata(relNode, context))
                        .compose(mpprKafkaRequest -> pluginService.mppr(actualDatasourceType, context.getMetrics(), mpprKafkaRequest)));
    }

    private Future<SourceType> getActualDatasourceType(EdmlRequestContext context) {
        SourceType preferredSourceType = null;
        val source = ((SqlInsert) context.getSqlNode()).getSource();
        if (source instanceof SqlDataSourceTypeGetter) {
            preferredSourceType = ((SqlDataSourceTypeGetter) source).getDatasourceType().getValue();
        }

        val pluginDeterminationRequest = PluginDeterminationRequest.builder()
                .sqlNode(source)
                .query(source.toSqlString(sqlDialect).getSql())
                .schema(context.getLogicalSchema())
                .preferredSourceType(preferredSourceType)
                .build();
        return pluginDeterminationService.determine(pluginDeterminationRequest)
                .map(PluginDeterminationResult::getExecution);
    }

    private DtmException getFailCheckColumnsException(EdmlRequestContext context) {
        return new DtmException(String.format(CheckColumnTypesService.FAIL_CHECK_COLUMNS_PATTERN,
                context.getDestinationEntity().getName()));
    }

    private Future<MpprRequest> initColumnMetadata(RelRoot relNode, EdmlRequestContext context) {
        return columnMetadataService.getColumnMetadata(relNode)
                .compose(columnMetadata -> mpprKafkaRequestFactory.create(context, columnMetadata));
    }

    @Override
    public ExternalTableLocationType getDownloadType() {
        return ExternalTableLocationType.KAFKA;
    }
}
