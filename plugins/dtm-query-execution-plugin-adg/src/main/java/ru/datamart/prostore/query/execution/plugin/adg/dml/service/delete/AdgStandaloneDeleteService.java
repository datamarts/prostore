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
package ru.datamart.prostore.query.execution.plugin.adg.dml.service.delete;

import io.vertx.core.Future;
import lombok.val;
import org.apache.calcite.sql.SqlBasicCall;
import org.apache.calcite.sql.SqlDelete;
import org.apache.calcite.sql.SqlDialect;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.util.Util;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Service;
import ru.datamart.prostore.query.calcite.core.service.QueryTemplateExtractor;
import ru.datamart.prostore.query.calcite.core.util.SqlNodeTemplates;
import ru.datamart.prostore.query.execution.plugin.adg.base.service.converter.AdgPluginSpecificLiteralConverter;
import ru.datamart.prostore.query.execution.plugin.adg.query.service.AdgQueryExecutorService;
import ru.datamart.prostore.query.execution.plugin.api.request.DeleteRequest;

@Service
public class AdgStandaloneDeleteService {
    private final AdgQueryExecutorService executor;
    private final AdgPluginSpecificLiteralConverter pluginSpecificLiteralConverter;
    private final QueryTemplateExtractor queryTemplateExtractor;
    private final SqlDialect sqlDialect;

    public AdgStandaloneDeleteService(AdgQueryExecutorService executor,
                                      @Qualifier("adgPluginSpecificLiteralConverter") AdgPluginSpecificLiteralConverter pluginSpecificLiteralConverter,
                                      @Qualifier("adgQueryTemplateExtractor") QueryTemplateExtractor queryTemplateExtractor,
                                      @Qualifier("adgSqlDialect") SqlDialect sqlDialect) {
        this.executor = executor;
        this.pluginSpecificLiteralConverter = pluginSpecificLiteralConverter;
        this.queryTemplateExtractor = queryTemplateExtractor;
        this.sqlDialect = sqlDialect;
    }

    public Future<Void> execute(DeleteRequest request) {
        return Future.future(promise -> {
            val condition = request.getQuery().getCondition();
            val tableIdentifier = getTableIdentifier(request);
            val sqlDelete = new SqlDelete(SqlParserPos.ZERO, tableIdentifier, condition, null, null);
            convertParams(request, sqlDelete, condition)
                    .map(this::sqlNodeToString)
                    .compose(enrichedQuery -> executor.executeUpdate(enrichedQuery, request.getParameters()))
                    .onComplete(promise);
        });
    }

    public static SqlNode getTableIdentifier(DeleteRequest request) {
        SqlNode tableIdentifier = SqlNodeTemplates.identifier(request.getEntity().getExternalTableLocationPath());
        if (request.getQuery().getAlias() != null) {
            tableIdentifier = new SqlBasicCall(SqlStdOperatorTable.AS, new SqlNode[]{tableIdentifier, request.getQuery().getAlias()}, SqlParserPos.ZERO);
        }
        return tableIdentifier;
    }

    private String sqlNodeToString(SqlNode sqlNode) {
        return Util.toLinux(sqlNode.toSqlString(sqlDialect).getSql())
                .replaceAll("\r\n|\r|\n", " ");
    }

    private Future<SqlNode> convertParams(DeleteRequest request, SqlNode sqlDelete, SqlNode origDeleteCondition) {
        return Future.future(promise -> {
            if (origDeleteCondition == null) {
                promise.complete(sqlDelete);
                return;
            }

            val convertedParams = pluginSpecificLiteralConverter.convertDeleteParams(request.getExtractedParams(), request.getParametersTypes());
            val result = queryTemplateExtractor.enrichTemplate(sqlDelete, convertedParams);
            promise.complete(result);
        });
    }
}
