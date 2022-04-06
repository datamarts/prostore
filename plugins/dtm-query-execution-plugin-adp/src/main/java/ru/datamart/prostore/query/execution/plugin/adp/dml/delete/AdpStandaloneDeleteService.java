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
package ru.datamart.prostore.query.execution.plugin.adp.dml.delete;

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
import ru.datamart.prostore.query.execution.plugin.adp.db.service.DatabaseExecutor;
import ru.datamart.prostore.query.execution.plugin.api.request.DeleteRequest;

import static java.util.Collections.emptyList;
import static ru.datamart.prostore.query.calcite.core.util.SqlNodeTemplates.identifier;

@Service
public class AdpStandaloneDeleteService {
    private final DatabaseExecutor executor;
    private final QueryTemplateExtractor queryTemplateExtractor;
    private final SqlDialect sqlDialect;

    public AdpStandaloneDeleteService(DatabaseExecutor executor,
                                      @Qualifier("adpQueryTemplateExtractor") QueryTemplateExtractor queryTemplateExtractor,
                                      @Qualifier("adpSqlDialect") SqlDialect sqlDialect) {
        this.executor = executor;
        this.queryTemplateExtractor = queryTemplateExtractor;
        this.sqlDialect = sqlDialect;
    }

    public Future<Void> execute(DeleteRequest request) {
        return Future.future(promise -> {
            val origQuery = request.getQuery();
            val deleteSql = new SqlDelete(origQuery.getParserPosition(),
                    getTableIdentifier(request),
                    origQuery.getCondition(),
                    origQuery.getSourceSelect(),
                    null);
            enrichParams(request, deleteSql, origQuery.getCondition())
                    .map(this::sqlNodeToString)
                    .compose(deleteQuery -> executor.executeWithParams(deleteQuery, request.getParameters(), emptyList()))
                    .<Void>mapEmpty()
                    .onComplete(promise);
        });
    }

    private Future<SqlNode> enrichParams(DeleteRequest request, SqlNode sqlDelete, SqlNode origDeleteCondition) {
        if (origDeleteCondition == null) {
            return Future.succeededFuture(sqlDelete);
        }

        return Future.succeededFuture(queryTemplateExtractor.enrichTemplate(sqlDelete, request.getExtractedParams()));
    }

    public static SqlNode getTableIdentifier(DeleteRequest request) {
        SqlNode tableIdentifier = identifier(request.getEntity().getExternalTableLocationPath());
        if (request.getQuery().getAlias() != null) {
            tableIdentifier = new SqlBasicCall(SqlStdOperatorTable.AS, new SqlNode[]{tableIdentifier, request.getQuery().getAlias()}, SqlParserPos.ZERO);
        }
        return tableIdentifier;
    }

    private String sqlNodeToString(SqlNode sqlNode) {
        return Util.toLinux(sqlNode.toSqlString(sqlDialect).getSql()).replaceAll("\r\n|\r|\n", " ");
    }

}
