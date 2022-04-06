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
package ru.datamart.prostore.query.execution.plugin.adg.dml.service.insert.values;

import io.vertx.core.Future;
import lombok.val;
import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.SqlDialect;
import org.apache.calcite.sql.SqlInsert;
import org.apache.calcite.sql.SqlNodeList;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Service;
import ru.datamart.prostore.query.execution.plugin.adg.query.service.AdgQueryExecutorService;
import ru.datamart.prostore.query.execution.plugin.api.dml.LlwUtils;
import ru.datamart.prostore.query.execution.plugin.api.request.InsertValuesRequest;
import ru.datamart.prostore.query.execution.plugin.api.service.PluginSpecificLiteralConverter;

import static ru.datamart.prostore.query.calcite.core.util.SqlNodeTemplates.identifier;

@Service
public class AdgStandaloneInsertValuesService {
    private final SqlDialect sqlDialect;
    private final AdgQueryExecutorService executor;
    private final PluginSpecificLiteralConverter specificLiteralConverter;

    public AdgStandaloneInsertValuesService(@Qualifier("adgSqlDialect") SqlDialect sqlDialect,
                                            AdgQueryExecutorService executor,
                                            @Qualifier("adgPluginSpecificLiteralConverter") PluginSpecificLiteralConverter specificLiteralConverter) {
        this.sqlDialect = sqlDialect;
        this.executor = executor;
        this.specificLiteralConverter = specificLiteralConverter;
    }

    public Future<Void> execute(InsertValuesRequest request) {
        return Future.future(promise -> {
            val source = (SqlCall) request.getQuery().getSource();
            val entity = request.getEntity();
            val targetColumns = request.getQuery().getTargetColumnList();
            val logicalFields = LlwUtils.getFilteredLogicalFields(entity, targetColumns);
            val actualColumns = LlwUtils.convertEntityFieldsToSqlNodeList(logicalFields);
            val values = LlwUtils.getConvertedRowsOfValues(source, logicalFields,
                    transformEntry -> specificLiteralConverter.convert(transformEntry.getSqlNode(), transformEntry.getSqlTypeName()));
            val sqlIdentifier = identifier(request.getEntity().getExternalTableLocationPath());
            val actualInsert = new SqlInsert(SqlParserPos.ZERO, SqlNodeList.EMPTY, sqlIdentifier, values, actualColumns);
            val sql = actualInsert.toSqlString(sqlDialect).getSql();

            executor.executeUpdate(sql, request.getParameters())
                    .onComplete(promise);
        });
    }

}
