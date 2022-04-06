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
package ru.datamart.prostore.query.execution.plugin.adqm.dml.service.insertvalues;

import io.vertx.core.Future;
import lombok.val;
import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.SqlInsert;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlNodeList;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Service;
import ru.datamart.prostore.common.model.ddl.EntityField;
import ru.datamart.prostore.common.model.ddl.EntityFieldUtils;
import ru.datamart.prostore.query.execution.plugin.adqm.factory.AdqmProcessingSqlFactory;
import ru.datamart.prostore.query.execution.plugin.adqm.query.service.DatabaseExecutor;
import ru.datamart.prostore.query.execution.plugin.api.dml.LlwUtils;
import ru.datamart.prostore.query.execution.plugin.api.request.InsertValuesRequest;
import ru.datamart.prostore.query.execution.plugin.api.service.PluginSpecificLiteralConverter;

import java.util.Collections;
import java.util.List;

import static ru.datamart.prostore.query.calcite.core.util.SqlNodeTemplates.identifier;
import static ru.datamart.prostore.query.execution.plugin.adqm.dml.util.AdqmDmlUtils.validatePrimaryKeys;
import static ru.datamart.prostore.query.execution.plugin.api.dml.LlwUtils.convertEntityFieldsToSqlNodeList;

@Service
public class AdqmStandaloneInsertValuesService {
    private static final String SHARD_POSTFIX = "_shard";

    private final PluginSpecificLiteralConverter pluginSpecificLiteralConverter;
    private final AdqmProcessingSqlFactory adqmProcessingSqlFactory;
    private final DatabaseExecutor databaseExecutor;

    public AdqmStandaloneInsertValuesService(@Qualifier("adqmPluginSpecificLiteralConverter") PluginSpecificLiteralConverter pluginSpecificLiteralConverter,
                                             AdqmProcessingSqlFactory adqmProcessingSqlFactory,
                                             @Qualifier("adqmQueryExecutor") DatabaseExecutor databaseExecutor) {
        this.pluginSpecificLiteralConverter = pluginSpecificLiteralConverter;
        this.adqmProcessingSqlFactory = adqmProcessingSqlFactory;
        this.databaseExecutor = databaseExecutor;
    }

    public Future<Void> execute(InsertValuesRequest request) {
        return Future.future(promise -> {
            val source = (SqlCall) request.getQuery().getSource();
            val entity = request.getEntity();
            val targetColumns = request.getQuery().getTargetColumnList();
            val logicalFields = LlwUtils.getFilteredLogicalFields(entity, targetColumns);
            val pkFieldNames = EntityFieldUtils.getPkFieldNames(entity);

            validatePrimaryKeys(logicalFields, pkFieldNames);

            val actualValues = LlwUtils.getConvertedRowsOfValues(source, logicalFields,
                    transformEntry -> pluginSpecificLiteralConverter.convert(transformEntry.getSqlNode(), transformEntry.getSqlTypeName()));
            val actualInsertSql = getSqlInsert(request, logicalFields, actualValues);

            databaseExecutor.executeWithParams(actualInsertSql, request.getParameters(), Collections.emptyList())
                    .compose(v -> flushAndOptimize(request))
                    .onComplete(promise);
        });
    }

    private Future<Void> flushAndOptimize(InsertValuesRequest request) {
        val entityPath = request.getEntity().getExternalTableLocationPath();
        val entityPathShard = entityPath + SHARD_POSTFIX;
        return databaseExecutor.executeUpdate(adqmProcessingSqlFactory.getFlushSql(entityPath))
                .compose(v -> databaseExecutor.executeUpdate(adqmProcessingSqlFactory.getOptimizeSql(entityPathShard)));
    }

    private String getSqlInsert(InsertValuesRequest request, List<EntityField> insertedColumns, SqlNode actualValues) {
        val actualColumnList = convertEntityFieldsToSqlNodeList(insertedColumns);
        val sqlIdentifier = identifier(request.getEntity().getExternalTableLocationPath());
        val result = new SqlInsert(SqlParserPos.ZERO, SqlNodeList.EMPTY, sqlIdentifier, actualValues, actualColumnList);
        return adqmProcessingSqlFactory.getSqlFromNodes(result);
    }
}
