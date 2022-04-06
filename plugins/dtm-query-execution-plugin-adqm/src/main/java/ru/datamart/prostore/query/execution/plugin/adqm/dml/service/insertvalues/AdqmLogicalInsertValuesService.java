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
import org.apache.calcite.sql.SqlBasicCall;
import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.SqlInsert;
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

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static ru.datamart.prostore.query.calcite.core.util.SqlNodeTemplates.longLiteral;
import static ru.datamart.prostore.query.execution.plugin.adqm.dml.util.AdqmDmlUtils.*;

@Service
public class AdqmLogicalInsertValuesService {
    private final PluginSpecificLiteralConverter pluginSpecificLiteralConverter;
    private final AdqmProcessingSqlFactory adqmProcessingSqlFactory;
    private final DatabaseExecutor databaseExecutor;

    public AdqmLogicalInsertValuesService(@Qualifier("adqmPluginSpecificLiteralConverter") PluginSpecificLiteralConverter pluginSpecificLiteralConverter,
                                          AdqmProcessingSqlFactory adqmProcessingSqlFactory,
                                          @Qualifier("adqmQueryExecutor") DatabaseExecutor databaseExecutor) {
        this.pluginSpecificLiteralConverter = pluginSpecificLiteralConverter;
        this.adqmProcessingSqlFactory = adqmProcessingSqlFactory;
        this.databaseExecutor = databaseExecutor;
    }

    public Future<Void> execute(InsertValuesRequest request) {
        return Future.future(promise -> {
            val source = (SqlCall) request.getQuery().getSource();
            val logicalFields = LlwUtils.getFilteredLogicalFields(request.getEntity(), request.getQuery().getTargetColumnList());
            val pkFieldNames = EntityFieldUtils.getPkFieldNames(request.getEntity());

            validatePrimaryKeys(logicalFields, pkFieldNames);

            val systemRowValuesToAdd = Arrays.asList(longLiteral(request.getSysCn()), MAX_CN_LITERAL,
                    ZERO_SYS_OP_LITERAL, MAX_CN_LITERAL, ONE_SIGN_LITERAL);
            val actualValues = LlwUtils.getExtendRowsOfValues(source, logicalFields, systemRowValuesToAdd,
                    transformEntry -> pluginSpecificLiteralConverter.convert(transformEntry.getSqlNode(), transformEntry.getSqlTypeName()));
            val actualInsertSql = getSqlInsert(request, logicalFields, actualValues);
            val closeInsertSql = adqmProcessingSqlFactory.getCloseVersionSqlByTableActual(request.getEnvName(), request.getDatamartMnemonic(), request.getEntity(), request.getSysCn());

            databaseExecutor.executeWithParams(actualInsertSql, request.getParameters(), Collections.emptyList())
                    .compose(ignored -> flushAndOptimize(request))
                    .compose(ignored -> databaseExecutor.executeUpdate(closeInsertSql))
                    .compose(ignored -> flushAndOptimize(request))
                    .onComplete(promise);
        });
    }

    private Future<Void> flushAndOptimize(InsertValuesRequest request) {
        return databaseExecutor.executeUpdate(adqmProcessingSqlFactory.getFlushActualSql(request.getEnvName(), request.getDatamartMnemonic(), request.getEntity().getName()))
                .compose(unused -> databaseExecutor.executeUpdate(adqmProcessingSqlFactory.getOptimizeActualSql(request.getEnvName(), request.getDatamartMnemonic(), request.getEntity().getName())));
    }

    private String getSqlInsert(InsertValuesRequest request, List<EntityField> insertedColumns, SqlBasicCall actualValues) {
        val actualColumnList = getInsertedColumnsList(insertedColumns);
        val result = new SqlInsert(SqlParserPos.ZERO, SqlNodeList.EMPTY, getActualTableIdentifier(request.getEnvName(), request.getDatamartMnemonic(), request.getEntity().getName()), actualValues, actualColumnList);
        return adqmProcessingSqlFactory.getSqlFromNodes(result);
    }

}
