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
package io.arenadata.dtm.query.execution.plugin.adqm.dml.service;

import io.arenadata.dtm.common.model.ddl.EntityField;
import io.arenadata.dtm.common.model.ddl.EntityFieldUtils;
import io.arenadata.dtm.query.execution.plugin.adqm.factory.AdqmProcessingSqlFactory;
import io.arenadata.dtm.query.execution.plugin.adqm.query.service.DatabaseExecutor;
import io.arenadata.dtm.query.execution.plugin.api.dml.LlwUtils;
import io.arenadata.dtm.query.execution.plugin.api.request.UpsertValuesRequest;
import io.arenadata.dtm.query.execution.plugin.api.service.PluginSpecificLiteralConverter;
import io.arenadata.dtm.query.execution.plugin.api.service.UpsertValuesService;
import io.vertx.core.Future;
import lombok.val;
import org.apache.calcite.sql.SqlBasicCall;
import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.SqlInsert;
import org.apache.calcite.sql.SqlNodeList;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Service;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static io.arenadata.dtm.query.calcite.core.util.SqlNodeTemplates.longLiteral;
import static io.arenadata.dtm.query.execution.plugin.adqm.dml.util.AdqmDmlUtils.*;

@Service("adqmUpsertValuesService")
public class AdqmUpsertValuesService implements UpsertValuesService {

    private final PluginSpecificLiteralConverter pluginSpecificLiteralConverter;
    private final AdqmProcessingSqlFactory adqmProcessingSqlFactory;
    private final DatabaseExecutor databaseExecutor;

    public AdqmUpsertValuesService(@Qualifier("adqmTemplateParameterConverter") PluginSpecificLiteralConverter pluginSpecificLiteralConverter,
                                   AdqmProcessingSqlFactory adqmProcessingSqlFactory,
                                   @Qualifier("adqmQueryExecutor") DatabaseExecutor databaseExecutor) {
        this.pluginSpecificLiteralConverter = pluginSpecificLiteralConverter;
        this.adqmProcessingSqlFactory = adqmProcessingSqlFactory;
        this.databaseExecutor = databaseExecutor;
    }

    @Override
    public Future<Void> execute(UpsertValuesRequest request) {
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

    private Future<Void> flushAndOptimize(UpsertValuesRequest request) {
        return databaseExecutor.executeUpdate(adqmProcessingSqlFactory.getFlushActualSql(request.getEnvName(), request.getDatamartMnemonic(), request.getEntity().getName()))
                .compose(unused -> databaseExecutor.executeUpdate(adqmProcessingSqlFactory.getOptimizeActualSql(request.getEnvName(), request.getDatamartMnemonic(), request.getEntity().getName())));
    }

    private String getSqlInsert(UpsertValuesRequest request, List<EntityField> insertedColumns, SqlBasicCall
            actualValues) {
        val actualColumnList = getInsertedColumnsList(insertedColumns);
        val result = new SqlInsert(SqlParserPos.ZERO, SqlNodeList.EMPTY, getActualTableIdentifier(request.getEnvName(), request.getDatamartMnemonic(), request.getEntity().getName()), actualValues, actualColumnList);
        return adqmProcessingSqlFactory.getSqlFromNodes(result);
    }
}
