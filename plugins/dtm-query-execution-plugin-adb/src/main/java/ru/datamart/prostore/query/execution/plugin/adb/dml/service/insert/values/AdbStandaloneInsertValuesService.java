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
package ru.datamart.prostore.query.execution.plugin.adb.dml.service.insert.values;

import io.vertx.core.Future;
import lombok.val;
import org.apache.calcite.sql.SqlDialect;
import org.apache.calcite.sql.SqlInsert;
import org.apache.calcite.sql.SqlNodeList;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Service;
import ru.datamart.prostore.query.execution.plugin.adb.query.service.DatabaseExecutor;
import ru.datamart.prostore.query.execution.plugin.api.request.InsertValuesRequest;

import static java.util.Collections.emptyList;
import static ru.datamart.prostore.query.calcite.core.util.SqlNodeTemplates.identifier;
import static ru.datamart.prostore.query.execution.plugin.api.dml.LlwUtils.replaceDynamicParams;

@Service
public class AdbStandaloneInsertValuesService {
    private final SqlDialect sqlDialect;
    private final DatabaseExecutor executor;


    public AdbStandaloneInsertValuesService(@Qualifier("adbSqlDialect") SqlDialect sqlDialect,
                                            DatabaseExecutor executor) {
        this.sqlDialect = sqlDialect;
        this.executor = executor;
    }

    public Future<Void> execute(InsertValuesRequest request) {
        return Future.future(promise -> {
            val query = request.getQuery();
            val source = replaceDynamicParams(query.getSource());
            val sqlIdentifier = identifier(request.getEntity().getExternalTableLocationPath());
            val actualInsert = new SqlInsert(SqlParserPos.ZERO, SqlNodeList.EMPTY, sqlIdentifier, source, query.getTargetColumnList());
            val sql = actualInsert.toSqlString(sqlDialect).getSql();
            executor.executeWithParams(sql, request.getParameters(), emptyList())
                    .<Void> mapEmpty()
                    .onComplete(promise);
        });
    }
}
