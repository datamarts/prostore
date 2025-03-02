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
package ru.datamart.prostore.query.execution.plugin.adb.ddl.factory.impl;

import ru.datamart.prostore.common.model.ddl.Entity;
import ru.datamart.prostore.query.execution.plugin.adb.base.dto.metadata.AdbTables;
import ru.datamart.prostore.query.execution.plugin.api.dto.TruncateHistoryRequest;
import org.apache.calcite.sql.SqlDialect;
import org.springframework.beans.factory.annotation.Qualifier;

import java.util.Arrays;
import java.util.List;

public class TruncateQueryWithHistoryFactory extends TruncateQueryBaseFactory {

    public TruncateQueryWithHistoryFactory(@Qualifier("adbSqlDialect") SqlDialect sqlDialect) {
        super(sqlDialect);
    }

    @Override
    public List<String> create(TruncateHistoryRequest request) {
        String whereExpression = buildCondition(request.getConditions(), " WHERE %s");
        Entity entity = request.getEntity();
        return Arrays.asList(String.format(DELETE_RECORDS_PATTERN, entity.getSchema(), entity.getName(),
                AdbTables.ACTUAL_TABLE_POSTFIX, whereExpression),
                String.format(DELETE_RECORDS_PATTERN, entity.getSchema(), entity.getName(),
                        AdbTables.HISTORY_TABLE_POSTFIX, whereExpression));
    }

    @Override
    public String createWithSysCn(TruncateHistoryRequest request) {
        Entity entity = request.getEntity();

        String concatenationExpression = buildCondition(request.getConditions(), "%s AND ");
        String sysCnExpression = String.format(SYS_CN_CONDITION, request.getSysCn());
        String whereExpression = String.format(" WHERE %s%s", concatenationExpression, sysCnExpression);

        return String.format(DELETE_RECORDS_PATTERN,
                entity.getSchema(),
                entity.getName(),
                AdbTables.HISTORY_TABLE_POSTFIX,
                whereExpression);
    }

}
