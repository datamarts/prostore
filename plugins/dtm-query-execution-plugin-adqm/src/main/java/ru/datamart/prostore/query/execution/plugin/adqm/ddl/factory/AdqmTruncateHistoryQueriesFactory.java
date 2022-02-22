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
package ru.datamart.prostore.query.execution.plugin.adqm.ddl.factory;

import ru.datamart.prostore.common.model.ddl.Entity;
import ru.datamart.prostore.common.model.ddl.EntityField;
import ru.datamart.prostore.query.execution.plugin.adqm.base.utils.Constants;
import ru.datamart.prostore.query.execution.plugin.api.dto.TruncateHistoryRequest;
import org.apache.calcite.sql.SqlDialect;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Service;

import java.util.List;
import java.util.stream.Collectors;

@Service
public class AdqmTruncateHistoryQueriesFactory {

    private static final String QUERY_PATTERN = "INSERT INTO %s.%s_actual (%s, sign)\n" +
            "SELECT %s, -1\n" +
            "FROM %s.%s_actual t FINAL\n" +
            "WHERE sign = 1%s%s";

    private final SqlDialect sqlDialect;

    @Autowired
    public AdqmTruncateHistoryQueriesFactory(@Qualifier("adqmSqlDialect") SqlDialect sqlDialect) {
        this.sqlDialect = sqlDialect;
    }

    public String insertIntoActualQuery(TruncateHistoryRequest request) {
        String sysCnExpression = request.getSysCn() != null ?
                String.format(" AND sys_to < %s", request.getSysCn()) : "";
        String whereExpression = request.getConditions() != null ?
                String.format(" AND (%s)", request.getConditions().toSqlString(sqlDialect)) : "";

        Entity entity = request.getEntity();
        String dbName = Constants.getDbName(request.getEnvName(), entity.getSchema());
        List<String> orderByColumns = entity.getFields().stream()
                .filter(field -> field.getPrimaryOrder() != null)
                .map(EntityField::getName)
                .collect(Collectors.toList());
        orderByColumns.add(Constants.SYS_FROM_FIELD);
        String orderByColumnsStr = String.join(", ", orderByColumns);
        return String.format(QUERY_PATTERN, dbName, entity.getName(), orderByColumnsStr, orderByColumnsStr, dbName,
                        entity.getName(), sysCnExpression, whereExpression);
    }
}
