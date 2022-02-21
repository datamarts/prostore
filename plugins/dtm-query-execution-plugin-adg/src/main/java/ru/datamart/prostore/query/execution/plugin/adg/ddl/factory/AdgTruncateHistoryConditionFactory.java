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
package ru.datamart.prostore.query.execution.plugin.adg.ddl.factory;

import ru.datamart.prostore.query.execution.plugin.api.dto.TruncateHistoryRequest;
import org.apache.calcite.sql.SqlDialect;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Service;

import java.util.ArrayList;
import java.util.List;

@Service
public class AdgTruncateHistoryConditionFactory {

    private static final String SYS_CN_CONDITION_PATTERN = "\"sys_to\" < %s";

    private final SqlDialect sqlDialect;

    @Autowired
    public AdgTruncateHistoryConditionFactory(@Qualifier("adgSqlDialect") SqlDialect sqlDialect) {
        this.sqlDialect = sqlDialect;
    }

    public String create(TruncateHistoryRequest request) {
        List<String> conditions = new ArrayList<>(2);

        if (request.getConditions() != null) {
            conditions.add(String.format("(%s)", request.getConditions().toSqlString(sqlDialect)));
        }
        if (request.getSysCn() != null) {
            conditions.add(String.format(SYS_CN_CONDITION_PATTERN, request.getSysCn()));
        }

        return String.join(" AND ", conditions);
    }
}
