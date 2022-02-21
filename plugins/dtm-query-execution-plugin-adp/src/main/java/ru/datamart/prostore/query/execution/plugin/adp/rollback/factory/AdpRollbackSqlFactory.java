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
package ru.datamart.prostore.query.execution.plugin.adp.rollback.factory;

import ru.datamart.prostore.common.model.ddl.Entity;

public class AdpRollbackSqlFactory {
    private AdpRollbackSqlFactory() {
    }

    private static final String ROLLBACK_SQL_TEMPLATE = "TRUNCATE ${datamart}.${tableName}_staging;\n" +
            "DELETE FROM ${datamart}.${tableName}_actual WHERE sys_from = ${sysCn};\n" +
            "UPDATE ${datamart}.${tableName}_actual SET sys_to = NULL, sys_op = 0 WHERE sys_to = ${previousSysCn};";

    public static String getRollbackSql(String datamart, Entity entity, Long sysCn) {
        String currentSysCn = Long.toString(sysCn);
        String previousSysCn = Long.toString(sysCn - 1L);

        return ROLLBACK_SQL_TEMPLATE.replace("${datamart}", datamart)
                .replace("${tableName}", entity.getName())
                .replace("${sysCn}", currentSysCn)
                .replace("${previousSysCn}", previousSysCn);
    }

}
