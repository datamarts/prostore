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
package ru.datamart.prostore.query.execution.plugin.adb.rollback.factory;

import ru.datamart.prostore.common.model.ddl.EntityField;
import ru.datamart.prostore.query.execution.plugin.api.dto.RollbackRequest;

import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

public class RollbackWithHistoryTableRequestFactory extends AdbRollbackRequestFactory {

    private static final String TRUNCATE_STAGING = "TRUNCATE %s.%s_staging";
    private static final String DELETE_FROM_ACTUAL = "DELETE FROM %s.%s_actual WHERE sys_from = %s";
    private static final String INSERT_ACTUAL_SQL = "INSERT INTO %s.%s_actual (%s, sys_from, sys_to, sys_op)\n" +
        "SELECT %s, sys_from, NULL, 0\n" +
        "FROM %s.%s_history\n" +
        "WHERE sys_to = %s";
    private static final String DELETE_FROM_HISTORY = "DELETE FROM %s.%s_history WHERE sys_to = %s";

    @Override
    protected String getTruncateStagingSql() {
        return TRUNCATE_STAGING;
    }

    @Override
    protected String getDeleteFromActualSql() {
        return DELETE_FROM_ACTUAL;
    }

    @Override
    protected List<String> getEraseSql(RollbackRequest rollbackRequest) {
        String fields = rollbackRequest.getEntity().getFields().stream()
                .map(EntityField::getName)
                .collect(Collectors.joining(","));
        long sysTo = rollbackRequest.getSysCn() - 1;

        String insertSql = String.format(INSERT_ACTUAL_SQL, rollbackRequest.getDatamartMnemonic(),
                rollbackRequest.getDestinationTable(), fields, fields,
                rollbackRequest.getDatamartMnemonic(), rollbackRequest.getDestinationTable(), sysTo);
        String deleteFromHistory = String.format(DELETE_FROM_HISTORY, rollbackRequest.getDatamartMnemonic(),
                rollbackRequest.getDestinationTable(), sysTo);

        return Arrays.asList(insertSql, deleteFromHistory);
    }
}
