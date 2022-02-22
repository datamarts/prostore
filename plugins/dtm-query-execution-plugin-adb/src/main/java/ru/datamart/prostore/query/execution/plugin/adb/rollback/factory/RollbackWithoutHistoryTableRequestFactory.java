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
package ru.datamart.prostore.query.execution.plugin.adb.rollback.factory;

import ru.datamart.prostore.query.execution.plugin.api.dto.RollbackRequest;

import java.util.Arrays;
import java.util.List;

public class RollbackWithoutHistoryTableRequestFactory extends AdbRollbackRequestFactory {

    private static final String TRUNCATE_STAGING = "TRUNCATE %s.%s_staging";
    private static final String DELETE_FROM_ACTUAL = "DELETE FROM %s.%s_actual WHERE sys_from = %s";
    private static final String UPDATE_ACTUAL_SQL = "UPDATE %s.%s_actual \n" +
        "SET sys_to = NULL, sys_op = 0 \n" +
        "WHERE sys_to = %s";

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
        long sysTo = rollbackRequest.getSysCn() - 1;
        return Arrays.asList(
                String.format(UPDATE_ACTUAL_SQL,
                        rollbackRequest.getDatamartMnemonic(),
                        rollbackRequest.getDestinationTable(),
                        sysTo)
        );
    }
}
