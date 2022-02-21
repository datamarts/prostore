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

import ru.datamart.prostore.common.plugin.sql.PreparedStatementRequest;
import ru.datamart.prostore.query.execution.plugin.adb.rollback.dto.AdbRollbackRequest;
import ru.datamart.prostore.query.execution.plugin.api.dto.RollbackRequest;
import ru.datamart.prostore.query.execution.plugin.api.factory.RollbackRequestFactory;

import java.util.List;
import java.util.stream.Collectors;

public abstract class AdbRollbackRequestFactory implements RollbackRequestFactory<AdbRollbackRequest> {

    @Override
    public AdbRollbackRequest create(RollbackRequest rollbackRequest) {
        String truncateSql = String.format(getTruncateStagingSql(),
            rollbackRequest.getDatamartMnemonic(), rollbackRequest.getDestinationTable());
        String deleteFromActualSql = String.format(getDeleteFromActualSql(), rollbackRequest.getDatamartMnemonic(),
            rollbackRequest.getDestinationTable(), rollbackRequest.getSysCn());
        List<PreparedStatementRequest> eraseOps = getEraseSql(rollbackRequest)
                .stream()
                .map(sql -> PreparedStatementRequest.onlySql(sql))
                .collect(Collectors.toList());

        return new AdbRollbackRequest(
            PreparedStatementRequest.onlySql(truncateSql),
            PreparedStatementRequest.onlySql(deleteFromActualSql),
            eraseOps
        );
    }

    protected abstract String getTruncateStagingSql();

    protected abstract String getDeleteFromActualSql();

    protected abstract List<String> getEraseSql(RollbackRequest rollbackRequest);

}
