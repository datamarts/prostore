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
package io.arenadata.dtm.query.execution.plugin.adb.rollback.dto;

import io.arenadata.dtm.common.plugin.sql.PreparedStatementRequest;
import io.arenadata.dtm.query.execution.plugin.api.rollback.PluginRollbackRequest;
import lombok.Data;
import lombok.EqualsAndHashCode;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

@Data
@EqualsAndHashCode(callSuper = true)
public class AdbRollbackRequest extends PluginRollbackRequest {
    private final PreparedStatementRequest truncate;
    private final PreparedStatementRequest deleteFromActual;
    private final List<PreparedStatementRequest> eraseOps;

    public AdbRollbackRequest(PreparedStatementRequest truncate,
                              PreparedStatementRequest deleteFromActual,
                              List<PreparedStatementRequest> eraseOps) {
        super(new ArrayList<>(Arrays.asList(
                truncate,
                deleteFromActual
        )));
        getStatements().addAll(eraseOps);

        this.truncate = truncate;
        this.deleteFromActual = deleteFromActual;
        this.eraseOps = eraseOps;
    }
}
