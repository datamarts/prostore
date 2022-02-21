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
package ru.datamart.prostore.query.execution.core.dml.service.impl.validate;

import io.vertx.core.Future;
import lombok.val;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlInsert;
import org.apache.calcite.sql.SqlNode;
import org.springframework.stereotype.Component;
import ru.datamart.prostore.common.model.ddl.Entity;
import ru.datamart.prostore.common.model.ddl.EntityFieldUtils;
import ru.datamart.prostore.query.execution.core.base.exception.table.ValidationDtmException;

import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

@Component
public class BasicUpdateColumnsValidator implements UpdateColumnsValidator {
    private static final List<String> SYSTEM_COLUMNS = Arrays.asList("sys_from", "sys_to", "sys_op");

    @Override
    public Future<Entity> validate(SqlNode sqlNode, Entity entity) {
        val insertNode = (SqlInsert) sqlNode;
        if (insertNode.getTargetColumnList() == null) {
            return Future.succeededFuture(entity);
        }
        val targetColumnNames = insertNode.getTargetColumnList().getList().stream()
                .map(node -> ((SqlIdentifier) node).getSimple())
                .collect(Collectors.toList());
        val containsSystemColumns = targetColumnNames.stream().anyMatch(SYSTEM_COLUMNS::contains);
        if (containsSystemColumns) {
            return Future.failedFuture(new ValidationDtmException(String.format("Columns [%s] is forbidden in query", String.join(", ", SYSTEM_COLUMNS))));
        }
        val entityColumnNames = EntityFieldUtils.getFieldNames(entity);
        targetColumnNames.removeAll(entityColumnNames);
        if (!targetColumnNames.isEmpty()) {
            return Future.failedFuture(new ValidationDtmException(String.format("Columns [%s] doesn't exist in entity %s",
                    String.join(", ", targetColumnNames), entity.getNameWithSchema())));
        }
        return Future.succeededFuture(entity);
    }
}
