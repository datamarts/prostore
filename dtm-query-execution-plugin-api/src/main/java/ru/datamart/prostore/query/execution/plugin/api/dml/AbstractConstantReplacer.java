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
package ru.datamart.prostore.query.execution.plugin.api.dml;

import lombok.val;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlNodeList;
import org.apache.calcite.sql.parser.SqlParserPos;
import ru.datamart.prostore.common.model.ddl.EntityField;
import ru.datamart.prostore.query.calcite.core.node.SqlPredicatePart;
import ru.datamart.prostore.query.calcite.core.node.SqlPredicates;
import ru.datamart.prostore.query.calcite.core.node.SqlSelectTree;

import java.util.List;

public abstract class AbstractConstantReplacer {
    private static final SqlPredicates COLUMN_PREDICATE = SqlPredicates.builder()
            .anyOf(SqlPredicatePart.eq(SqlKind.SELECT))
            .anyOf(SqlPredicatePart.eq(SqlKind.OTHER))
            .build();

    public SqlNode replace(List<EntityField> entityFields, SqlNode sourceQuery) {
        val sourceQueryColumnNodes = new SqlSelectTree(sourceQuery)
                .findNodes(COLUMN_PREDICATE, true);
        val sqlTreeNode = sourceQueryColumnNodes.get(0);
        val nodeList = (SqlNodeList) sqlTreeNode.getNode();

        val containsStar = nodeList.getList().stream()
                .anyMatch(sqlNode -> sqlNode instanceof SqlIdentifier && ((SqlIdentifier) sqlNode).isStar());
        if (containsStar) {
            return sourceQuery;
        }

        val sqlNodes = new SqlNodeList(SqlParserPos.ZERO);

        for (int i = 0; i < entityFields.size(); i++) {
            val node = getSqlNode(entityFields.get(i), nodeList.get(i));
            sqlNodes.add(node);
        }

        sqlTreeNode.getSqlNodeSetter().accept(sqlNodes);
        return sourceQuery;
    }

    protected abstract SqlNode getSqlNode(EntityField field, SqlNode node);
}
