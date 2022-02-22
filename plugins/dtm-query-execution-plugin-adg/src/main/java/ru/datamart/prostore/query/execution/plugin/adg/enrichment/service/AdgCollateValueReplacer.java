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
package ru.datamart.prostore.query.execution.plugin.adg.enrichment.service;

import ru.datamart.prostore.query.calcite.core.node.SqlPredicatePart;
import ru.datamart.prostore.query.calcite.core.node.SqlPredicates;
import ru.datamart.prostore.query.calcite.core.node.SqlSelectTree;
import ru.datamart.prostore.query.calcite.core.node.SqlTreeNode;
import lombok.val;
import org.apache.calcite.sql.*;
import org.springframework.stereotype.Service;

@Service
public class AdgCollateValueReplacer {

    private static final String COLLATE_OPERATOR = "COLLATE";
    private static final SqlPredicates LITERAL = SqlPredicates.builder()
            .anyOf(SqlPredicatePart.eq(SqlKind.LITERAL))
            .build();

    public SqlNode replace(SqlNode sqlNode) {
        val selectTree = new SqlSelectTree(sqlNode);
        selectTree.findNodes(LITERAL, true)
                .forEach(sqlTreeNode -> replaceValue(sqlTreeNode, selectTree));
        return selectTree.getRoot().getNode();
    }

    private void replaceValue(SqlTreeNode sqlTreeNode, SqlSelectTree selectTree) {
        val node = sqlTreeNode.getNode();
        if (node instanceof SqlCharStringLiteral) {
            val parent = selectTree.getParentByChild(sqlTreeNode).orElse(null);
            if (parent != null && isCollate(parent.getNode())) {
                val secondParentOperand = selectTree.getChildNodesMap().get(parent.getId()).get(1);
                if (secondParentOperand.equals(sqlTreeNode)) {
                    val newValue = ((SqlCharStringLiteral) node).getNlsString().getValue().replace("'", "");
                    sqlTreeNode.getSqlNodeSetter().accept(new SqlIdentifier(newValue, node.getParserPosition()));
                }
            }
        }
    }

    private boolean isCollate(SqlNode operand) {
        if (!(operand instanceof SqlBasicCall))
            return false;
        SqlBasicCall basicCall = (SqlBasicCall) operand;
        return COLLATE_OPERATOR.equals(basicCall.getOperator().getName());
    }
}
