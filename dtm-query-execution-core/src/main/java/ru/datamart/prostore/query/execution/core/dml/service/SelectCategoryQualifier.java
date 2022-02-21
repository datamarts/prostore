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
package ru.datamart.prostore.query.execution.core.dml.service;

import ru.datamart.prostore.common.dml.SelectCategory;
import ru.datamart.prostore.common.model.ddl.EntityField;
import ru.datamart.prostore.query.execution.core.base.exception.table.ValidationDtmException;
import ru.datamart.prostore.query.execution.model.metadata.Datamart;
import lombok.val;
import org.apache.calcite.sql.*;
import org.springframework.stereotype.Component;

import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static ru.datamart.prostore.query.calcite.core.util.SqlNodeUtil.containsAggregates;

@Component
public class SelectCategoryQualifier {

    public SelectCategory qualify(List<Datamart> schema, SqlNode query) {
        SqlSelect sqlSelect;
        if (query instanceof SqlOrderBy) {
            val sqlOrderBy = (SqlOrderBy) query;
            if (sqlOrderBy.query instanceof SqlSelect) {
                sqlSelect = (SqlSelect) sqlOrderBy.query;
            } else {
                return SelectCategory.UNDEFINED;
            }
        } else if (query instanceof SqlSelect) {
            sqlSelect = (SqlSelect) query;
        } else {
            return SelectCategory.UNDEFINED;
        }

        if (checkSelectForSubquery(sqlSelect)) {
            throw new ValidationDtmException("Unsupported subqueries in SELECT clause");
        }
        if (isRelational(sqlSelect))
            return SelectCategory.RELATIONAL;
        if (isAnalytical(sqlSelect))
            return SelectCategory.ANALYTICAL;
        if (isDictionary(schema, sqlSelect))
            return SelectCategory.DICTIONARY;
        return SelectCategory.UNDEFINED;
    }

    private boolean isRelational(SqlSelect query) {
        if (query.getFrom().getKind().equals(SqlKind.JOIN)) {
            return true;
        }
        if (query.hasWhere()) {
            return checkWhereForSubquery((SqlBasicCall) query.getWhere());
        }
        return false;
    }

    private boolean checkSelectForSubquery(SqlSelect query) {
        return query.getSelectList().getList().stream()
                .anyMatch(sqlNode -> sqlNode instanceof SqlSelect || sqlNode instanceof SqlOrderBy);
    }

    private boolean checkWhereForSubquery(SqlBasicCall whereNode) {
        Queue<SqlNode> queue = new LinkedList<>();
        queue.add(whereNode);
        while (!queue.isEmpty()) {
            SqlNode childNode = queue.poll();
            if (childNode instanceof SqlSelect) {
                return true;
            } else if (childNode instanceof SqlBasicCall) {
                queue.addAll(((SqlBasicCall) childNode).getOperandList());
            }
        }
        return false;
    }

    private boolean isAnalytical(SqlSelect query) {
        return query.getGroup() != null || containsAggregates(query);
    }

    private boolean isDictionary(List<Datamart> schema, SqlSelect query) {
        if (query.hasWhere()) {
            List<String> primaryKeys = schema.get(0).getEntities().get(0).getFields().stream()
                    .filter(field -> field.getPrimaryOrder() != null)
                    .map(EntityField::getName)
                    .collect(Collectors.toList());
            return containsPrimaryKey(primaryKeys, query.getWhere());
        }
        return false;
    }

    private boolean containsPrimaryKey(List<String> primaryKeys, SqlNode where) {
        List<List<String>> identifiersGroups = new ArrayList<>();
        Queue<List<SqlNode>> queue = new LinkedList<>();
        queue.add(Collections.singletonList(where));
        while (!queue.isEmpty()) {
            val nodes = queue.poll();
            if (isAllIdentifier(nodes)) {
                identifiersGroups.add(nodes.stream()
                        .map(node -> {
                            val conditionIdentifier = (SqlIdentifier) node;
                            return conditionIdentifier.names.get(conditionIdentifier.names.size() - 1);
                        }).collect(Collectors.toList()));
            } else {
                processNodes(queue, nodes);
            }
        }
        return identifiersGroups.stream()
                .anyMatch(identifiersGroup -> identifiersGroup.containsAll(primaryKeys));
    }

    private boolean isAllIdentifier(List<SqlNode> nodes) {
        return nodes.stream().allMatch(SqlIdentifier.class::isInstance);
    }

    private void processNodes(Queue<List<SqlNode>> queue, List<SqlNode> nodes) {
        for (int i = 0; i < nodes.size(); i++) {
            if (nodes.get(i) instanceof SqlBasicCall) {
                val node = (SqlBasicCall) nodes.get(i);
                if (node.getKind().equals(SqlKind.OR)) {
                    processOrNode(node, nodes, i, queue);
                } else {
                    processOtherNode(node, nodes, i, queue);
                }
                break;
            }
        }
    }

    private void processOrNode(SqlBasicCall orNode, List<SqlNode> currentNodes, int idx, Queue<List<SqlNode>> queue) {
        List<SqlNode> nodeWithLeft = new ArrayList<>(currentNodes);
        List<SqlNode> nodeWithRight = new ArrayList<>(currentNodes);
        SqlNode leftNode = orNode.getOperandList().get(0);
        SqlNode rightNode = orNode.getOperandList().get(1);
        nodeWithLeft.set(idx, leftNode);
        nodeWithRight.set(idx, rightNode);
        queue.add(nodeWithLeft);
        queue.add(nodeWithRight);
    }

    private void processOtherNode(SqlBasicCall node, List<SqlNode> nodes, int idx, Queue<List<SqlNode>> queue) {
        val operands = node.getOperands();
        List<SqlNode> newNodes = new ArrayList<>(nodes);
        newNodes.remove(idx);
        newNodes.addAll(Stream.of(operands)
                .filter(n -> !(n instanceof SqlDynamicParam || n instanceof SqlNodeList || n instanceof SqlNumericLiteral))
                .collect(Collectors.toList()));
        queue.add(newNodes);
    }
}
