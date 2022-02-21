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

import ru.datamart.prostore.common.dml.ShardingCategory;
import ru.datamart.prostore.common.model.ddl.ColumnType;
import ru.datamart.prostore.common.model.ddl.Entity;
import ru.datamart.prostore.common.model.ddl.EntityField;
import ru.datamart.prostore.common.model.ddl.EntityFieldUtils;
import ru.datamart.prostore.query.execution.model.metadata.Datamart;
import lombok.AllArgsConstructor;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import lombok.val;
import org.apache.calcite.sql.*;
import org.apache.calcite.sql.fun.SqlBetweenOperator;
import org.apache.calcite.sql.fun.SqlInOperator;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.springframework.stereotype.Component;

import java.util.*;
import java.util.stream.Collectors;

import static ru.datamart.prostore.query.calcite.core.util.SqlNodeTemplates.basicCall;
import static ru.datamart.prostore.query.calcite.core.util.SqlNodeTemplates.identifier;

@Component
@Slf4j
public class ShardingCategoryQualifier {

    private static final SqlLiteral NULL_LITERAL = SqlLiteral.createNull(SqlParserPos.ZERO);
    private static final SqlLiteral TRUE_LITERAL = SqlLiteral.createBoolean(true, SqlParserPos.ZERO);
    private static final SqlNumericLiteral NUMERIC_LITERAL = SqlLiteral.createExactNumeric("1", SqlParserPos.ZERO);
    private static final SqlNode IN_VALUES_REPLACER = SqlNodeList.of(NUMERIC_LITERAL, NUMERIC_LITERAL);

    public ShardingCategory qualify(List<Datamart> schema, SqlNode query) {
        try {
            SqlSelect sqlSelect;
            if (query instanceof SqlOrderBy) {
                val sqlOrderBy = (SqlOrderBy) query;
                if (sqlOrderBy.query instanceof SqlSelect) {
                    sqlSelect = (SqlSelect) sqlOrderBy.query;
                } else {
                    return ShardingCategory.SHARD_ALL;
                }
            } else if (query instanceof SqlSelect) {
                sqlSelect = (SqlSelect) query;
            } else {
                return ShardingCategory.SHARD_ALL;
            }

            if (sqlSelect.hasWhere()) {
                val entities = getAllEntities(schema);
                val shardingKeyList = EntityFieldUtils.getShardingKeyList(entities.stream()
                        .flatMap(entity -> entity.getFields().stream())
                        .collect(Collectors.toList()));
                List<List<SqlBasicCall>> conditionGroups = getConditionGroups(sqlSelect.getWhere(), shardingKeyList);
                conditionGroups = conditionGroups.stream()
                        .map(conditionGroup -> removeNonShardingKeyConditions(conditionGroup, shardingKeyList))
                        .collect(Collectors.toList());
                conditionGroups = replaceSingleInAndIsNullTrueClauses(conditionGroups);
                if (sqlSelect.getFrom() instanceof SqlJoin) {
                    val joinCondition = ((SqlJoin) sqlSelect.getFrom()).getCondition();
                    val joinConditionGroups = getConditionGroups(joinCondition, shardingKeyList);
                    conditionGroups = addJoinCondition(conditionGroups, joinConditionGroups);
                }
                List<ShardingCategory> groupsCategories = calculateGroupsCategories(conditionGroups, shardingKeyList, entities);
                Set<ShardingCategory> categorySet = new HashSet<>(groupsCategories);
                if (categorySet.contains(ShardingCategory.SHARD_ALL)) {
                    return ShardingCategory.SHARD_ALL;
                }
                if (categorySet.contains(ShardingCategory.SHARD_SET)) {
                    return ShardingCategory.SHARD_SET;
                }
                if (groupsCategories.stream().filter(category -> category.equals(ShardingCategory.SHARD_ONE)).count() == 1) {
                    return ShardingCategory.SHARD_ONE;
                } else {
                    return ShardingCategory.SHARD_SET;
                }
            }
            return ShardingCategory.SHARD_ALL;
        } catch (Exception e) {
            log.error("Error during defining sharding category", e);
            return ShardingCategory.SHARD_ALL;
        }
    }

    private List<List<SqlBasicCall>> addJoinCondition(List<List<SqlBasicCall>> conditionGroups, List<List<SqlBasicCall>> joinConditionGroups) {
        List<List<SqlBasicCall>> newCondGroups = new ArrayList<>();
        for (List<SqlBasicCall> conditionGroup : conditionGroups) {
            for (List<SqlBasicCall> joinConditionGroup : joinConditionGroups) {
                List<SqlBasicCall> newCondGroup = new ArrayList<>(conditionGroup);
                for (SqlBasicCall joinCondition : joinConditionGroup) {
                    for (SqlBasicCall condition : conditionGroup) {
                        val leftName = getIdentifier(joinCondition.getOperandList().get(0));
                        val rightName = getIdentifier(joinCondition.getOperandList().get(1));
                        if (condition.getOperator().getKind().equals(SqlKind.EQUALS)) {
                            val identifier = getIdentifierName(condition);
                            if (leftName.equals(identifier)) {
                                newCondGroup.add(basicCall(condition.getOperator(), identifier(rightName), condition.getOperandList().get(1)));
                            }
                            if (rightName.equals(identifier)) {
                                newCondGroup.add(basicCall(condition.getOperator(), identifier(leftName), condition.getOperandList().get(1)));
                            }
                        }
                    }

                }
                newCondGroups.add(newCondGroup);
            }
        }

        return newCondGroups;
    }

    private String getIdentifier(SqlNode node) {
        val identifier = (SqlIdentifier) node;
        return identifier.isSimple() ? identifier.getSimple() : identifier.names.get(1);
    }

    private List<ShardingCategory> calculateGroupsCategories(List<List<SqlBasicCall>> conditionGroups, List<EntityField> shardingKeyList, List<Entity> entities) {
        return conditionGroups.stream()
                .map(conditionGroup -> calculateGroupCategory(conditionGroup, shardingKeyList, entities))
                .collect(Collectors.toList());
    }

    @Getter
    @AllArgsConstructor
    @EqualsAndHashCode
    private static class ValueTypeKey {
        private final String value;
        private final ColumnType type;
    }

    private ColumnType getColumnTypeByName(String name, List<EntityField> fields) {
        return fields.stream().filter(field -> field.getName().equalsIgnoreCase(name))
                .map(EntityField::getType)
                .findFirst()
                .orElse(ColumnType.ANY);
    }

    private String getColumnTable(String column, List<Entity> entities) {
        return entities.stream()
                .filter(entity -> entity.getFields().stream()
                        .map(EntityField::getName)
                        .map(String::toLowerCase)
                        .anyMatch(name -> name.equals(column)))
                .findFirst()
                .map(Entity::getName)
                .orElse("");
    }

    private ShardingCategory calculateGroupCategory(List<SqlBasicCall> conditionGroup, List<EntityField> shardingKeyList, List<Entity> entities) {
        val conditionIdetifiers = conditionGroup.stream()
                .map(this::getIdentifierName)
                .collect(Collectors.toList());
        if (!containsAllShardingKeys(shardingKeyList, conditionIdetifiers)) {
            return ShardingCategory.SHARD_ALL;
        }
        if (conditionGroup.isEmpty()) {
            return ShardingCategory.SHARD_ALL;
        }
        if (entities.size() > 1) {
            if (isEachConditionHasPair(conditionGroup, entities, shardingKeyList)) {
                return ShardingCategory.SHARD_ONE;
            }
            return ShardingCategory.SHARD_SET;
        } else {
            if (isAllEqualsCondition(conditionGroup)) {
                return ShardingCategory.SHARD_ONE;
            }
            if (hasInCondition(conditionGroup)) {
                return ShardingCategory.SHARD_SET;
            }
        }
        return ShardingCategory.SHARD_ALL;
    }

    private boolean containsAllShardingKeys(List<EntityField> shardingKeyList, List<String> conditionIdetifiers) {
        return conditionIdetifiers.containsAll(shardingKeyList.stream()
                .map(EntityField::getName)
                .map(String::toLowerCase)
                .collect(Collectors.toList()));
    }

    private boolean hasInCondition(List<SqlBasicCall> conditionGroup) {
        return conditionGroup.stream()
                .map(SqlBasicCall::getOperator)
                .anyMatch(sqlOperator -> sqlOperator.getKind().equals(SqlKind.IN));
    }

    private boolean isEachConditionHasPair(List<SqlBasicCall> conditionGroup,
                                           List<Entity> entities,
                                           List<EntityField> shardingKeyList) {
        Map<ValueTypeKey, List<String>> valueTypeIdentifiers = new HashMap<>();
        conditionGroup.forEach(node -> {
            String identifierName = getIdentifierName(node);
            val key = new ValueTypeKey(getValueString(node), getColumnTypeByName(identifierName, shardingKeyList));
            if (valueTypeIdentifiers.containsKey(key)) {
                valueTypeIdentifiers.get(key).add(identifierName);
            } else {
                List<String> valueList = new ArrayList<>();
                valueList.add(identifierName);
                valueTypeIdentifiers.put(key, valueList);
            }
        });
        return valueTypeIdentifiers.values().stream()
                .allMatch(list -> {
                    List<String> pairList = new ArrayList<>();
                    list.forEach(column1 -> {
                        val columnTable1 = getColumnTable(column1, entities);
                        list.forEach(column2 -> {
                            if (!column1.equals(column2)) {
                                val columnTable2 = getColumnTable(column2, entities);
                                if (!pairList.contains(column1) && !pairList.contains(column2) && !columnTable1.equals(columnTable2)) {
                                    pairList.add(column1);
                                    pairList.add(column2);
                                }
                            }
                        });
                    });
                    return pairList.size() == list.size();
                });
    }

    private boolean isAllEqualsCondition(List<SqlBasicCall> conditionGroup) {
        return conditionGroup.stream()
                .map(SqlBasicCall::getOperator)
                .allMatch(sqlOperator -> sqlOperator.getKind().equals(SqlKind.EQUALS));
    }

    private List<SqlBasicCall> removeNonShardingKeyConditions(List<SqlBasicCall> conditionGroup, List<EntityField> shardingKeyList) {
        val keysNames = shardingKeyList.stream()
                .map(EntityField::getName)
                .map(String::toLowerCase)
                .collect(Collectors.toList());
        return conditionGroup.stream()
                .filter(sqlNode -> keysNames.contains(getIdentifierName(sqlNode)))
                .collect(Collectors.toList());
    }

    private List<List<SqlBasicCall>> replaceSingleInAndIsNullTrueClauses(List<List<SqlBasicCall>> conditionGroups) {
        List<List<SqlBasicCall>> newConditionGroups = new ArrayList<>();
        conditionGroups.forEach(list -> {
            List<SqlBasicCall> newList = list.stream()
                    .map(node -> {
                        if (node.getOperator() instanceof SqlInOperator && node.getOperandList().get(1) instanceof SqlNodeList) {
                            SqlNodeList rightOperand = (SqlNodeList) node.getOperandList().get(1);
                            if (rightOperand.getList().size() == 1) {
                                return basicCall(SqlStdOperatorTable.EQUALS, node.getOperandList().get(0), rightOperand.get(0));
                            }
                        }
                        val kind = node.getOperator().getKind();
                        if (SqlKind.IS_NULL.equals(kind)) {
                            return basicCall(SqlStdOperatorTable.EQUALS, node.getOperandList().get(0), NULL_LITERAL);
                        }
                        if (SqlKind.IS_TRUE.equals(kind)) {
                            return basicCall(SqlStdOperatorTable.EQUALS, node.getOperandList().get(0), TRUE_LITERAL);
                        }
                        return node;
                    })
                    .collect(Collectors.toList());
            newConditionGroups.add(newList);
        });
        return newConditionGroups;
    }

    private String getIdentifierName(SqlBasicCall condition) {
        val leftOperand = (SqlIdentifier) condition.getOperandList().get(0);
        if (leftOperand.isSimple()) {
            return leftOperand.getSimple();
        }
        return leftOperand.names.get(1);
    }

    private String getValueString(SqlBasicCall condition) {
        val rightOperand = condition.getOperandList().get(1);
        return rightOperand.toString();
    }

    private boolean isLeafCondition(List<SqlNode> nodes) {
        return nodes.stream().allMatch(node -> isLeafCondition((SqlBasicCall) node));
    }

    private boolean isLeafCondition(SqlBasicCall sqlBasicCall) {
        if (sqlBasicCall.getOperator() instanceof SqlPostfixOperator) {
            return true;
        } else if (sqlBasicCall.getOperator() instanceof SqlInOperator) {
            return isLeafIn(sqlBasicCall);
        } else if (sqlBasicCall.getOperator() instanceof SqlBetweenOperator) {
            return isLeafBetween(sqlBasicCall);
        } else if (sqlBasicCall.getOperands().length == 2) {
            return isLeafBinary(sqlBasicCall);
        }
        return false;
    }

    private boolean isLeafIn(SqlBasicCall sqlBasicCall) {
        val leftOperand = sqlBasicCall.getOperandList().get(0);
        val rightOperand = sqlBasicCall.getOperandList().get(1);
        return leftOperand instanceof SqlIdentifier && rightOperand instanceof SqlNodeList;
    }

    private boolean isLeafBetween(SqlBasicCall sqlBasicCall) {
        val leftOperand = sqlBasicCall.getOperandList().get(0);
        val firstRightOperand = sqlBasicCall.getOperandList().get(1);
        val secondRightOperand = sqlBasicCall.getOperandList().get(2);
        return leftOperand instanceof SqlIdentifier && isValue(firstRightOperand) && isValue(secondRightOperand);
    }

    private boolean isLeafBinary(SqlBasicCall sqlBasicCall) {
        val leftOperand = sqlBasicCall.getOperandList().get(0);
        val rightOperand = sqlBasicCall.getOperandList().get(1);
        return ((leftOperand instanceof SqlIdentifier && isValue(rightOperand)) ||
                (isValue(leftOperand) || rightOperand instanceof SqlIdentifier));
    }

    private boolean isValue(SqlNode rightOperand) {
        return rightOperand.getKind() == SqlKind.DYNAMIC_PARAM || rightOperand instanceof SqlLiteral;
    }

    private List<List<SqlBasicCall>> getConditionGroups(SqlNode where, List<EntityField> shardingKeyList) {
        List<List<SqlBasicCall>> conditionGroups = new ArrayList<>();
        Queue<List<SqlNode>> queue = new LinkedList<>();
        queue.add(Collections.singletonList(where));
        while (!queue.isEmpty()) {
            val nodes = queue.poll();
            if (isLeafCondition(nodes)) {
                conditionGroups.add(nodes.stream()
                        .map(SqlBasicCall.class::cast)
                        .collect(Collectors.toList()));
            } else {
                processNodes(queue, nodes, shardingKeyList);
            }
        }
        return conditionGroups;
    }

    private void processNodes(Queue<List<SqlNode>> queue, List<SqlNode> nodes, List<EntityField> shardingKeyList) {
        for (int i = 0; i < nodes.size(); i++) {
            if (nodes.get(i) instanceof SqlBasicCall) {
                val node = (SqlBasicCall) nodes.get(i);
                if (node.getKind().equals(SqlKind.OR)) {
                    processOrNode(node, nodes, i, queue);
                } else {
                    processOtherNode(node, nodes, i, queue, shardingKeyList);
                }
                break;
            }
        }
    }

    private void processOrNode(SqlBasicCall orNode, List<SqlNode> currentNodes, int idx, Queue<List<SqlNode>> queue) {
        List<SqlNode> nodesWithLeft = new ArrayList<>(currentNodes);
        List<SqlNode> nodesWithRight = new ArrayList<>(currentNodes);
        SqlNode leftNode = orNode.getOperandList().get(0);
        SqlNode rightNode = orNode.getOperandList().get(1);
        nodesWithLeft.set(idx, leftNode);
        nodesWithRight.set(idx, rightNode);
        queue.add(nodesWithLeft);
        queue.add(nodesWithRight);
    }

    private void processOtherNode(SqlBasicCall node, List<SqlNode> nodes, int idx, Queue<List<SqlNode>> queue, List<EntityField> shardingKeyList) {
        val operands = node.getOperandList();
        if (node.getOperator().getKind().equals(SqlKind.IN) && operands.get(1) instanceof SqlSelect) {
            processInSubquery(node, nodes, idx, queue, operands, shardingKeyList);
        } else {
            List<SqlNode> newNodes = new ArrayList<>(operands);
            newNodes.addAll(nodes.subList(idx + 1, nodes.size()));
            queue.add(newNodes);
        }
    }

    private void processInSubquery(SqlBasicCall inNode,
                                   List<SqlNode> nodes,
                                   int idx,
                                   Queue<List<SqlNode>> queue,
                                   List<SqlNode> operands,
                                   List<EntityField> shardingKeyList) {
        List<SqlNode> newNodes = new ArrayList<>();
        val leftOperand = operands.get(0);
        if (leftOperand instanceof SqlIdentifier) {
            if (isShardingKey(getIdentifierName(inNode), shardingKeyList)) {
                val inReplacer = basicCall(SqlStdOperatorTable.IN, leftOperand, IN_VALUES_REPLACER);
                newNodes.add(inReplacer);
            }
        } else {
            val leftList = ((SqlBasicCall) leftOperand).getOperandList();
            leftList.forEach(node -> {
                val identifier = (SqlIdentifier) node;
                val columnName = identifier.isSimple() ? identifier.getSimple() : identifier.names.get(1);
                if (isShardingKey(columnName, shardingKeyList)) {
                    val inReplacer = basicCall(SqlStdOperatorTable.IN, identifier, IN_VALUES_REPLACER);
                    newNodes.add(inReplacer);
                }
            });
        }
        if (((SqlSelect) operands.get(1)).hasWhere()) {
            newNodes.add(((SqlSelect) operands.get(1)).getWhere());
        }
        newNodes.addAll(nodes.subList(idx + 1, nodes.size()));
        queue.add(newNodes);
    }

    private boolean isShardingKey(String columnName, List<EntityField> shardingKeyList) {
        return shardingKeyList.stream()
                .map(EntityField::getName)
                .map(String::toLowerCase)
                .anyMatch(keyName -> keyName.equals(columnName));
    }

    private List<Entity> getAllEntities(List<Datamart> schema) {
        return schema.stream()
                .flatMap(datamart -> datamart.getEntities().stream())
                .collect(Collectors.toList());
    }

}
