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
package ru.datamart.prostore.query.calcite.core.service.impl;

import ru.datamart.prostore.common.exception.DtmException;
import ru.datamart.prostore.common.reader.QueryTemplateResult;
import ru.datamart.prostore.query.calcite.core.node.SqlPredicatePart;
import ru.datamart.prostore.query.calcite.core.node.SqlPredicates;
import ru.datamart.prostore.query.calcite.core.node.SqlSelectTree;
import ru.datamart.prostore.query.calcite.core.node.SqlTreeNode;
import ru.datamart.prostore.query.calcite.core.service.DefinitionService;
import ru.datamart.prostore.query.calcite.core.service.QueryTemplateExtractor;
import ru.datamart.prostore.query.calcite.core.util.SqlNodeUtil;
import lombok.val;
import org.apache.calcite.sql.*;
import org.apache.calcite.sql.parser.SqlParserPos;

import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public abstract class AbstractQueryTemplateExtractor implements QueryTemplateExtractor {
    public static final SqlPredicates DYNAMIC_PARAM_PREDICATE = SqlPredicates.builder()
            .anyOf(SqlPredicatePart.eq(SqlKind.DYNAMIC_PARAM))
            .build();
    private static final SqlDynamicParam DYNAMIC_PARAM = new SqlDynamicParam(0, SqlParserPos.QUOTED_ZERO);
    private static final SqlPredicates SET_DYNAMIC_PARAM = SqlPredicates.builder()
            .anyOf(
                    SqlPredicatePart.eq(SqlKind.LITERAL),
                    SqlPredicatePart.eq(SqlKind.DYNAMIC_PARAM)
            )
            .build();
    private static final Set<SqlKind> ALLOWED_PARENT_KINDS = EnumSet.of(
            SqlKind.LIKE,
            SqlKind.EQUALS,
            SqlKind.NOT_EQUALS,
            SqlKind.PERIOD_EQUALS,
            SqlKind.LESS_THAN_OR_EQUAL,
            SqlKind.LESS_THAN,
            SqlKind.GREATER_THAN_OR_EQUAL,
            SqlKind.GREATER_THAN,
            SqlKind.BETWEEN,
            SqlKind.IN,
            SqlKind.NOT_IN,
            SqlKind.ORDER_BY
    );
    private static final String COLLATE_OPERATOR = "COLLATE";
    private final DefinitionService<SqlNode> definitionService;
    private final SqlDialect sqlDialect;

    protected AbstractQueryTemplateExtractor(DefinitionService<SqlNode> definitionService, SqlDialect sqlDialect) {
        this.definitionService = definitionService;
        this.sqlDialect = sqlDialect;
    }

    @Override
    public QueryTemplateResult extract(String sql) {
        return extract(definitionService.processingQuery(sql));
    }

    @Override
    public QueryTemplateResult extract(String sql, List<String> excludeColumns) {
        return extract(definitionService.processingQuery(sql), excludeColumns);
    }

    @Override
    public SqlNode enrichTemplate(SqlNode templateNode, List<SqlNode> params) {
        //TODO perhaps it will be better to move method of enriching query template to separate interface
        // and implement it in different plugin classes
        SqlSelectTree selectTree = new SqlSelectTree(SqlNodeUtil.copy(templateNode));
        List<SqlTreeNode> dynamicNodes = selectTree.findNodes(DYNAMIC_PARAM_PREDICATE, false);

        Iterator<SqlNode> paramIterator = params.iterator();
        for (SqlTreeNode dynamicNode : dynamicNodes) {
            SqlNode param;
            if (!paramIterator.hasNext()) {
                paramIterator = params.iterator();
            }
            param = paramIterator.next();
            dynamicNode.getSqlNodeSetter().accept(param);
        }
        if (paramIterator.hasNext()) {
            throw new DtmException("The number of passed parameters and parameters in the template does not match");
        } else {
            return selectTree.getRoot().getNode();
        }
    }

    @Override
    public QueryTemplateResult extract(SqlNode sqlNode) {
        return extract(sqlNode, Collections.emptyList());
    }

    @Override
    public QueryTemplateResult extract(SqlNode sqlNode, List<String> excludeColumns) {
        SqlSelectTree selectTree = new SqlSelectTree(sqlNode);
        List<SqlNode> params = setDynamicParams(excludeColumns, selectTree);
        SqlNode resultTemplateNode = selectTree.getRoot().getNode();
        return new QueryTemplateResult(
                resultTemplateNode
                        .toSqlString(sqlDialect).toString(),
                resultTemplateNode,
                params
        );
    }

    private List<SqlNode> setDynamicParams(List<String> excludeList, SqlSelectTree selectTree) {
        if (excludeList.isEmpty()) {
            return selectTree.findNodes(SET_DYNAMIC_PARAM, false).stream()
                    .flatMap(sqlTreeNode -> replace(sqlTreeNode, selectTree))
                    .collect(Collectors.toList());
        } else {
            return selectTree.findNodes(SET_DYNAMIC_PARAM, false).stream()
                    .flatMap(sqlTreeNode -> replaceWithExclude(sqlTreeNode, selectTree, excludeList))
                    .collect(Collectors.toList());
        }
    }

    private Stream<SqlNode> replace(SqlTreeNode sqlTreeNode, SqlSelectTree selectTree) {
        val childSqlNode = sqlTreeNode.getNode();
        val parentTreeNode = selectTree.getParentByChild(sqlTreeNode);

        if (isValid(parentTreeNode, childSqlNode, selectTree)) {
            sqlTreeNode.getSqlNodeSetter().accept(DYNAMIC_PARAM);
            return Stream.of(childSqlNode);
        }

        return Stream.empty();
    }

    private boolean isValid(SqlTreeNode parentTreeNode, SqlNode child, SqlSelectTree selectTree) {
        if (parentTreeNode == null) {
            return false;
        }

        if (child instanceof SqlDynamicParam) {
            return true;
        }

        val parentSqlNode = parentTreeNode.getNode();
        if (isCollate(parentSqlNode)) {
            return child == ((SqlBasicCall) parentSqlNode).getOperands()[0];
        }

        if (parentSqlNode instanceof SqlNodeList) {
            val parentByChild = selectTree.getParentByChild(parentTreeNode);
            if (parentByChild != null) {
                val node = parentByChild.getNode();
                // order by can't be dynamic parameter
                if (node instanceof SqlOrderBy && parentSqlNode == ((SqlOrderBy) node).orderList) {
                    return false;
                }
                val kind = node.getKind();
                return ALLOWED_PARENT_KINDS.contains(kind);
            }

            return false;
        }

        if (parentSqlNode instanceof SqlBasicCall) {
            val parentOperands = ((SqlBasicCall) parentSqlNode).getOperandList();
            // left operand can't be dynamic parameter
            if (parentOperands.get(0) == child) {
                return false;
            }
        }

        if (parentSqlNode instanceof SqlOrderBy) {
            // only topmost order by can be templated
            return parentTreeNode.getKindPath().size() == 1;
        }

        return ALLOWED_PARENT_KINDS.contains(parentSqlNode.getKind());
    }

    private boolean isCollate(SqlNode operand) {
        if (!(operand instanceof SqlBasicCall))
            return false;
        SqlBasicCall basicCall = (SqlBasicCall) operand;
        return COLLATE_OPERATOR.equals(basicCall.getOperator().getName());
    }

    private Stream<SqlNode> replaceWithExclude(SqlTreeNode sqlTreeNode, SqlSelectTree selectTree, List<String> excludeList) {
        val sqlNode = sqlTreeNode.getNode();
        val parentTreeNode = selectTree.getParentByChild(sqlTreeNode);

        if (isValid(parentTreeNode, sqlNode, selectTree)) {
            val parentSqlNode = parentTreeNode.getNode();
            if (parentSqlNode instanceof SqlBasicCall) {
                val sqlBasicCall = (SqlBasicCall) parentSqlNode;
                if (sqlBasicCall.getOperands().length == 2) {
                    val leftOperand = sqlBasicCall.getOperands()[0];
                    val rightOperand = sqlBasicCall.getOperands()[1];
                    if (leftOperand == sqlNode && isNotExclude(rightOperand, excludeList) ||
                            rightOperand == sqlNode && isNotExclude(leftOperand, excludeList)) {
                        sqlTreeNode.getSqlNodeSetter().accept(DYNAMIC_PARAM);
                        return Stream.of(sqlNode);
                    }
                }
            } else {
                sqlTreeNode.getSqlNodeSetter().accept(DYNAMIC_PARAM);
                return Stream.of(sqlNode);
            }
        }
        return Stream.empty();
    }

    private boolean isNotExclude(SqlNode operand, List<String> excludeList) {
        if (excludeList.isEmpty()) {
            return true;
        }

        if (operand instanceof SqlIdentifier) {
            return isNotExcludedIdentifier((SqlIdentifier) operand, excludeList);
        }

        if (operand instanceof SqlBasicCall) {
            return ((SqlBasicCall) operand).getOperandList().stream()
                    .filter(SqlIdentifier.class::isInstance)
                    .allMatch(sqlNode -> isNotExcludedIdentifier((SqlIdentifier) sqlNode, excludeList));
        }

        return true;
    }

    private boolean isNotExcludedIdentifier(SqlIdentifier identifier, List<String> excludeList) {
        val columnName = identifier.isSimple() ? identifier.getSimple() : identifier.names.get(1);
        return excludeList.stream()
                .noneMatch(e -> e.equalsIgnoreCase(columnName));
    }

}
