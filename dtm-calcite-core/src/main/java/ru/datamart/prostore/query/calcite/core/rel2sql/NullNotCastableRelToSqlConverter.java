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
package ru.datamart.prostore.query.calcite.core.rel2sql;

import lombok.val;
import org.apache.calcite.linq4j.Ord;
import org.apache.calcite.linq4j.tree.Expressions;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.*;
import org.apache.calcite.rel.rel2sql.RelToSqlConverter;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.*;
import org.apache.calcite.sql.validate.SqlValidatorUtil;

import java.util.ArrayList;
import java.util.List;

public class NullNotCastableRelToSqlConverter extends RelToSqlConverter {
    private final boolean allowStarInProject;

    public NullNotCastableRelToSqlConverter(SqlDialect dialect, boolean allowStarInProject) {
        super(dialect);
        this.allowStarInProject = allowStarInProject;
    }

    @Override
    public Result visit(Project e) {
        e.getVariablesSet();
        val x = visitChild(0, e.getInput());
        parseCorrelationTable(e, x);

        if (allowStarInProject && !(e.getInput() instanceof Sort) && !(e.getInput() instanceof Join)
                && isStar(e.getChildExps(), e.getInput().getRowType(), e.getRowType())) {
            return x;
        }

        val builder = x.builder(e, Clause.SELECT);
        val selectList = new ArrayList<SqlNode>();
        for (RexNode ref : e.getChildExps()) {
            val sqlExpr = builder.context.toSql(null, ref);
            addSelect(selectList, sqlExpr, e.getRowType());
        }
        builder.setSelect(new SqlNodeList(selectList, POS));
        return builder.result();
    }

    @Override
    public Result visit(Filter e) {
        val visit = super.visit(e);
        parseCorrelationTable(e, visit);
        if (allowStarInProject) {
            return visit;
        }

        val builder = visit.builder(e, Clause.SELECT);
        val selectList = new ArrayList<SqlNode>();
        val rexBuilder = e.getCluster().getRexBuilder();
        for (int i = 0; i < e.getRowType().getFieldCount(); i++) {
            val columnRex = rexBuilder.makeInputRef(e, i);
            val sqlExpr = builder.context.toSql(null, columnRex);
            addSelect(selectList, sqlExpr, e.getRowType());
        }
        builder.setSelect(new SqlNodeList(selectList, POS));
        return builder.result();
    }

    @Override
    public Result setOpToSql(SqlSetOperator operator, RelNode rel) {
        if (operator.getKind() != SqlKind.UNION) {
            return super.setOpToSql(operator, rel);
        }

        SqlNode node = null;
        for (Ord<RelNode> input : Ord.zip(rel.getInputs())) {

            Result result;
            if (input.e instanceof Sort) {
                result = visitSort((Sort) input.e);
            } else {
                result = visitChild(input.i, input.e);
            }

            if (node == null) {
                node = result.asSelect();
            } else {
                node = operator.createCall(POS, node, result.asSelect());
            }
        }
        val clauses = Expressions.list(Clause.SET_OP);
        return result(node, clauses, rel, null);
    }

    private Result visitSort(Sort e) {
        val visit = super.visit(e);
        val builder = visit.builder(e, Clause.SELECT);
        if (allowStarInProject) {
            return builder.result();
        }

        val sqlNodes = builder.context.fieldList();
        builder.setSelect(new SqlNodeList(sqlNodes, POS));
        return builder.result();
    }

    private void parseCorrelationTable(RelNode relNode, Result x) {
        for (CorrelationId id : relNode.getVariablesSet()) {
            correlTableMap.put(id, x.qualifiedContext());
        }
    }

    @Override
    public void addSelect(List<SqlNode> selectList, SqlNode node, RelDataType rowType) {
        val name = rowType.getFieldNames().get(selectList.size());
        val alias = SqlValidatorUtil.getAlias(node, -1);
        if (alias == null || !alias.equals(name)) {
            selectList.add(as(node, name));
            return;
        }

        selectList.add(node);
    }

    @Override
    protected boolean isAnon() {
        return false;
    }
}
