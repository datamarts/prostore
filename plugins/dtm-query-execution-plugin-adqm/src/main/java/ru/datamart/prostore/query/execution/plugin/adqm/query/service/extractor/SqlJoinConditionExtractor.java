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
package ru.datamart.prostore.query.execution.plugin.adqm.query.service.extractor;

import ru.datamart.prostore.query.execution.plugin.adqm.query.dto.AdqmJoinQuery;
import lombok.extern.slf4j.Slf4j;
import lombok.val;
import org.apache.calcite.rel.RelHomogeneousShuttle;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.JoinInfo;
import org.apache.calcite.rel.logical.LogicalJoin;
import org.apache.calcite.rel.logical.LogicalProject;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexShuttle;
import org.springframework.stereotype.Service;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

@Service
@Slf4j
public class SqlJoinConditionExtractor {

    public List<AdqmJoinQuery> extract(RelNode relNode) {
        List<AdqmJoinQuery> joins = new ArrayList<>();
        getLogicalJoins(relNode, joins);
        return joins;
    }

    private void getLogicalJoins(RelNode relNode, List<AdqmJoinQuery> joins) {
        relNode.accept(new RelHomogeneousShuttle() {
            @Override
            public RelNode visit(LogicalJoin join) {
                JoinInfo joinInfo = join.analyzeCondition();
                boolean hasNonEquidConditions = !joinInfo.nonEquiConditions.isEmpty();

                List<List<String>> leftConditionParts = joinInfo.leftKeys.stream()
                        .map(index -> getFieldName(index, join.getLeft()))
                        .collect(Collectors.toList());

                List<List<String>> rightConditionParts = joinInfo.rightKeys.stream()
                        .map(index -> getFieldName(index, join.getRight()))
                        .collect(Collectors.toList());

                joins.add(new AdqmJoinQuery(join.getLeft(),
                        join.getRight(),
                        leftConditionParts,
                        rightConditionParts,
                        hasNonEquidConditions));
                return super.visit(join);
            }
        });

    }

    private List<String> getFieldName(Integer index, RelNode relNode) {
        if (relNode instanceof LogicalProject) {
            val logicalProject = (LogicalProject) relNode;
            val inputFieldList = logicalProject.getInput().getRowType().getFieldList();
            val rexNode = logicalProject.getChildExps().get(index);
            if (rexNode instanceof RexInputRef) {
                int inputRefIndex = ((RexInputRef) rexNode).getIndex();
                return Collections.singletonList(inputFieldList.get(inputRefIndex).getName());
            }

            val usedInputColumnsIndexes = new ArrayList<Integer>();
            rexNode.accept(new RexShuttle() {
                @Override
                public RexNode visitInputRef(RexInputRef inputRef) {
                    usedInputColumnsIndexes.add(inputRef.getIndex());
                    return inputRef;
                }
            });

            return usedInputColumnsIndexes.stream()
                    .map(inputColumnIndex -> inputFieldList.get(inputColumnIndex).getName())
                    .collect(Collectors.toList());
        }

        val fieldList = relNode.getRowType().getFieldList();
        val fieldName = fieldList.get(index).getName();
        return Collections.singletonList(fieldName);
    }
}
