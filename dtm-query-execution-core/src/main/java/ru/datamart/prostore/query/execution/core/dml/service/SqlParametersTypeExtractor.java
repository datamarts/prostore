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

import org.apache.calcite.rel.RelHomogeneousShuttle;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rex.RexDynamicParam;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexShuttle;
import org.apache.calcite.rex.RexSubQuery;
import org.apache.calcite.sql.type.SqlTypeName;
import org.springframework.stereotype.Service;

import java.util.Comparator;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

@Service
public class SqlParametersTypeExtractor {

    public List<SqlTypeName> extract(RelNode relNode) {
        Set<RexDynamicParam> dynamicParams = new HashSet<>();
        getParameterTypes(relNode, dynamicParams);
        return dynamicParams.stream()
                .sorted(Comparator.comparing(RexDynamicParam::getIndex))
                .map(rn -> rn.getType().getSqlTypeName())
                .collect(Collectors.toList());
    }

    private void getParameterTypes(RelNode relNode, Set<RexDynamicParam> dynamicParams) {
        relNode.accept(new RelHomogeneousShuttle() {
            @Override
            protected RelNode visitChild(RelNode parent, int i, RelNode child) {
                visitRexNodes(child, dynamicParams);
                return super.visitChild(parent, i, child);
            }
        });

        visitRexNodes(relNode, dynamicParams);
    }

    private void visitRexNodes(RelNode relNode, Set<RexDynamicParam> dynamicParams) {
        relNode.accept(new RexShuttle() {
            @Override
            public RexNode visitSubQuery(RexSubQuery subQuery) {
                getParameterTypes(subQuery.rel, dynamicParams);
                return super.visitSubQuery(subQuery);
            }

            @Override
            public RexNode visitDynamicParam(RexDynamicParam dynamicParam) {
                dynamicParams.add(dynamicParam);
                return super.visitDynamicParam(dynamicParam);
            }
        });
    }
}
