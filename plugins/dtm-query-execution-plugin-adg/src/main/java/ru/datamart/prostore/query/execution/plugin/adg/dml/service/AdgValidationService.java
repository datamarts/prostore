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
package ru.datamart.prostore.query.execution.plugin.adg.dml.service;

import ru.datamart.prostore.common.dto.QueryParserResponse;
import ru.datamart.prostore.query.execution.plugin.adg.base.exception.DtmTarantoolException;
import ru.datamart.prostore.query.execution.plugin.api.service.LlrValidationService;
import org.apache.calcite.rel.RelHomogeneousShuttle;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.JoinRelType;
import org.apache.calcite.rel.logical.LogicalJoin;
import org.springframework.stereotype.Service;

import java.util.HashSet;
import java.util.Set;

@Service("adgValidationService")
public class AdgValidationService implements LlrValidationService {

    @Override
    public void validate(QueryParserResponse queryParserResponse) {
        Set<JoinRelType> joinTypes = new HashSet<>();
        queryParserResponse.getRelNode().project().accept(new RelHomogeneousShuttle(){
            @Override
            public RelNode visit(LogicalJoin join) {
                joinTypes.add(join.getJoinType());
                return super.visit(join);
            }
        });

        if (joinTypes.contains(JoinRelType.FULL)) {
            throw new DtmTarantoolException("Tarantool does not support FULL and FULL OUTER JOINs");
        }
    }
}
