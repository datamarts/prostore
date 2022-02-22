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
package ru.datamart.prostore.query.execution.plugin.adqm.query.service;

import ru.datamart.prostore.common.exception.DtmException;
import ru.datamart.prostore.query.calcite.core.node.SqlSelectTree;
import ru.datamart.prostore.query.calcite.core.node.SqlTreeNode;
import ru.datamart.prostore.query.calcite.core.service.DefinitionService;
import ru.datamart.prostore.query.calcite.core.service.impl.AbstractQueryTemplateExtractor;
import ru.datamart.prostore.query.calcite.core.util.SqlNodeUtil;
import lombok.val;
import org.apache.calcite.sql.SqlDialect;
import org.apache.calcite.sql.SqlNode;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Service;

import java.util.Iterator;
import java.util.List;

@Service("adqmQueryTemplateExtractor")
public class AdqmQueryTemplateExtractor extends AbstractQueryTemplateExtractor {

    @Autowired
    public AdqmQueryTemplateExtractor(@Qualifier("adqmCalciteDefinitionService") DefinitionService<SqlNode> definitionService,
                                      @Qualifier("adqmSqlDialect") SqlDialect sqlDialect) {
        super(definitionService, sqlDialect);
    }

    @Override
    public SqlNode enrichTemplate(SqlNode templateNode, List<SqlNode> params) {
        SqlNode sqlNode = SqlNodeUtil.copy(templateNode);
        SqlSelectTree selectTree = new SqlSelectTree(sqlNode);

        List<SqlTreeNode> dynamicNodes = selectTree.findNodes(DYNAMIC_PARAM_PREDICATE, true);
        // parameters encounter twice
        if (dynamicNodes.size() != params.size() * 2) {
            throw new DtmException("The number of passed parameters and parameters in the template does not match");
        }

        Iterator<SqlNode> paramIterator = params.subList(0, params.size()).iterator();
        for (final SqlTreeNode dynamicNode : dynamicNodes) {
            SqlNode param;
            if (!paramIterator.hasNext()) {
                paramIterator = params.iterator();
            }
            param = paramIterator.next();
            dynamicNode.getSqlNodeSetter().accept(param);
        }

        return selectTree.getRoot().getNode();
    }

    public SqlNode enrichConditionTemplate(SqlNode templateNode, List<SqlNode> params) {
        val sqlNode = SqlNodeUtil.copy(templateNode);
        val selectTree = new SqlSelectTree(sqlNode);

        val dynamicNodes = selectTree.findNodes(DYNAMIC_PARAM_PREDICATE, true);
        // parameters encounter twice excluding fetch/offset ones
        if (dynamicNodes.size() != params.size()) {
            throw new DtmException("The number of passed parameters and parameters in the template does not match");
        }

        Iterator<SqlNode> paramIterator = params.iterator();
        for (final SqlTreeNode dynamicNode : dynamicNodes) {
            SqlNode param;
            if (!paramIterator.hasNext()) {
                paramIterator = params.iterator();
            }
            param = paramIterator.next();
            dynamicNode.getSqlNodeSetter().accept(param);
        }

        return selectTree.getRoot().getNode();
    }
}
