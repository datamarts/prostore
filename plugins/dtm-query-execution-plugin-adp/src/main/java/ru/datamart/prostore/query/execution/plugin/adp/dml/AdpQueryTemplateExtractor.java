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
package ru.datamart.prostore.query.execution.plugin.adp.dml;

import ru.datamart.prostore.common.exception.DtmException;
import ru.datamart.prostore.query.calcite.core.extension.dml.SqlDynamicLiteral;
import ru.datamart.prostore.query.calcite.core.node.SqlSelectTree;
import ru.datamart.prostore.query.calcite.core.node.SqlTreeNode;
import ru.datamart.prostore.query.calcite.core.service.DefinitionService;
import ru.datamart.prostore.query.calcite.core.service.impl.AbstractQueryTemplateExtractor;
import ru.datamart.prostore.query.calcite.core.util.SqlNodeUtil;
import org.apache.calcite.sql.SqlDialect;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.type.SqlTypeName;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Service;

import java.util.Iterator;
import java.util.List;

@Service("adpQueryTemplateExtractor")
public class AdpQueryTemplateExtractor extends AbstractQueryTemplateExtractor {

    @Autowired
    public AdpQueryTemplateExtractor(@Qualifier("adpCalciteDefinitionService") DefinitionService<SqlNode> definitionService,
                                     @Qualifier("adpSqlDialect") SqlDialect sqlDialect) {
        super(definitionService, sqlDialect);
    }

    @Override
    public SqlNode enrichTemplate(SqlNode templateNode, List<SqlNode> params) {
        SqlSelectTree selectTree = new SqlSelectTree(SqlNodeUtil.copy(templateNode));
        List<SqlTreeNode> dynamicNodes = selectTree.findNodes(DYNAMIC_PARAM_PREDICATE, true);

        Iterator<SqlNode> paramIterator = params.iterator();
        int paramNum = 1;
        for (SqlTreeNode dynamicNode : dynamicNodes) {
            SqlNode param;
            if (!paramIterator.hasNext()) {
                paramIterator = params.iterator();
            }
            param = paramIterator.next();
            if (param.getKind() == SqlKind.DYNAMIC_PARAM) {
                param = new SqlDynamicLiteral(paramNum, SqlTypeName.ANY, param.getParserPosition());
                paramNum++;
            }
            dynamicNode.getSqlNodeSetter().accept(param);
        }
        if (paramIterator.hasNext()) {
            throw new DtmException("The number of passed parameters and parameters in the template does not match");
        } else {
            return selectTree.getRoot().getNode();
        }
    }
}
