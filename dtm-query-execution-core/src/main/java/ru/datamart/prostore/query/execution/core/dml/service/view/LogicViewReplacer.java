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
package ru.datamart.prostore.query.execution.core.dml.service.view;

import ru.datamart.prostore.query.calcite.core.service.DefinitionService;
import io.vertx.core.Future;
import lombok.extern.slf4j.Slf4j;
import org.apache.calcite.sql.SqlNode;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Component;

@Slf4j
@Component("logicViewReplacer")
public class LogicViewReplacer implements ViewReplacer {
    private final DefinitionService<SqlNode> definitionService;

    @Autowired
    public LogicViewReplacer(@Qualifier("coreCalciteDefinitionService") DefinitionService<SqlNode> definitionService) {
        this.definitionService = definitionService;
    }

    @Override
    public Future<Void> replace(ViewReplaceContext context) {
        ViewReplacerService replacerService = context.getViewReplacerService();
        context.setViewQueryNode(definitionService.processingQuery(context.getEntity().getViewQuery()));
        return replacerService.replace(context);
    }
}


