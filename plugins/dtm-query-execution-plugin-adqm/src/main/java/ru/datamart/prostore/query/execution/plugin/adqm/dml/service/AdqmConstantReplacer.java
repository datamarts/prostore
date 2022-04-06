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
package ru.datamart.prostore.query.execution.plugin.adqm.dml.service;

import lombok.val;
import org.apache.calcite.sql.SqlLiteral;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.type.SqlTypeName;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Service;
import ru.datamart.prostore.common.model.ddl.EntityField;
import ru.datamart.prostore.query.execution.plugin.api.dml.AbstractConstantReplacer;
import ru.datamart.prostore.query.execution.plugin.api.service.PluginSpecificLiteralConverter;

@Service("adqmConstantReplacer")
public class AdqmConstantReplacer extends AbstractConstantReplacer {
    private final PluginSpecificLiteralConverter pluginSpecificLiteralConverter;

    public AdqmConstantReplacer(@Qualifier("adqmPluginSpecificLiteralConverter") PluginSpecificLiteralConverter pluginSpecificLiteralConverter) {
        this.pluginSpecificLiteralConverter = pluginSpecificLiteralConverter;
    }

    @Override
    protected SqlNode getSqlNode(EntityField field, SqlNode node) {
        val type = field.getType();
        if (node instanceof SqlLiteral) {
            switch (type) {
                case BOOLEAN:
                    return pluginSpecificLiteralConverter.convert(node, SqlTypeName.BOOLEAN);
                case DATE:
                    return pluginSpecificLiteralConverter.convert(node, SqlTypeName.DATE);
                case TIME:
                    return pluginSpecificLiteralConverter.convert(node, SqlTypeName.TIME);
                case TIMESTAMP:
                    return pluginSpecificLiteralConverter.convert(node, SqlTypeName.TIMESTAMP);
                default: break;
            }
        }
        return node;
    }
}