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
package ru.datamart.prostore.query.execution.core.dml.service;

import io.vertx.core.Future;
import lombok.extern.slf4j.Slf4j;
import lombok.val;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelRoot;
import org.apache.calcite.rel.core.Project;
import org.apache.calcite.rel.core.TableScan;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.rex.RexInputRef;
import org.apache.commons.lang3.StringUtils;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Service;
import ru.datamart.prostore.common.dto.QueryParserRequest;
import ru.datamart.prostore.common.model.ddl.ColumnType;
import ru.datamart.prostore.query.calcite.core.service.QueryParserService;
import ru.datamart.prostore.query.calcite.core.util.CalciteUtil;
import ru.datamart.prostore.query.execution.core.calcite.model.schema.CoreDtmTable;
import ru.datamart.prostore.query.execution.model.metadata.ColumnMetadata;

import java.util.Comparator;
import java.util.List;
import java.util.stream.Collectors;

@Slf4j
@Service
public class ColumnMetadataService {
    private final QueryParserService parserService;

    public ColumnMetadataService(@Qualifier("coreCalciteDMLQueryParserService") QueryParserService parserService) {
        this.parserService = parserService;
    }

    public Future<List<ColumnMetadata>> getColumnMetadata(QueryParserRequest request) {
        return parserService.parse(request)
                .map(response -> getColumnMetadataInner(response.getRelNode()));
    }

    public Future<List<ColumnMetadata>> getColumnMetadata(RelRoot relNode) {
        return Future.succeededFuture(getColumnMetadataInner(relNode));
    }

    private List<ColumnMetadata> getColumnMetadataInner(RelRoot relNode) {
        val project = relNode.project();
        return project.getRowType().getFieldList()
                .stream()
                .sorted(Comparator.comparing(RelDataTypeField::getIndex))
                .map(relColumn -> {
                    val columnType = getColumnType(project, relColumn);
                    return new ColumnMetadata(relColumn.getName(), columnType, getSize(relColumn), relColumn.getType().isNullable());
                })
                .collect(Collectors.toList());
    }

    private ColumnType getColumnType(RelNode relNode, RelDataTypeField relColumn) {
        val foundRealColumnType = findRootType(relNode, relColumn.getIndex());
        if (foundRealColumnType == null) {
            return CalciteUtil.toColumnType(relColumn.getType().getSqlTypeName());
        }

        val sqlRepresentationOfRealColumnType = CalciteUtil.valueOf(foundRealColumnType);

        val actualSqlType = relColumn.getType().getSqlTypeName();
        if (actualSqlType != sqlRepresentationOfRealColumnType) {
            return CalciteUtil.toColumnType(actualSqlType);
        }
        return foundRealColumnType;
    }

    private ColumnType findRootType(RelNode relNode, int rexIndex) {
        if (rexIndex < 0) {
            return null;
        }

        val input = relNode.getInputs();
        if (relNode instanceof Project) {
            val project = (Project) relNode;
            val rexNodes = project.getChildExps();
            if (rexIndex >= rexNodes.size()) {
                return null;
            }

            val rexNode = rexNodes.get(rexIndex);
            if (rexNode instanceof RexInputRef) {
                val name = ((RexInputRef) rexNode).getName();
                val inputIndex = Integer.parseInt(StringUtils.substringAfter(name, "$"));
                return traverseInputs(input, inputIndex);
            }

            return null;
        }

        if (relNode instanceof TableScan) {
            val tableScan = (TableScan) relNode;
            val table = tableScan.getTable().unwrap(CoreDtmTable.class);
            val fields = table.getEntity().getFields();
            if (rexIndex < fields.size()) {
                return fields.get(rexIndex).getType();
            }
        }

        return traverseInputs(input, rexIndex);
    }

    private ColumnType traverseInputs(List<RelNode> inputs, int inputIndex) {
        int movedRexNodes = 0;
        for (val node : inputs) {
            val rexIndex = inputIndex - movedRexNodes;
            val sqlType = findRootType(node, rexIndex);
            if (sqlType != null) {
                return sqlType;
            }

            movedRexNodes += node.getRowType().getFieldCount();
        }

        return null;
    }

    private ColumnType getType(RelDataType type) {
        return CalciteUtil.toColumnType(type.getSqlTypeName());
    }

    private Integer getSize(RelDataTypeField field) {
        ColumnType type = getType(field.getType());
        switch (type) {
            case VARCHAR:
            case CHAR:
            case UUID:
            case TIME:
            case TIMESTAMP:
                return field.getValue().getPrecision();
            default:
                return null;
        }
    }
}
