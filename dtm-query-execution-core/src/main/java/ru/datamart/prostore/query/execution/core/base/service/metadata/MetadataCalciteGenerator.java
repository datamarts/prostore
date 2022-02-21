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
package ru.datamart.prostore.query.execution.core.base.service.metadata;

import ru.datamart.prostore.common.exception.DtmException;
import ru.datamart.prostore.common.model.ddl.ColumnType;
import ru.datamart.prostore.common.model.ddl.Entity;
import ru.datamart.prostore.common.model.ddl.EntityField;
import ru.datamart.prostore.query.calcite.core.extension.ddl.SqlCreateMaterializedView;
import ru.datamart.prostore.query.calcite.core.extension.ddl.SqlCreateTable;
import ru.datamart.prostore.query.calcite.core.extension.eddl.SqlNodeUtils;
import ru.datamart.prostore.query.execution.core.base.utils.ColumnTypeUtil;
import lombok.extern.slf4j.Slf4j;
import lombok.val;
import org.apache.calcite.sql.*;
import org.apache.calcite.sql.ddl.SqlColumnDeclaration;
import org.apache.calcite.sql.ddl.SqlKeyConstraint;
import org.apache.calcite.sql.type.SqlTypeName;
import org.jetbrains.annotations.NotNull;
import org.springframework.stereotype.Component;

import java.util.*;

@Component
@Slf4j
public class MetadataCalciteGenerator {
    private static final Integer DEFAULT_PRECISION = 6;

    public Entity generateTableMetadata(SqlCreate sqlCreate) {
        final List<String> names = SqlNodeUtils.getTableNames(sqlCreate);
        final List<EntityField> fields = createTableFields(sqlCreate);
        return new Entity(getTableName(names), getSchema(names), fields);
    }

    private List<EntityField> createTableFields(SqlCreate sqlCreate) {
        final List<EntityField> fields = new ArrayList<>();
        final SqlNodeList columnList = (SqlNodeList) sqlCreate.getOperandList().get(1);
        if (columnList != null) {
            final Map<String, EntityField> fieldMap = new HashMap<>();
            for (int ordinalPos = 0; ordinalPos < columnList.getList().size(); ordinalPos++) {
                SqlNode col = columnList.getList().get(ordinalPos);
                if (col.getKind().equals(SqlKind.COLUMN_DECL)) {
                    final EntityField field = createField((SqlColumnDeclaration) col, ordinalPos);
                    fieldMap.put(field.getName(), field);
                    fields.add(field);
                } else if (col.getKind().equals(SqlKind.PRIMARY_KEY)) {
                    initPrimaryKeyColumns((SqlKeyConstraint) col, fieldMap);
                } else {
                    throw new DtmException(String.format("Attribute type %s is not supported", col.getKind()));
                }
            }
            initDistributedKeyColumns(sqlCreate, fieldMap);
        }
        return fields;
    }

    private String getTableName(List<String> names) {
        return names.get(names.size() - 1);
    }

    private String getSchema(List<String> names) {
        return names.size() > 1 ? names.get(names.size() - 2) : null;
    }

    @NotNull
    private EntityField createField(SqlColumnDeclaration columnValue, int ordinalPos) {
        val column = getColumn(columnValue);
        val columnTypeSpec = getColumnTypeSpec(columnValue);
        val columnType = getColumnType(columnTypeSpec);
        if (columnType == ColumnType.ANY) {
            throw new DtmException(String.format("Unknown type for column: %s", column.getSimple()));
        }

        final EntityField field = new EntityField(
                ordinalPos,
                column.getSimple(),
                columnType,
                columnTypeSpec.getNullable()
        );
        if (columnTypeSpec.getTypeNameSpec() instanceof SqlBasicTypeNameSpec) {
            val basicTypeNameSpec = (SqlBasicTypeNameSpec) columnTypeSpec.getTypeNameSpec();
            if (field.getType() == ColumnType.TIMESTAMP || field.getType() == ColumnType.TIME) {
                field.setAccuracy(Optional.ofNullable(getPrecision(basicTypeNameSpec)).orElse(DEFAULT_PRECISION));
            } else {
                field.setSize(getPrecision(basicTypeNameSpec));
                field.setAccuracy(getScale(basicTypeNameSpec));
            }
        }
        return field;
    }

    private ColumnType getColumnType(SqlDataTypeSpec sqlDataTypeSpec) {
        val typeName = sqlDataTypeSpec.getTypeName().getSimple().toUpperCase();
        val sqlType = SqlTypeName.get(typeName);
        if (sqlType == null) {
            return ColumnTypeUtil.fromTypeString(typeName.toLowerCase());
        } else {
            return ColumnTypeUtil.valueOf(sqlType);
        }
    }

    private void initPrimaryKeyColumns(SqlKeyConstraint col, Map<String, EntityField> fieldMap) {
        final List<SqlNode> pks = getPrimaryKeys(col);
        Integer pkOrder = 1;
        for (SqlNode pk : pks) {
            SqlIdentifier pkIdent = (SqlIdentifier) pk;
            EntityField keyfield = fieldMap.get(pkIdent.getSimple());
            if (keyfield == null) {
                throw new DtmException(String.format("Unknown primary key column: %s",
                        pkIdent.getSimple()));
            }
            keyfield.setPrimaryOrder(pkOrder);
            keyfield.setNullable(false);
            pkOrder++;
        }
    }

    @NotNull
    private SqlIdentifier getColumn(SqlColumnDeclaration col) {
        return ((SqlIdentifier) col.getOperandList().get(0));
    }

    private SqlDataTypeSpec getColumnTypeSpec(SqlColumnDeclaration col) {
        if (col.getOperandList().size() > 1) {
            return (SqlDataTypeSpec) col.getOperandList().get(1);
        } else {
            throw new DtmException("Column type error");
        }
    }

    private List<SqlNode> getPrimaryKeys(SqlKeyConstraint col) {
        if (col.getOperandList().size() > 0) {
            return ((SqlNodeList) col.getOperandList().get(1)).getList();
        } else {
            throw new DtmException("Primary key definition failed");
        }
    }

    private Integer getPrecision(SqlBasicTypeNameSpec columnType) {
        return columnType.getPrecision() != -1 ? columnType.getPrecision() : null;
    }

    private Integer getScale(SqlBasicTypeNameSpec columnType) {
        return columnType.getScale() != -1 ? columnType.getScale() : null;
    }

    private void initDistributedKeyColumns(SqlCreate sqlCreate, Map<String, EntityField> fieldMap) {
        if (!(sqlCreate instanceof SqlCreateTable || sqlCreate instanceof SqlCreateMaterializedView)) {
            return;
        }

        SqlNodeList distributedBy;
        if (sqlCreate instanceof SqlCreateTable) {
            SqlCreateTable createTable = (SqlCreateTable) sqlCreate;
            distributedBy = createTable.getDistributedBy().getNodeList();
        } else {
            SqlCreateMaterializedView createTable = (SqlCreateMaterializedView) sqlCreate;
            distributedBy = createTable.getDistributedBy().getNodeList();
        }

        if (distributedBy != null) {
            List<SqlNode> distrColumnList = distributedBy.getList();
            if (distrColumnList != null) {
                initDistributedOrderAttr(distrColumnList, fieldMap);
            }
        }
    }

    private void initDistributedOrderAttr(List<SqlNode> distrColumnList, Map<String, EntityField> fieldMap) {
        Integer dkOrder = 1;
        for (SqlNode sqlNode : distrColumnList) {
            SqlIdentifier node = (SqlIdentifier) sqlNode;
            final EntityField field = fieldMap.get(node.getSimple());
            if (field == null) {
                throw new DtmException(String.format("Unknown distributed key column: %s",
                        node.getSimple()));
            }
            field.setShardingOrder(dkOrder);
            dkOrder++;
        }
    }
}
