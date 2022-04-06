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
package ru.datamart.prostore.query.execution.plugin.adb.base.factory.adqm;

import lombok.val;
import org.springframework.stereotype.Component;
import ru.datamart.prostore.common.exception.DtmException;
import ru.datamart.prostore.common.model.ddl.ColumnType;
import ru.datamart.prostore.common.model.ddl.Entity;
import ru.datamart.prostore.common.model.ddl.EntityField;
import ru.datamart.prostore.common.model.ddl.EntityFieldUtils;
import ru.datamart.prostore.query.execution.plugin.api.service.shared.adqm.AdqmSharedService;

import java.util.Comparator;
import java.util.stream.Collectors;

import static ru.datamart.prostore.query.execution.plugin.adb.base.utils.AdbTypeUtil.adbTypeFromDtmType;

@Component
public class AdqmConnectorSqlFactory {

    private static final String EXT_TABLE_NAME_TEMPLATE = "%s.CLICKHOUSE_EXT_%s";
    private static final String EXT_TABLE_PK_ONLY_NAME_TEMPLATE = "%s.CLICKHOUSE_EXT_PK_%s";
    private static final String DROP_EXTERNAL_TABLE_BY_NAME = "DROP EXTERNAL TABLE IF EXISTS %s";
    private static final String CREATE_EXTERNAL_TABLE = "CREATE WRITABLE EXTERNAL TABLE %s.CLICKHOUSE_EXT%s_%s\n" +
            "(%s) LOCATION ('pxf://%s" +
            "?PROFILE=clickhouse-insert" +
            "&CLICKHOUSE_SERVERS=%s" +
            "%s" +
            "%s" +
            "&TIMEOUT_CONNECT=%d" +
            "&TIMEOUT_REQUEST=%d" +
            "&BUFFER_SIZE=%d" +
            "')\n" +
            "FORMAT 'CUSTOM' (FORMATTER = 'pxfwritable_export')";
    private static final String INSERT_INTO_EXTERNAL_TABLE =
            "INSERT INTO %s.CLICKHOUSE_EXT_%s (%s, sys_op, sys_from, sys_to, sys_close_date, sign) " +
                    "SELECT *, 0, %s, 9223372036854775807, 9223372036854775807, 1 FROM (%s) as __temp_tbl";
    private static final String INSERT_INTO_EXTERNAL_TABLE_ONLY_PK = "INSERT INTO %s.CLICKHOUSE_EXT_PK_%s (%s) %s";
    private static final String DELIMETER = ", ";
    private static final String ACTUAL_POSTFIX = "actual";
    private static final String BUFFER_POSTFIX = "buffer";
    private final AdqmSharedService adqmSharedService;

    public AdqmConnectorSqlFactory(AdqmSharedService adqmSharedService) {
        this.adqmSharedService = adqmSharedService;
    }

    public String extTableName(Entity entity) {
        return String.format(EXT_TABLE_NAME_TEMPLATE, entity.getSchema(), entity.getName());
    }

    public String extTablePkOnlyName(Entity entity) {
        return String.format(EXT_TABLE_PK_ONLY_NAME_TEMPLATE, entity.getSchema(), entity.getName());
    }

    public String createStandaloneExternalTable(Entity entity) {
        val tableName = entity.getExternalTableLocationPath();
        val datamart = entity.getSchema();
        return createTable(tableName, datamart, entity, getColumns(entity, false), "");
    }

    public String createExternalTable(String env, String datamart, Entity entity) {
        return createTable(env, datamart, entity, getColumns(entity, true), "", ACTUAL_POSTFIX);
    }

    public String createExternalPkOnlyTable(String env, String datamart, Entity entity) {
        return createTable(env, datamart, entity, getPkColumns(entity), "_PK", BUFFER_POSTFIX);
    }

    private String createTable(String tableName, String datamart, Entity entity, String columns, String pk) {
        val sharedProperties = adqmSharedService.getSharedProperties();
        return String.format(CREATE_EXTERNAL_TABLE, datamart, pk, entity.getName(),
                columns, tableName, sharedProperties.getHosts(),
                sharedProperties.getUser().isEmpty() ? "" : String.format("&USER=%s", sharedProperties.getUser()),
                sharedProperties.getPassword().isEmpty() ? "" : String.format("&PASSWORD=%s", sharedProperties.getPassword()),
                sharedProperties.getSocketTimeout(), sharedProperties.getDataTransferTimeout(), sharedProperties.getBufferSize());
    }

    private String createTable(String env, String datamart, Entity entity, String columns, String pk, String tablePostfix) {
        val tableName = getTableName(env, datamart, entity, tablePostfix);
        return createTable(tableName, datamart, entity, columns, pk);
    }

    public String dropExternalTable(String extTableName) {
        return String.format(DROP_EXTERNAL_TABLE_BY_NAME, extTableName);
    }

    public String insertIntoExternalTable(String datamart, Entity entity, String query, long sysCn) {
        val columns = String.join(DELIMETER, EntityFieldUtils.getFieldNames(entity));
        return String.format(INSERT_INTO_EXTERNAL_TABLE, datamart, entity.getName(), columns, sysCn, query);
    }

    public String insertIntoExternalTable(String datamart, Entity entity, String query) {
        val columns = entity.getFields().stream()
                .filter(field -> field.getPrimaryOrder() != null)
                .sorted(Comparator.comparingInt(EntityField::getOrdinalPosition))
                .map(EntityField::getName)
                .collect(Collectors.joining(DELIMETER));
        return String.format(INSERT_INTO_EXTERNAL_TABLE_ONLY_PK, datamart, entity.getName(), columns, query);
    }

    private String getTableName(String env, String datamart, Entity entity, String tablePostfix) {
        return String.format("%s__%s.%s_%s", env, datamart, entity.getName(), tablePostfix);
    }

    private String getColumns(Entity entity, boolean addSystemColumns) {
        val builder = new StringBuilder();
        val fields = entity.getFields().stream()
                .sorted(Comparator.comparingInt(EntityField::getOrdinalPosition))
                .map(field -> field.getName() + " " + mapType(field.getType()))
                .collect(Collectors.joining(","));
        builder.append(fields);

        if (addSystemColumns) {
            builder.append(",sys_from ").append(mapType(ColumnType.BIGINT));
            builder.append(",sys_to ").append(mapType(ColumnType.BIGINT));
            builder.append(",sys_op ").append(mapType(ColumnType.INT32));
            builder.append(",sys_close_date ").append(mapType(ColumnType.BIGINT));
            builder.append(",sign ").append(mapType(ColumnType.INT32));
        }
        return builder.toString();
    }

    private String getPkColumns(Entity entity) {
        return EntityFieldUtils.getPrimaryKeyList(entity.getFields()).stream()
                .sorted(Comparator.comparingInt(EntityField::getOrdinalPosition))
                .map(field -> field.getName() + " " + mapType(field.getType()))
                .collect(Collectors.joining(","));
    }

    private String mapType(ColumnType type) {
        switch (type) {
            case VARCHAR:
            case CHAR:
            case UUID:
            case LINK:
                return adbTypeFromDtmType(ColumnType.VARCHAR, null);
            case BIGINT:
            case INT:
            case DATE:
            case TIME:
            case TIMESTAMP:
                return adbTypeFromDtmType(ColumnType.BIGINT, null);
            case BOOLEAN:
            case INT32:
                return adbTypeFromDtmType(ColumnType.INT32, null);
            case DOUBLE:
            case FLOAT:
                return adbTypeFromDtmType(type, null);
            default:
                throw new DtmException("Could not map type to external table type: " + type.name());
        }
    }
}
