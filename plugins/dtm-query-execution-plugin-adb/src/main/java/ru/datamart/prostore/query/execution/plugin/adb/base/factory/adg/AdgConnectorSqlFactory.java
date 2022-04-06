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
package ru.datamart.prostore.query.execution.plugin.adb.base.factory.adg;

import lombok.val;
import org.springframework.stereotype.Service;
import ru.datamart.prostore.common.exception.DtmException;
import ru.datamart.prostore.common.model.ddl.ColumnType;
import ru.datamart.prostore.common.model.ddl.Entity;
import ru.datamart.prostore.common.model.ddl.EntityField;
import ru.datamart.prostore.query.execution.plugin.api.service.shared.adg.AdgSharedService;

import java.util.Comparator;
import java.util.stream.Collectors;

import static ru.datamart.prostore.query.execution.plugin.adb.base.utils.AdbTypeUtil.adbTypeFromDtmType;

@Service
public class AdgConnectorSqlFactory {
    private static final String EXT_TABLE_NAME_TEMPLATE = "%s.TARANTOOL_EXT_%s";
    private static final String DROP_EXTERNAL_TABLE_BY_NAME = "DROP EXTERNAL TABLE IF EXISTS %s";
    private static final String DROP_EXTERNAL_TABLE = "DROP EXTERNAL TABLE IF EXISTS %s.TARANTOOL_EXT_%s";
    private static final String CREATE_EXTERNAL_TABLE = "CREATE WRITABLE EXTERNAL TABLE %s.TARANTOOL_EXT_%s\n" +
            "(%s) LOCATION ('pxf://%s?PROFILE=tarantool-upsert&TARANTOOL_SERVER=%s&USER=%s&PASSWORD=%s&TIMEOUT_CONNECT=%d&TIMEOUT_READ=%d&TIMEOUT_REQUEST=%d&BUFFER_SIZE=%d')\n" +
            "FORMAT 'CUSTOM' (FORMATTER = 'pxfwritable_export')";
    private static final String INSERT_INTO_EXTERNAL_TABLE = "INSERT INTO %s.TARANTOOL_EXT_%s SELECT *, 0 FROM (%s) as __temp_tbl";
    private static final String INSERT_INTO_EXTERNAL_TABLE_ONLY_PK = "INSERT INTO %s.TARANTOOL_EXT_%s (%s) SELECT *, 1 FROM (%s) as __temp_tbl";
    private static final String ADDITIONAL_FIELD_TO_ONLY_PK = ", sys_op";
    private static final String DELIMETER = ", ";
    private final AdgSharedService adgSharedService;

    public AdgConnectorSqlFactory(AdgSharedService adgSharedService) {
        this.adgSharedService = adgSharedService;
    }

    public String extTableName(Entity entity) {
        return String.format(EXT_TABLE_NAME_TEMPLATE, entity.getSchema(), entity.getName());
    }

    public String createStandaloneExternalTable(Entity entity) {
        val columns = getColumns(entity, false);
        val sharedProperties = adgSharedService.getSharedProperties();
        return String.format(CREATE_EXTERNAL_TABLE, entity.getSchema(), entity.getName(),
                columns, entity.getExternalTableLocationPath(), sharedProperties.getServer(), sharedProperties.getUser(), sharedProperties.getPassword(),
                sharedProperties.getConnectTimeout(), sharedProperties.getReadTimeout(), sharedProperties.getRequestTimeout(), sharedProperties.getBufferSize());
    }

    public String createExternalTable(String env, String datamart, Entity entity) {
        val spaceName = getSpaceName(env, datamart, entity);
        val columns = getColumns(entity, true);
        val sharedProperties = adgSharedService.getSharedProperties();
        return String.format(CREATE_EXTERNAL_TABLE, datamart, entity.getName(),
                columns, spaceName, sharedProperties.getServer(), sharedProperties.getUser(), sharedProperties.getPassword(),
                sharedProperties.getConnectTimeout(), sharedProperties.getReadTimeout(), sharedProperties.getRequestTimeout(), sharedProperties.getBufferSize());
    }

    public String dropExternalTable(String extTableName) {
        return String.format(DROP_EXTERNAL_TABLE_BY_NAME, extTableName);
    }

    public String dropExternalTable(String datamart, Entity entity) {
        return String.format(DROP_EXTERNAL_TABLE, datamart, entity.getName());
    }

    public String insertIntoExternalTable(String datamart, Entity entity, String query, boolean onlyNotNullableKeys) {
        if (!onlyNotNullableKeys) {
            return String.format(INSERT_INTO_EXTERNAL_TABLE, datamart, entity.getName(), query);
        }

        val columns = entity.getFields().stream()
                .filter(field -> field.getPrimaryOrder() != null || !field.getNullable())
                .sorted(Comparator.comparingInt(EntityField::getOrdinalPosition))
                .map(EntityField::getName)
                .collect(Collectors.joining(DELIMETER, "", ADDITIONAL_FIELD_TO_ONLY_PK));

        return String.format(INSERT_INTO_EXTERNAL_TABLE_ONLY_PK, datamart, entity.getName(), columns, query);
    }

    private String getSpaceName(String env, String datamart, Entity entity) {
        return String.format("%s__%s__%s_staging", env, datamart, entity.getName());
    }

    private String getColumns(Entity entity, boolean addSysOp) {
        val builder = new StringBuilder();
        val fields = entity.getFields().stream()
                .sorted(Comparator.comparingInt(EntityField::getOrdinalPosition))
                .collect(Collectors.toList());
        for (int i = 0; i < fields.size(); i++) {
            val field = fields.get(i);
            if (i > 0) {
                builder.append(',');
            }
            builder.append(field.getName()).append(' ').append(mapType(field.getType()));
        }
        if (addSysOp) {
            builder.append(",sys_op ").append(mapType(ColumnType.BIGINT));
            builder.append(",bucket_id ").append(mapType(ColumnType.BIGINT));
        }
        return builder.toString();
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
            case DOUBLE:
            case FLOAT:
                return adbTypeFromDtmType(type, null);
            default:
                throw new DtmException("Could not map type to external table type: " + type.name());
        }
    }
}
