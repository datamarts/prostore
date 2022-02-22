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
package ru.datamart.prostore.query.execution.plugin.adb.base.factory.metadata;

import ru.datamart.prostore.query.execution.plugin.adb.base.dto.metadata.AdbTableColumn;
import ru.datamart.prostore.query.execution.plugin.adb.base.dto.metadata.AdbTableEntity;
import ru.datamart.prostore.query.execution.plugin.adb.query.service.DatabaseExecutor;
import ru.datamart.prostore.query.execution.plugin.api.factory.MetaTableEntityFactory;
import io.vertx.core.Future;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.*;
import java.util.stream.Collectors;

@Service("adbMetadataEntityFactory")
public class AdbMetaTableEntityFactory implements MetaTableEntityFactory<AdbTableEntity> {

    public static final String CHARACTER_MAXIMUM_LENGTH = "character_maximum_length";
    public static final String DATETIME_PRECISION = "datetime_precision";
    public static final String ORDINAL_POSITION = "ordinal_position";
    public static final String CONSTRAINT_TYPE = "constraint_type";
    public static final String PRIMARY_KEY_CS_TYPE = "PRIMARY KEY";
    public static final String QUERY_PATTERN = String.format("SELECT \n" +
                    "  c.column_name as %s, \n" +
                    "  c.udt_name as %s, \n" +
                    "  con.constraint_type as %s,\n" +
                    "  c.%s,\n" +
                    "  c.%s,\n" +
                    "  u.%s\n" +
                    " FROM INFORMATION_SCHEMA.COLUMNS c\n" +
                    "    LEFT OUTER JOIN INFORMATION_SCHEMA.KEY_COLUMN_USAGE u\n" +
                    "      ON u.table_schema = c.table_schema\n" +
                    "        AND u.table_name = c.table_name\n" +
                    "        AND u.column_name = c.column_name\n" +
                    "    LEFT OUTER JOIN INFORMATION_SCHEMA.TABLE_CONSTRAINTS con\n" +
                    "      ON con.constraint_schema = u.constraint_schema\n" +
                    "        AND con.table_schema = u.table_schema\n" +
                    "        AND con.constraint_catalog = u.constraint_catalog\n" +
                    "        AND con.table_name = u.table_name\n" +
                    " WHERE %s",
            COLUMN_NAME, DATA_TYPE, CONSTRAINT_TYPE, CHARACTER_MAXIMUM_LENGTH, DATETIME_PRECISION, ORDINAL_POSITION,
            "c.table_schema = '%s' AND c.table_name = '%s';");

    private final DatabaseExecutor adbQueryExecutor;

    @Autowired
    public AdbMetaTableEntityFactory(DatabaseExecutor adbQueryExecutor) {
        this.adbQueryExecutor = adbQueryExecutor;
    }

    @Override
    public Future<Optional<AdbTableEntity>> create(String envName, String schema, String table) {
        //TODO need to move creating query to factory class
        String query = String.format(QUERY_PATTERN, schema, table);
        return adbQueryExecutor.execute(query)
                .compose(result -> Future.succeededFuture(result.isEmpty()
                        ? Optional.empty()
                        : Optional.of(transformToAdbEntity(result))));
    }

    private AdbTableEntity transformToAdbEntity(List<Map<String, Object>> mapList) {
        AdbTableEntity result = new AdbTableEntity();
        Map<Integer, String> pkKeys = new TreeMap<>();
        List<AdbTableColumn> columns = mapList.stream()
                .filter(map -> Optional.ofNullable(map.get(CONSTRAINT_TYPE))
                        .map(PRIMARY_KEY_CS_TYPE::equals)
                        .orElse(true))
                .peek(map -> Optional.ofNullable(map.get(ORDINAL_POSITION))
                        .ifPresent(pos -> pkKeys.put(Integer.parseInt(pos.toString()), map.get(COLUMN_NAME).toString())))
                .map(map -> new AdbTableColumn(map.get(COLUMN_NAME).toString(), getType(map), false))
                .collect(Collectors.toList());
        result.setColumns(columns);
        result.setPrimaryKeys(new ArrayList<>(pkKeys.values()));
        return result;
    }

    private String getType(Map<String, Object> map) {
        String type = map.get(DATA_TYPE).toString();
        String size = Optional.ofNullable(map.get(CHARACTER_MAXIMUM_LENGTH))
                .map(val -> String.format("(%s)", val))
                .orElse("");
        String precision = Optional.ofNullable(map.get(DATETIME_PRECISION))
                .map(val -> String.format("(%s)", val))
                .orElse("");
        String result;
        switch (type) {
            case "varchar":
            case "char":
                result = String.format("%s%s", type, size);
                break;
            case "time":
            case "timestamp":
                result = String.format("%s%s", type, precision);
                break;
            default :
                result = type;
        }
        return result;
    }
}
