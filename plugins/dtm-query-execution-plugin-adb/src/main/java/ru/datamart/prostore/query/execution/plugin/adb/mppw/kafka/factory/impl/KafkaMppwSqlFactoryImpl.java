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
package ru.datamart.prostore.query.execution.plugin.adb.mppw.kafka.factory.impl;

import ru.datamart.prostore.common.model.ddl.ColumnType;
import ru.datamart.prostore.common.model.ddl.Entity;
import ru.datamart.prostore.common.model.ddl.EntityField;
import ru.datamart.prostore.query.execution.plugin.adb.base.utils.AdbTypeUtil;
import ru.datamart.prostore.query.execution.plugin.adb.mppw.configuration.properties.AdbMppwProperties;
import ru.datamart.prostore.query.execution.plugin.adb.mppw.kafka.factory.KafkaMppwSqlFactory;
import ru.datamart.prostore.query.execution.plugin.api.mppw.kafka.MppwKafkaRequest;
import ru.datamart.prostore.query.execution.plugin.api.mppw.kafka.UploadExternalEntityMetadata;
import lombok.val;
import org.springframework.stereotype.Service;

import java.util.List;
import java.util.UUID;
import java.util.stream.Collectors;

import static ru.datamart.prostore.query.execution.plugin.adb.base.factory.Constants.STAGING_TABLE_SUFFIX;

@Service("kafkaMppwSqlFactoryImpl")
public class KafkaMppwSqlFactoryImpl implements KafkaMppwSqlFactory {
    private static final String COMMIT_OFFSETS = "SELECT kadb.commit_offsets('%s.%s'::regclass::oid)";
    private static final String SERVER_NAME_TEMPLATE = "FDW_KAFKA_%s";
    private static final String WRITABLE_EXT_TABLE_PREF = "FDW_EXT_";
    private static final String DELIMITER = ", ";
    private static final String DROP_FOREIGN_TABLE_SQL = "DROP FOREIGN TABLE IF EXISTS %s.%s";
    private static final String INSERT_INTO_KADB_OFFSETS = "insert into kadb.offsets SELECT * from kadb.load_partitions('%s.%s'::regclass::oid)";
    private static final String MOVE_TO_OFFSETS_FOREIGN_TABLE_SQL = "SELECT kadb.offsets_to_committed('%s.%s'::regclass::oid)";
    private static final String CREATE_FOREIGN_TABLE_SQL =
            "CREATE FOREIGN TABLE %s.%s (%s)\n" +
                    "SERVER %s\n" +
                    "OPTIONS (\n" +
                    "    format '%s',\n" +
                    "    k_topic '%s',\n" +
                    "    k_consumer_group '%s',\n" +
                    "    k_seg_batch '%s',\n" +
                    "    k_timeout_ms '%s',\n" +
                    "    k_initial_offset '0'\n" +
                    ")";
    private static final String CHECK_SERVER_SQL = "select fs.foreign_server_name from information_schema.foreign_servers fs\n" +
            "join information_schema.foreign_server_options fso\n" +
            "on fs.foreign_server_catalog = fso.foreign_server_catalog\n" +
            "and fs.foreign_server_name = fso.foreign_server_name\n" +
            "where fs.foreign_server_catalog = '%s'\n" +
            "and fs.foreign_data_wrapper_catalog = '%s'\n" +
            "and fs.foreign_data_wrapper_name = 'kadb_fdw'\n" +
            "and fso.option_name = 'k_brokers'\n" +
            "and fso.option_value = '%s'\n" +
            "LIMIT 1";
    private static final String CREATE_SERVER_SQL =
            "CREATE SERVER FDW_KAFKA_%s_%s\n" +
                    "FOREIGN DATA WRAPPER kadb_fdw\n" +
                    "OPTIONS (\n" +
                    "  k_brokers '%s'\n" +
                    ")";
    private static final String INSERT_INTO_STAGING_TABLE_SQL = "INSERT INTO %s.%s (%s) SELECT %s FROM %s.%s";
    private static final String CREATE_READABLE_EXT_TABLE_SQL = "CREATE READABLE EXTERNAL TABLE %s_%s_ext (%s)" +
            " LOCATION ('pxf://%s?" +
            "PROFILE=kafka-greenplum-writer&" +
            "KAFKA_BROKERS=%s&" +
            "CONSUMER_GROUP_NAME=%s&" +
            "POLL_TIMEOUT=%d')\n" +
            "FORMAT 'CUSTOM' (FORMATTER='pxfwritable_import')";
    private static final String DROP_EXT_TABLE_SQL = "DROP EXTERNAL TABLE IF EXISTS %s.%s_%s_ext";
    private static final String CHECK_STAGING_TABLE = "SELECT 1 FROM %s_staging LIMIT 1";

    @Override
    public String checkStagingTableSqlQuery(String tableWithSchema) {
        return String.format(CHECK_STAGING_TABLE, tableWithSchema);
    }

    @Override
    public String createReadableExtTableSqlQuery(String tableWithSchema,
                                                 String requestId,
                                                 List<String> columnNameTypeList,
                                                 String topic,
                                                 String brokerList,
                                                 String consumerGroup,
                                                 long pollTimeout) {
        val columns = String.join(DELIMITER, columnNameTypeList);
        return String.format(CREATE_READABLE_EXT_TABLE_SQL, tableWithSchema, requestId, columns, topic, brokerList, consumerGroup, pollTimeout);
    }

    @Override
    public String dropExtTableSqlQuery(String schema, String talbe, String requestId) {
        return String.format(DROP_EXT_TABLE_SQL, schema, talbe, requestId);
    }

    @Override
    public String moveOffsetsExtTableSqlQuery(String schema, String table) {
        return String.format(MOVE_TO_OFFSETS_FOREIGN_TABLE_SQL, schema, table);
    }

    @Override
    public String commitOffsetsSqlQuery(String schema, String table) {
        return String.format(COMMIT_OFFSETS, schema, table);
    }

    @Override
    public String insertIntoKadbOffsetsSqlQuery(String schema, String table) {
        return String.format(INSERT_INTO_KADB_OFFSETS, schema, table);
    }

    @Override
    public String createWritableExtTableSqlQuery(String server,
                                                 List<String> columnNameTypeList,
                                                 MppwKafkaRequest request,
                                                 AdbMppwProperties adbMppwProperties) {
        val schema = request.getDatamartMnemonic();
        val table = WRITABLE_EXT_TABLE_PREF + getUuidString(request.getRequestId());
        val columns = String.join(DELIMITER, columnNameTypeList);
        val format = request.getUploadMetadata().getFormat().getName();
        val topic = request.getTopic();
        val consumerGroup = adbMppwProperties.getConsumerGroup();
        val uploadMessageLimit = ((UploadExternalEntityMetadata) request.getUploadMetadata()).getUploadMessageLimit();
        val chunkSize = uploadMessageLimit != null ? uploadMessageLimit : adbMppwProperties.getDefaultMessageLimit();
        val timeout = adbMppwProperties.getFdwTimeoutMs();
        return String.format(CREATE_FOREIGN_TABLE_SQL, schema, table, columns, server, format, topic, consumerGroup, chunkSize, timeout);
    }

    @Override
    public String checkServerSqlQuery(String database, String brokerList) {
        return String.format(CHECK_SERVER_SQL, database, database, brokerList);
    }

    @Override
    public String createServerSqlQuery(String database, UUID requestId, String brokerList) {
        return String.format(CREATE_SERVER_SQL, database, getUuidString(requestId), brokerList);
    }

    @Override
    public List<String> getPxfColumnsFromEntity(Entity entity) {
        return entity.getFields().stream()
                .map(this::getPxfColumnDDLByField)
                .collect(Collectors.toList());
    }

    private String getPxfColumnDDLByField(EntityField field) {
        val sb = new StringBuilder();
        sb.append(field.getName())
                .append(" ");
        if (field.getType().equals(ColumnType.TIME)) {
            sb.append(AdbTypeUtil.adbTypeFromDtmType(ColumnType.TIMESTAMP, field.getSize()));
        } else {
            sb.append(AdbTypeUtil.adbTypeFromDtmType(field));
        }
        return sb.toString();
    }

    @Override
    public List<String> getColumnsFromEntity(Entity entity) {
        return entity.getFields().stream()
                .map(this::getColumnDDLByField)
                .collect(Collectors.toList());
    }

    private String getColumnDDLByField(EntityField field) {
        val sb = new StringBuilder();
        sb.append(field.getName())
                .append(" ")
                .append(AdbTypeUtil.adbTypeFromDtmType(field))
                .append(" ");
        if (!field.getNullable()) {
            sb.append("NOT NULL");
        }
        return sb.toString();
    }

    @Override
    public String dropForeignTableSqlQuery(String schema, String table) {
        return String.format(DROP_FOREIGN_TABLE_SQL, schema, table);
    }

    @Override
    public String insertIntoStagingTableSqlQuery(String schema, String columns, String table, String extTable) {
        val stagingTable = table + STAGING_TABLE_SUFFIX;
        return String.format(INSERT_INTO_STAGING_TABLE_SQL, schema, stagingTable, columns, columns, schema, extTable);
    }

    @Override
    public String insertIntoStagingTablePxfSqlQuery(String schema, String insertColumns, String selectColumns, String table, String extTable) {
        val stagingTable = table + STAGING_TABLE_SUFFIX;
        return String.format(INSERT_INTO_STAGING_TABLE_SQL, schema, stagingTable, insertColumns, selectColumns, schema, extTable);
    }

    @Override
    public String getTableName(String requestId) {
        return WRITABLE_EXT_TABLE_PREF + requestId.replace("-", "_");
    }

    @Override
    public String getServerName(String database, UUID requestId) {
        return String.format(SERVER_NAME_TEMPLATE, database + "_" + getUuidString(requestId));
    }

    private String getUuidString(UUID requestId) {
        return requestId.toString().replace("-", "_");
    }
}
