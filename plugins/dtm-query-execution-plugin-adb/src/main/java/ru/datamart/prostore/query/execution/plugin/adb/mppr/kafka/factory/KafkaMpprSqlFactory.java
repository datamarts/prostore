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
package ru.datamart.prostore.query.execution.plugin.adb.mppr.kafka.factory;

import lombok.val;
import org.springframework.stereotype.Service;
import ru.datamart.prostore.common.dto.KafkaBrokerInfo;
import ru.datamart.prostore.query.execution.plugin.adb.base.utils.AdbTypeUtil;
import ru.datamart.prostore.query.execution.plugin.api.mppr.kafka.DownloadExternalEntityMetadata;
import ru.datamart.prostore.query.execution.plugin.api.mppr.kafka.MpprKafkaRequest;

import java.util.stream.Collectors;

@Service
public class KafkaMpprSqlFactory {

    private static final String DELIMITER = ", ";
    private static final String WRITABLE_EXTERNAL_TABLE_PREF = "PXF_EXT_";
    private static final String CREAT_WRITABLE_EXT_TABLE_SQL = "CREATE WRITABLE EXTERNAL TABLE %s.%s (%s)\n" +
            "    LOCATION ('pxf://%s?PROFILE=kafka&BOOTSTRAP_SERVERS=%s&BATCH_SIZE=%d')\n" +
            "    FORMAT 'CUSTOM' (FORMATTER='pxfwritable_export')";
    public static final String INSERT_INTO_WRITABLE_EXT_TABLE_SQL = "INSERT INTO %s.%s %s";
    public static final String DROP_WRITABLE_EXT_TABLE_SQL = "DROP EXTERNAL TABLE IF EXISTS %s.%s";

    public String createWritableExtTableSqlQuery(MpprKafkaRequest request) {
        val schema = request.getDatamartMnemonic();
        val table = getTableName(request.getRequestId().toString());
        val columns = request.getDestinationEntity().getFields().stream()
                .map(field -> field.getName() + " " + AdbTypeUtil.adbTypeFromDtmType(field))
                .collect(Collectors.toList());
        val topic = request.getTopic();
        val brokers = request.getBrokers().stream()
                .map(KafkaBrokerInfo::getAddress)
                .collect(Collectors.toList());
        val chunkSize = ((DownloadExternalEntityMetadata) request.getDownloadMetadata()).getChunkSize();
        return String.format(CREAT_WRITABLE_EXT_TABLE_SQL,
                schema,
                table,
                String.join(DELIMITER, columns),
                topic,
                String.join(DELIMITER, brokers),
                chunkSize);
    }

    public String insertIntoWritableExtTableSqlQuery(String schema, String table, String enrichedSql) {
        return String.format(INSERT_INTO_WRITABLE_EXT_TABLE_SQL, schema, table, enrichedSql);
    }

    public String dropWritableExtTableSqlQuery(String schema, String table) {
        return String.format(DROP_WRITABLE_EXT_TABLE_SQL, schema, table);
    }

    public String getTableName(String requestId) {
        return WRITABLE_EXTERNAL_TABLE_PREF + requestId.replace("-", "_");
    }
}
