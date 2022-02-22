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
package ru.datamart.prostore.query.execution.plugin.adb.mppw.kafka.factory;

import ru.datamart.prostore.common.model.ddl.Entity;
import ru.datamart.prostore.query.execution.plugin.adb.mppw.configuration.properties.AdbMppwProperties;
import ru.datamart.prostore.query.execution.plugin.api.mppw.kafka.MppwKafkaRequest;

import java.util.List;
import java.util.UUID;

public interface KafkaMppwSqlFactory {

    String checkStagingTableSqlQuery(String tableWithSchema);

    String createReadableExtTableSqlQuery(String tableWithSchema,
                                          String requestId,
                                          List<String> columnNameTypeList,
                                          String topic,
                                          String brokerList,
                                          String consumerGroup,
                                          long pollTimeout);

    String dropExtTableSqlQuery(String schema, String talbe, String requestId);

    String moveOffsetsExtTableSqlQuery(String schema, String table);

    String commitOffsetsSqlQuery(String schema, String table);

    String insertIntoKadbOffsetsSqlQuery(String schema, String table);

    String createWritableExtTableSqlQuery(String server, List<String> columnNameTypeList, MppwKafkaRequest request,
                                          AdbMppwProperties adbMppwProperties);

    String checkServerSqlQuery(String database, String brokerList);

    String createServerSqlQuery(String database, UUID requestId, String brokerList);

    List<String> getPxfColumnsFromEntity(Entity entity);

    List<String> getColumnsFromEntity(Entity entity);

    String dropForeignTableSqlQuery(String schema, String table);

    String insertIntoStagingTableSqlQuery(String schema, String columns, String table, String extTable);

    String insertIntoStagingTablePxfSqlQuery(String schema, String insertColumns, String selectColumns, String table, String extTable);

    String getTableName(String requestId);

    String getServerName(String database, UUID requestId);
}
