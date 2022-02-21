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
package ru.datamart.prostore.query.execution.core.eddl.service;

import ru.datamart.prostore.common.dto.TableInfo;
import ru.datamart.prostore.common.exception.DtmException;
import ru.datamart.prostore.common.model.ddl.EntityType;
import ru.datamart.prostore.common.plugin.exload.Type;
import ru.datamart.prostore.kafka.core.configuration.kafka.KafkaZookeeperProperties;
import ru.datamart.prostore.query.calcite.core.extension.eddl.*;
import ru.datamart.prostore.query.execution.core.eddl.dto.*;
import ru.datamart.prostore.query.execution.core.base.service.avro.AvroSchemaGenerator;
import ru.datamart.prostore.query.execution.core.base.service.metadata.MetadataCalciteGenerator;
import io.vertx.core.Future;
import lombok.extern.slf4j.Slf4j;
import lombok.val;
import org.apache.calcite.sql.SqlDdl;
import org.apache.calcite.sql.SqlNode;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

@Component
@Slf4j
public class EddlQueryParamExtractor {

    public static final String START_LOCATION_TOKEN = "$";
    private static final int ZOOKEEPER_DEFAULT_PORT = 2181;
    private final MetadataCalciteGenerator metadataCalciteGenerator;
    private final AvroSchemaGenerator avroSchemaGenerator;
    private final KafkaZookeeperProperties kafkaZookeeperProperties;

    @Autowired
    public EddlQueryParamExtractor(
            MetadataCalciteGenerator metadataCalciteGenerator,
            AvroSchemaGenerator avroSchemaGenerator,
            KafkaZookeeperProperties kafkaZookeeperProperties) {
        this.metadataCalciteGenerator = metadataCalciteGenerator;
        this.avroSchemaGenerator = avroSchemaGenerator;
        this.kafkaZookeeperProperties = kafkaZookeeperProperties;
    }

    public Future<EddlQuery> extract(EddlRequestContext context) {
        return extract(context.getSqlNode(), context.getRequest().getQueryRequest().getDatamartMnemonic());
    }

    private Future<EddlQuery> extract(SqlNode sqlNode, String defaultSchema) {
        return Future.future(promise -> {
            if (sqlNode instanceof SqlDdl) {
                if (sqlNode instanceof SqlDropDownloadExternalTable) {
                    promise.complete(extractDropDownloadExternalTable(
                            (SqlDropDownloadExternalTable) sqlNode,
                            defaultSchema));
                } else if (sqlNode instanceof SqlCreateDownloadExternalTable) {
                    promise.complete(extractCreateDownloadExternalTable(
                            (SqlCreateDownloadExternalTable) sqlNode,
                            defaultSchema));
                } else if (sqlNode instanceof SqlCreateUploadExternalTable) {
                    promise.complete(extractCreateUploadExternalTable((SqlCreateUploadExternalTable) sqlNode,
                            defaultSchema));
                } else if (sqlNode instanceof SqlDropUploadExternalTable) {
                    promise.complete(extractDropUploadExternalTable((SqlDropUploadExternalTable) sqlNode,
                            defaultSchema));
                } else {
                    promise.fail(new DtmException(String.format("Query [%s] is not an EDDL statement",
                            sqlNode)));
                }
            } else {
                promise.fail(new DtmException(String.format("Query [%s] is not an EDDL statement",
                        sqlNode)));
            }
        });
    }

    private DropDownloadExternalTableQuery extractDropDownloadExternalTable(SqlDropDownloadExternalTable ddl,
                                                                            String defaultSchema) {
        try {
            TableInfo tableInfo = SqlNodeUtils.getTableInfo(ddl, defaultSchema);
            return new DropDownloadExternalTableQuery(tableInfo.getSchemaName(), tableInfo.getTableName());
        } catch (RuntimeException e) {
            throw new DtmException("Error generating drop download external table query", e);
        }
    }

    private CreateDownloadExternalTableQuery extractCreateDownloadExternalTable(SqlCreateDownloadExternalTable ddl,
                                                                                String defaultSchema) {
        try {
            val tableInfo = SqlNodeUtils.getTableInfo(ddl, defaultSchema);
            val entity = metadataCalciteGenerator.generateTableMetadata(ddl);
            entity.setEntityType(EntityType.DOWNLOAD_EXTERNAL_TABLE);
            val avroSchema = avroSchemaGenerator.generateTableSchema(entity, false);
            val locationOperator = SqlNodeUtils.getOne(ddl, LocationOperator.class);
            val format = SqlNodeUtils.getOne(ddl, FormatOperator.class).getFormat();
            val chunkSizeOperator = SqlNodeUtils.getOne(ddl, ChunkSizeOperator.class);
            return CreateDownloadExternalTableQuery.builder()
                    .schemaName(tableInfo.getSchemaName())
                    .tableName(tableInfo.getTableName())
                    .entity(entity)
                    .locationType(locationOperator.getType())
                    .locationPath(getLocation(locationOperator))
                    .format(format)
                    .tableSchema(avroSchema.toString())
                    .chunkSize(chunkSizeOperator.getChunkSize())
                    .build();
        } catch (RuntimeException e) {
            throw new DtmException("Error generating create download external table query", e);
        }
    }

    private CreateUploadExternalTableQuery extractCreateUploadExternalTable(SqlCreateUploadExternalTable ddl,
                                                                            String defaultSchema) {
        try {
            val tableInfo = SqlNodeUtils.getTableInfo(ddl, defaultSchema);
            val entity = metadataCalciteGenerator.generateTableMetadata(ddl);
            entity.setEntityType(EntityType.UPLOAD_EXTERNAL_TABLE);
            val avroSchema = avroSchemaGenerator.generateTableSchema(entity);
            val locationOperator = SqlNodeUtils.getOne(ddl, LocationOperator.class);
            val format = SqlNodeUtils.getOne(ddl, FormatOperator.class).getFormat();
            val messageLimitOperator = SqlNodeUtils.getOne(ddl, MessageLimitOperator.class);
            return CreateUploadExternalTableQuery.builder()
                    .schemaName(tableInfo.getSchemaName())
                    .tableName(tableInfo.getTableName())
                    .entity(entity)
                    .locationType(locationOperator.getType())
                    .locationPath(getLocation(locationOperator))
                    .format(format)
                    .tableSchema(avroSchema.toString())
                    .messageLimit(messageLimitOperator.getMessageLimit())
                    .build();
        } catch (RuntimeException e) {
            throw new DtmException("Error generating create upload external table query", e);
        }
    }

    private DropUploadExternalTableQuery extractDropUploadExternalTable(SqlDropUploadExternalTable sqlNode,
                                                                        String defaultSchema) {
        try {
            TableInfo tableInfo = SqlNodeUtils.getTableInfo(sqlNode, defaultSchema);
            return new DropUploadExternalTableQuery(tableInfo.getSchemaName(), tableInfo.getTableName());
        } catch (RuntimeException e) {
            throw new DtmException("Error generating drop upload external table query", e);
        }
    }

    private String getLocation(LocationOperator locationOperator) {
        String replaceToken = START_LOCATION_TOKEN + locationOperator.getType().getName();
        return locationOperator.getLocation().replace(replaceToken, getConfigUrl(locationOperator.getType()));
    }

    private String getConfigUrl(Type type) {
        switch (type) {
            case KAFKA_TOPIC:
                return getZookeeperHostPort();
            case CSV_FILE:
            case HDFS_LOCATION:
                throw new DtmException("The given location type: " + type + " is not supported!");
            default:
                throw new DtmException("This type is not supported!");
        }
    }

    private String getZookeeperHostPort() {
        return kafkaZookeeperProperties.getConnectionString() + ":" + ZOOKEEPER_DEFAULT_PORT;
    }
}
