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
package ru.datamart.prostore.query.execution.core.eddl.service;

import io.vertx.core.Future;
import lombok.extern.slf4j.Slf4j;
import lombok.val;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;
import ru.datamart.prostore.common.exception.DtmException;
import ru.datamart.prostore.common.metrics.RequestMetrics;
import ru.datamart.prostore.common.model.ddl.EntityType;
import ru.datamart.prostore.common.model.ddl.ExternalTableLocationType;
import ru.datamart.prostore.common.plugin.exload.Type;
import ru.datamart.prostore.kafka.core.configuration.kafka.KafkaZookeeperProperties;
import ru.datamart.prostore.query.calcite.core.extension.eddl.*;
import ru.datamart.prostore.query.calcite.core.extension.eddl.standalone.SqlCreateStandaloneExternalTable;
import ru.datamart.prostore.query.calcite.core.extension.eddl.standalone.SqlCreateWriteableExternalTable;
import ru.datamart.prostore.query.calcite.core.extension.eddl.standalone.SqlDropStandaloneExternalTable;
import ru.datamart.prostore.query.calcite.core.extension.eddl.standalone.SqlDropWriteableExternalTable;
import ru.datamart.prostore.query.execution.core.base.service.avro.AvroSchemaGenerator;
import ru.datamart.prostore.query.execution.core.base.service.metadata.MetadataCalciteGenerator;
import ru.datamart.prostore.query.execution.core.eddl.dto.*;

import java.util.Collections;

@Component
@Slf4j
public class EddlQueryParamExtractor {
    private static final String AUTO_CREATE_SYS_OP_ENABLE_OPTION = "auto.create.sys_op.enable";
    private static final String START_LOCATION_TOKEN = "$";
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
        return Future.future(promise -> {
            val sqlNode = context.getSqlNode();
            val defaultSchema = context.getRequest().getQueryRequest().getDatamartMnemonic();
            EddlQuery eddlQuery;
            if (sqlNode instanceof SqlDropExternalTable) {
                eddlQuery = extractDropExternalTable((SqlDropExternalTable) sqlNode, defaultSchema);
            } else if (sqlNode instanceof SqlCreateDownloadExternalTable) {
                eddlQuery = extractCreateDownloadExternalTable((SqlCreateDownloadExternalTable) sqlNode, defaultSchema);
            } else if (sqlNode instanceof SqlCreateUploadExternalTable) {
                eddlQuery = extractCreateUploadExternalTable((SqlCreateUploadExternalTable) sqlNode, defaultSchema);
            } else if (sqlNode instanceof SqlCreateStandaloneExternalTable) {
                eddlQuery = extractCreateStandaloneExternalTable((SqlCreateStandaloneExternalTable) sqlNode, defaultSchema, context.getEnvName(), context.getMetrics());
            } else if (sqlNode instanceof SqlDropStandaloneExternalTable) {
                eddlQuery = extractDropStandaloneExternalTable((SqlDropStandaloneExternalTable) sqlNode, defaultSchema, context.getEnvName(), context.getMetrics());
            } else {
                throw new DtmException(String.format("Query [%s] is not an EDDL statement", sqlNode));
            }
            promise.complete(eddlQuery);
        });
    }

    private EddlQuery extractDropExternalTable(SqlDropExternalTable ddl,
                                               String defaultSchema) {
        try {
            val names = ddl.getName().names;
            val datamart = names.size() > 1 ? names.get(0) : defaultSchema;
            val table = names.get(names.size() - 1);
            if (ddl instanceof SqlDropUploadExternalTable) {
                return new DropUploadExternalTableQuery(datamart, table);
            }
            return new DropDownloadExternalTableQuery(datamart, table);
        } catch (RuntimeException e) {
            throw new DtmException("Error generating drop upload/download external table query", e);
        }
    }

    private CreateDownloadExternalTableQuery extractCreateDownloadExternalTable(SqlCreateDownloadExternalTable ddl,
                                                                                String defaultSchema) {
        try {
            val tableInfo = SqlNodeUtils.getTableInfo(ddl, defaultSchema);
            val entity = metadataCalciteGenerator.generateTableMetadata(ddl);
            entity.setEntityType(EntityType.DOWNLOAD_EXTERNAL_TABLE);
            val avroSchema = avroSchemaGenerator.generateTableSchema(entity, false);
            val locationOperator = ddl.getLocationOperator();
            val format = ddl.getFormatOperator().getFormat();
            val chunkSizeOperator = ddl.getChunkSizeOperator();
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
        } catch (DtmException e) {
            throw e;
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
            val withSysOpField = isWithSysOpField(ddl);
            val avroSchema = avroSchemaGenerator.generateTableSchema(entity, withSysOpField);
            val locationOperator = ddl.getLocationOperator();
            val format = ddl.getFormatOperator().getFormat();
            val messageLimitOperator = ddl.getMassageLimitOperator();
            val optionsMap = ddl.getOptions().getOptionsMap();
            return CreateUploadExternalTableQuery.builder()
                    .schemaName(tableInfo.getSchemaName())
                    .tableName(tableInfo.getTableName())
                    .entity(entity)
                    .locationType(locationOperator.getType())
                    .locationPath(getLocation(locationOperator))
                    .format(format)
                    .tableSchema(avroSchema.toString())
                    .messageLimit(messageLimitOperator.getMessageLimit())
                    .optionsMap(optionsMap)
                    .build();
        } catch (DtmException e) {
            throw e;
        } catch (RuntimeException e) {
            throw new DtmException("Error generating create upload external table query", e);
        }
    }

    private boolean isWithSysOpField(SqlCreateUploadExternalTable ddl) {
        val createSysOp = ddl.getOptions().getOptionsMap().get(AUTO_CREATE_SYS_OP_ENABLE_OPTION);
        if (createSysOp == null) {
            return true;
        }

        return Boolean.parseBoolean(createSysOp);
    }

    private CreateStandaloneExternalTableQuery extractCreateStandaloneExternalTable(SqlCreateStandaloneExternalTable ddl,
                                                                                    String defaultSchema,
                                                                                    String envName,
                                                                                    RequestMetrics metrics) {
        try {
            EddlAction eddlAction;
            EntityType entityType;
            if (ddl instanceof SqlCreateWriteableExternalTable) {
                eddlAction = EddlAction.CREATE_WRITEABLE_EXTERNAL_TABLE;
                entityType = EntityType.WRITEABLE_EXTERNAL_TABLE;
            } else {
                eddlAction = EddlAction.CREATE_READABLE_EXTERNAL_TABLE;
                entityType = EntityType.READABLE_EXTERNAL_TABLE;
            }

            val tableInfo = SqlNodeUtils.getTableInfo(ddl, defaultSchema);
            val entity = metadataCalciteGenerator.generateTableMetadata(ddl);
            entity.setEntityType(entityType);
            entity.setExternalTableOptions(ddl.getOptions().getOptionsMap());
            val sourceType = ddl.getLocation().getType();
            entity.setDestination(Collections.singleton(sourceType));
            entity.setExternalTableLocationType(ExternalTableLocationType.fromSourceType(sourceType));
            entity.setExternalTableLocationPath(ddl.getLocation().getPath());
            entity.setExternalTableSchema(avroSchemaGenerator.generateTableSchema(entity, false).toString());
            return CreateStandaloneExternalTableQuery.builder()
                    .eddlAction(eddlAction)
                    .envName(envName)
                    .metrics(metrics)
                    .schemaName(tableInfo.getSchemaName())
                    .tableName(tableInfo.getTableName())
                    .sourceType(sourceType)
                    .entity(entity)
                    .build();
        } catch (DtmException e) {
            throw e;
        } catch (RuntimeException e) {
            throw new DtmException("Error generating create standalone external table query", e);
        }
    }

    private DropStandaloneExternalTableQuery extractDropStandaloneExternalTable(SqlDropStandaloneExternalTable ddl,
                                                                                String defaultSchema,
                                                                                String envName,
                                                                                RequestMetrics metrics) {
        try {
            EddlAction eddlAction;
            if (ddl instanceof SqlDropWriteableExternalTable) {
                eddlAction = EddlAction.DROP_WRITEABLE_EXTERNAL_TABLE;
            } else {
                eddlAction = EddlAction.DROP_READABLE_EXTERNAL_TABLE;
            }
            val tableInfo = SqlNodeUtils.getTableInfo(ddl, defaultSchema);
            return DropStandaloneExternalTableQuery.builder()
                    .eddlAction(eddlAction)
                    .envName(envName)
                    .metrics(metrics)
                    .schemaName(tableInfo.getSchemaName())
                    .tableName(tableInfo.getTableName())
                    .options(ddl.getOptions().getOptionsMap())
                    .build();
        } catch (RuntimeException e) {
            throw new DtmException("Error generating drop standalone external table query", e);
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
