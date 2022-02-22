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
package ru.datamart.prostore.query.execution.plugin.adg.base.configuration.properties;

import lombok.Data;
import org.springframework.boot.context.properties.ConfigurationProperties;

@Data
@ConfigurationProperties("adg.tarantool.cartridge")
public class TarantoolCartridgeProperties {

  private String adminApiUrl = "/admin/api";

  private String sendQueryUrl = "/api/kafka/send_query";

  private String url;

  private String kafkaSubscriptionUrl = "/api/v1/kafka/subscription";

  private String kafkaLoadDataUrl = "/api/v1/kafka/dataload";

  private String transferDataToScdTableUrl = "/api/etl/transfer_data_to_scd_table";

  private String kafkaUploadDataUrl = "/api/v1/kafka/dataunload/query";

  private String tableBatchDeleteUrl = "/api/v1/ddl/table/batchDelete";

  private String tableQueuedCreate = "/api/v1/ddl/table/queuedCreate";

  private String tableQueuedDelete = "/api/v1/ddl/table/queuedDelete";

  private String reverseHistoryTransferUrl = "/api/v1/ddl/table/reverseHistoryTransfer";

  private String tableSchemaUrl = "/api/v1/ddl/table/schema";

  private String checkSumUrl = "/api/etl/get_scd_table_checksum";

  private String deleteSpaceTuples = "/api/etl/delete_data_from_scd_table";

  private String truncateSpace = "/api/etl/truncate_space_on_cluster";

  private String checkVersionsUrl = "/versions";
}
