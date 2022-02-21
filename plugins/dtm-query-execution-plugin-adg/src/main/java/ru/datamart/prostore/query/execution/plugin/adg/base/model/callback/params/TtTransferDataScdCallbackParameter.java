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
package ru.datamart.prostore.query.execution.plugin.adg.base.model.callback.params;

import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@AllArgsConstructor
@NoArgsConstructor
public class TtTransferDataScdCallbackParameter implements TtKafkaCallbackParameter {

    @JsonProperty("_space")
    private String spaceName;
    @JsonProperty("_stage_data_table_name")
    private String stageTableName;
    @JsonProperty("_actual_data_table_name")
    private String actualTableName;
    @JsonProperty("_historical_data_table_name")
    private String historyTableName;
    @JsonProperty("_delta_number")
    private long deltaNumber;

}
