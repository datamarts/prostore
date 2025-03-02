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
package ru.datamart.prostore.query.execution.plugin.adb.synchronize.service;

import ru.datamart.prostore.common.delta.DeltaData;
import ru.datamart.prostore.common.model.ddl.Entity;
import ru.datamart.prostore.query.execution.model.metadata.Datamart;
import lombok.Data;
import org.apache.calcite.sql.SqlNode;

import java.util.List;

@Data
public class PrepareRequestOfChangesRequest {
    private final List<Datamart> datamarts;
    private final String envName;
    private final DeltaData deltaToBe;
    private final Long beforeDeltaCnTo;
    private final SqlNode viewQuery;
    private final Entity entity;
}
