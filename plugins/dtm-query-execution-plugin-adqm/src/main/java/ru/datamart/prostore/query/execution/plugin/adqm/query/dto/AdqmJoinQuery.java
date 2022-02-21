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
package ru.datamart.prostore.query.execution.plugin.adqm.query.dto;

import lombok.AllArgsConstructor;
import lombok.Data;
import org.apache.calcite.rel.RelNode;

import java.util.List;

@Data
@AllArgsConstructor
public class AdqmJoinQuery {
    private RelNode left;
    private RelNode right;
    private List<List<String>> leftConditionColumns;
    private List<List<String>> rightConditionColumns;
    private boolean hasNonEquiConditions;
}
