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
package ru.datamart.prostore.query.execution.core.base.dto.metadata;

import ru.datamart.prostore.common.reader.InformationSchemaView;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * The position of the information schema view in the request
 */
@Data
@AllArgsConstructor
@NoArgsConstructor
public class InformationSchemaViewPosition {

    /**
     * View
     */
    private InformationSchemaView view;

    /**
     * Start position
     */
    private int start;

    /**
     * End position
     */
    private int end;
}
