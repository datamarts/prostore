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
package ru.datamart.prostore.common.dto;

import lombok.AllArgsConstructor;
import lombok.Data;

/**
 * Actual delta request dto
 */
@Data
@AllArgsConstructor
public class ActualDeltaRequest {
    /**
     * Datamart
     */
    private String datamart;
    /**
     * Datatime in format: 2019-12-23 15:15:14
     */
    private String dateTime;
    /**
     * Is last uncommitted delta
     */
    private boolean isLatestUncommittedDelta;
}
