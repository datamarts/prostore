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
package ru.datamart.prostore.query.execution.core.delta.utils;

import java.time.format.DateTimeFormatter;

public class DeltaQueryUtil {

    public static final String DELTA_DATE_TIME_PATTERN = "yyyy-MM-dd HH:mm:ss";
    public static final DateTimeFormatter DELTA_DATE_TIME_FORMATTER = DateTimeFormatter.ofPattern(DELTA_DATE_TIME_PATTERN);
    public static final String NUM_FIELD = "delta_num";
    public static final String CN_FROM_FIELD = "cn_from";
    public static final String CN_TO_FIELD = "cn_to";
    public static final String DATE_TIME_FIELD = "delta_date";
    public static final String CN_MAX_FIELD = "cn_max";
    public static final String IS_ROLLING_BACK_FIELD = "is_rolling_back";
    public static final String WRITE_OP_FINISHED_FIELD = "write_op_finished";

    private DeltaQueryUtil() {
    }
}
