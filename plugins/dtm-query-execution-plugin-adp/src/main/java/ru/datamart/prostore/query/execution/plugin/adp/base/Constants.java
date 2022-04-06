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
package ru.datamart.prostore.query.execution.plugin.adp.base;

import java.util.Arrays;
import java.util.List;

public class Constants {

    public static final String ACTUAL_TABLE = "actual";
    public static final String STAGING_TABLE = "staging";
    public static final String HISTORY_TABLE = "history";
    public static final String SYS_FROM_ATTR = "sys_from";
    public static final String SYS_TO_ATTR = "sys_to";
    public static final String SYS_OP_ATTR = "sys_op";
    public static final String ACTUAL_TABLE_SUFFIX = "_" + ACTUAL_TABLE;
    public static final String STAGING_TABLE_SUFFIX = "_" + STAGING_TABLE;
    public static final String HISTORY_TABLE_SUFFIX = "_" + HISTORY_TABLE;
    public static final List<String> SYSTEM_FIELDS = Arrays.asList(
            SYS_FROM_ATTR, SYS_TO_ATTR, SYS_OP_ATTR
    );

    private Constants() {
    }

}
