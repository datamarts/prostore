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
package ru.datamart.prostore.query.execution.plugin.adb.check.factory.impl;

import org.springframework.stereotype.Service;

@Service
public class AdbVersionQueriesFactory {

    public static final String COMPONENT_NAME_COLUMN = "name";
    public static final String VERSION_COLUMN = "version";
    private static final String ADB_NAME = "'adb cluster'";
    private static final String FDW_NAME = "'kafka-greenplum connector writer (fdw)'";
    private static final String PXF_NAME = "'kafka-greenplum connector reader (pxf)'";
    private static final String CONNECTORS_QUERY = "SELECT %s as %s, extversion as %s FROM pg_catalog.pg_extension WHERE extname = '%s'";

    public String createAdbVersionQuery() {
        return String.format("SELECT %s as %s, VERSION() as %s", ADB_NAME, COMPONENT_NAME_COLUMN, VERSION_COLUMN);
    }

    public String createFdwVersionQuery() {
        return String.format(CONNECTORS_QUERY, FDW_NAME, COMPONENT_NAME_COLUMN, VERSION_COLUMN, "kadb_fdw");
    }

    public String createPxfVersionQuery() {
        return String.format(CONNECTORS_QUERY, PXF_NAME, COMPONENT_NAME_COLUMN, VERSION_COLUMN, "pxf");
    }
}
