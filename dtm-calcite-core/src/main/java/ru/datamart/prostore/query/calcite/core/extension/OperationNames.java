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
package ru.datamart.prostore.query.calcite.core.extension;

public final class OperationNames {

    //DATABASE DDL
    public static final String CREATE_DATABASE = "CREATE DATABASE";
    public static final String DROP_DATABASE = "DROP DATABASE";

    //TABLE DDL
    public static final String CREATE_TABLE = "CREATE TABLE";
    public static final String DROP_TABLE = "DROP TABLE";

    //VIEW DDL
    public static final String CREATE_VIEW = "CREATE VIEW";
    public static final String ALTER_VIEW = "ALTER VIEW";
    public static final String DROP_VIEW = "DROP VIEW";

    //MATERIALIZED VIEW DDL
    public static final String CREATE_MATERIALIZED_VIEW = "CREATE MATERIALIZED VIEW";
    public static final String DROP_MATERIALIZED_VIEW = "DROP MATERIALIZED VIEW";

    //OTHER DDL
    public static final String TRUNCATE_HISTORY = "TRUNCATE HISTORY";
    public static final String ALLOW_CHANGES = "ALLOW_CHANGES";
    public static final String DENY_CHANGES = "DENY_CHANGES";

    private OperationNames() {
    }
}
