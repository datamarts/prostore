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
package ru.datamart.prostore.jdbc.core;

public class FieldMetadata {

    final String columnName;
    final String tableName;
    final String schemaName;
    final int nullable;
    final boolean autoIncrement;

    public FieldMetadata(String columnName) {
        this(columnName, "", "", 1, false);
    }

    public FieldMetadata(String columnName, String schemaName) {
        this(columnName, "", schemaName, 1, false);
    }

    FieldMetadata(String columnName, String tableName, String schemaName, int nullable, boolean autoIncrement) {
        this.columnName = columnName;
        this.tableName = tableName;
        this.schemaName = schemaName;
        this.nullable = nullable;
        this.autoIncrement = autoIncrement;
    }

    public String getColumnName() {
        return columnName;
    }

    public String getTableName() {
        return tableName;
    }

    public String getSchemaName() {
        return schemaName;
    }

    public int getNullable() {
        return nullable;
    }

    public boolean isAutoIncrement() {
        return autoIncrement;
    }
}
