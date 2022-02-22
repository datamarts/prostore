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
package ru.datamart.prostore.jdbc.model;

import ru.datamart.prostore.common.model.ddl.ColumnType;

/**
 * Information about table column, receiving from LL-R service
 */
public class ColumnInfo {
    /**
     * Column name
     */
    private String mnemonic;
    /**
     * Column type
     */
    private ColumnType dataType;
    /**
     * Column length
     */
    private Integer length;
    /**
     * Precision
     */
    private Integer accuracy;
    /**
     * Table name
     */
    private String entityMnemonic;
    /**
     * Schema name
     */
    private String datamartMnemonic;
    /**
     * Order num of primary key
     */
    private Integer primaryKeyOrder;
    /**
     * Order num distributed key
     */
    private Integer distributeKeykOrder;
    /**
     * Column order
     */
    private Integer ordinalPosition;
    /**
     * Nullable
     */
    private Boolean nullable;

    public String getMnemonic() {
        return mnemonic;
    }

    public ColumnType getDataType() {
        return dataType;
    }

    public Integer getLength() {
        return length;
    }

    public Integer getAccuracy() {
        return accuracy;
    }

    public String getEntityMnemonic() {
        return entityMnemonic;
    }

    public String getDatamartMnemonic() {
        return datamartMnemonic;
    }

    public Integer getPrimaryKeyOrder() {
        return primaryKeyOrder;
    }

    public Integer getDistributeKeykOrder() {
        return distributeKeykOrder;
    }

    public Integer getOrdinalPosition() {
        return ordinalPosition;
    }

    public Boolean getNullable() {
        return nullable;
    }
}
