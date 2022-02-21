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

import java.util.List;

/*Запрос на перенос данных в историческую таблицу*/
public class HistoryTransferRequest {
    private int deltaOk;
    private int deltaHot;
    private String tableName;
    private String datemartMnemonic;
    private String requestId;
    private List<String> columns;

    public HistoryTransferRequest() {
    }

    public HistoryTransferRequest(int deltaOk, int deltaHot, String tableName, String datemartMnemonic, String requestId, List<String> columns) {
        this.deltaOk = deltaOk;
        this.deltaHot = deltaHot;
        this.tableName = tableName;
        this.datemartMnemonic = datemartMnemonic;
        this.requestId = requestId;
        this.columns = columns;
    }

    public int getDeltaOk() {
        return deltaOk;
    }

    public void setDeltaOk(int deltaOk) {
        this.deltaOk = deltaOk;
    }

    public int getDeltaHot() {
        return deltaHot;
    }

    public void setDeltaHot(int deltaHot) {
        this.deltaHot = deltaHot;
    }

    public String getTableName() {
        return tableName;
    }

    public void setTableName(String tableName) {
        this.tableName = tableName;
    }

    public String getDatemartMnemonic() {
        return datemartMnemonic;
    }

    public void setDatemartMnemonic(String datemartMnemonic) {
        this.datemartMnemonic = datemartMnemonic;
    }

    public String getRequestId() {
        return requestId;
    }

    public void setRequestId(String requestId) {
        this.requestId = requestId;
    }

    public List<String> getColumns() {
        return columns;
    }

    public void setColumns(List<String> columns) {
        this.columns = columns;
    }
}
