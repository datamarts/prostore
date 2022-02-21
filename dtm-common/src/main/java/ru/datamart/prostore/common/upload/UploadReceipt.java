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
package ru.datamart.prostore.common.upload;

/*Квитанция об успешной загрузке чанка*/
public class UploadReceipt {
    String requestId;
    String datamartMnemonic;
    Integer sinId;
    Integer streamNumber;
    Integer chunkNumber;
    String tableName;


    public UploadReceipt() {
    }

    public UploadReceipt(String requestId, String datamartMnemonic, Integer sinId, Integer streamNumber, Integer chunkNumber, String tableName) {
        this.requestId = requestId;
        this.datamartMnemonic = datamartMnemonic;
        this.sinId = sinId;
        this.streamNumber = streamNumber;
        this.chunkNumber = chunkNumber;
        this.tableName = tableName;
    }

    public String getRequestId() {
        return requestId;
    }

    public void setRequestId(String requestId) {
        this.requestId = requestId;
    }

    public String getDatamartMnemonic() {
        return datamartMnemonic;
    }

    public void setDatamartMnemonic(String datamartMnemonic) {
        this.datamartMnemonic = datamartMnemonic;
    }

    public Integer getSinId() {
        return sinId;
    }

    public void setSinId(Integer sinId) {
        this.sinId = sinId;
    }

    public Integer getStreamNumber() {
        return streamNumber;
    }

    public void setStreamNumber(Integer streamNumber) {
        this.streamNumber = streamNumber;
    }

    public Integer getChunkNumber() {
        return chunkNumber;
    }

    public void setChunkNumber(Integer chunkNumber) {
        this.chunkNumber = chunkNumber;
    }

    public String getTableName() {
        return tableName;
    }

    public void setTableName(String tableName) {
        this.tableName = tableName;
    }
}

