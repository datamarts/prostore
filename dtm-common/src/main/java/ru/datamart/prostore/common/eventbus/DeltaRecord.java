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
package ru.datamart.prostore.common.eventbus;

import java.time.LocalDateTime;

/*Метаданные дельты*/
public class DeltaRecord {
    /*идентификатор сессии загрузки*/
    private Integer loadId;
    /*схема данных*/
    private String datemartMnemonic;
    /*дата/время сессии загрузки*/
    private LocalDateTime sysDate;
    /*порядковый номер дельты*/
    private Integer sinId;
    /*уникальный идентификатор пакета входящих данных со стороны ETL, ИС Поставщика*/
    private String loadProcId;
    /*статус загрузк*/
    private Integer status;

    public DeltaRecord(Integer loadId, String datemartMnemonic, LocalDateTime sysDate, Integer sinId, String loadProcId, Integer status) {
        this.loadId = loadId;
        this.datemartMnemonic = datemartMnemonic;
        this.sysDate = sysDate;
        this.sinId = sinId;
        this.loadProcId = loadProcId;
        this.status = status;
    }

    public DeltaRecord() {
    }

    public Integer getLoadId() {
        return loadId;
    }

    public void setLoadId(Integer loadId) {
        this.loadId = loadId;
    }

    public String getDatemartMnemonic() {
        return datemartMnemonic;
    }

    public void setDatemartMnemonic(String datemartMnemonic) {
        this.datemartMnemonic = datemartMnemonic;
    }

    public LocalDateTime getSysDate() {
        return sysDate;
    }

    public void setSysDate(LocalDateTime sysDate) {
        this.sysDate = sysDate;
    }

    public Integer getSinId() {
        return sinId;
    }

    public void setSinId(Integer sinId) {
        this.sinId = sinId;
    }

    public String getLoadProcId() {
        return loadProcId;
    }

    public void setLoadProcId(String loadProcId) {
        this.loadProcId = loadProcId;
    }

    public Integer getStatus() {
        return status;
    }

    public void setStatus(Integer status) {
        this.status = status;
    }

    public static DeltaRecord createDeltaRecordDto() {
        return new DeltaRecord();
    }

    public DeltaRecord addDatemartMnemonic(String datemartMnemonic) {
        this.datemartMnemonic = datemartMnemonic;
        return this;
    }

    public DeltaRecord addSinId(Integer sinId) {
        this.sinId = sinId;
        return this;
    }

    public DeltaRecord addSysDate(LocalDateTime sysDate) {
        this.sysDate = sysDate;
        return this;
    }

    public DeltaRecord addStatus(Integer status) {
        this.status = status;
        return this;
    }
}
