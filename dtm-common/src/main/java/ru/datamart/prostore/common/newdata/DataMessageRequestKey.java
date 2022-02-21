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
package ru.datamart.prostore.common.newdata;

import java.time.LocalDateTime;
import java.util.UUID;

/*
 * Message request key DTO
 * */
public class DataMessageRequestKey {

    /**
     * Request UUID
     */
    UUID requestId;

    /**
     * Load session identifier
     */
    UUID loadProcID;

    /**
     * Load date of entire data chunk
     */
    LocalDateTime loadDate;

    /**
     * System operation sign. 1 - DELETE, default - 0
     */
    Integer sysOperation;

    /**
     * Datamart mnemonic
     */
    String datamartMnemonic;

    /**
     * Table name
     */
    String tableName;

    /**
     * Stream number
     */
    Integer streamNumber;

    /**
     * Number of streams
     */
    Integer streamTotal;

    /**
     * Chunk number within stream
     */
    Integer chunkNumber;

    /**
     * Is last chunk flag
     */
    boolean isLastChunk;


    public UUID getRequestId() {
        return requestId;
    }

    public void setRequestId(UUID requestId) {
        this.requestId = requestId;
    }

    public UUID getLoadProcID() {
        return loadProcID;
    }

    public void setLoadProcID(UUID loadProcID) {
        this.loadProcID = loadProcID;
    }

    public LocalDateTime getLoadDate() {
        return loadDate;
    }

    public void setLoadDate(LocalDateTime loadDate) {
        this.loadDate = loadDate;
    }

    public Integer getSysOperation() {
        return sysOperation;
    }

    public void setSysOperation(Integer sysOperation) {
        this.sysOperation = sysOperation;
    }

    public String getDatamartMnemonic() {
        return datamartMnemonic;
    }

    public void setDatamartMnemonic(String datamartMnemonic) {
        this.datamartMnemonic = datamartMnemonic;
    }

    public String getTableName() {
        return tableName;
    }

    public void setTableName(String tableName) {
        this.tableName = tableName;
    }

    public Integer getStreamNumber() {
        return streamNumber;
    }

    public void setStreamNumber(Integer streamNumber) {
        this.streamNumber = streamNumber;
    }

    public Integer getStreamTotal() {
        return streamTotal;
    }

    public void setStreamTotal(Integer streamTotal) {
        this.streamTotal = streamTotal;
    }

    public Integer getChunkNumber() {
        return chunkNumber;
    }

    public void setChunkNumber(Integer chunkNumber) {
      this.chunkNumber = chunkNumber;
    }

  public boolean getIsLastChunk() {
    return isLastChunk;
  }

  public void setIsLastChunk(boolean lastChunk) {
    isLastChunk = lastChunk;
  }

  @Override
  public String toString() {
    return "DataMessageRequestKey{" +
      "requestId=" + requestId +
      ", loadProcID=" + loadProcID +
      ", loadDate=" + loadDate +
      ", sysOperation=" + sysOperation +
      ", datamartMnemonic='" + datamartMnemonic + '\'' +
      ", tableName='" + tableName + '\'' +
      ", streamNumber=" + streamNumber +
      ", streamTotal=" + streamTotal +
      ", chunkNumber=" + chunkNumber +
      ", isLastChunk=" + isLastChunk +
      '}';
  }
}
