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
package ru.datamart.prostore.common.dto;

import java.util.Objects;
import java.util.UUID;

public class AdgHistoryTransferRequest {
  private UUID requestId;
  private String datamartMnemonic;
  private int sinId;
  private int streamNumber;
  private int chunkNumber;
  private String tableName;

  public UUID getRequestId() {
    return requestId;
  }

  public void setRequestId(UUID requestId) {
    this.requestId = requestId;
  }

  public String getDatamartMnemonic() {
    return datamartMnemonic;
  }

  public void setDatamartMnemonic(String datamartMnemonic) {
    this.datamartMnemonic = datamartMnemonic;
  }

  public int getSinId() {
    return sinId;
  }

  public void setSinId(int sinId) {
    this.sinId = sinId;
  }

  public int getStreamNumber() {
    return streamNumber;
  }

  public void setStreamNumber(int streamNumber) {
    this.streamNumber = streamNumber;
  }

  public int getChunkNumber() {
    return chunkNumber;
  }

  public void setChunkNumber(int chunkNumber) {
    this.chunkNumber = chunkNumber;
  }

  public String getTableName() {
    return tableName;
  }

  public void setTableName(String tableName) {
    this.tableName = tableName;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (!(o instanceof AdgHistoryTransferRequest)) return false;
    AdgHistoryTransferRequest that = (AdgHistoryTransferRequest) o;
    return sinId == that.sinId &&
      streamNumber == that.streamNumber &&
      chunkNumber == that.chunkNumber &&
      requestId.equals(that.requestId) &&
      datamartMnemonic.equals(that.datamartMnemonic) &&
      tableName.equals(that.tableName);
  }

  @Override
  public int hashCode() {
    return Objects.hash(requestId, datamartMnemonic, sinId, streamNumber, chunkNumber, tableName);
  }

  @Override
  public String toString() {
    return "AdgHistoryTransferRequest{" +
      "requestId=" + requestId +
      ", datamartMnemonic='" + datamartMnemonic + '\'' +
      ", sinId=" + sinId +
      ", streamNumber=" + streamNumber +
      ", chunkNumber=" + chunkNumber +
      ", tableName='" + tableName + '\'' +
      '}';
  }
}
