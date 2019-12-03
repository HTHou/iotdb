/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.iotdb.tsfile.file.metadataV2;

import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.file.metadata.statistics.Statistics;
import org.apache.iotdb.tsfile.read.common.Path;
import org.apache.iotdb.tsfile.utils.ReadWriteIOUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.util.Objects;

/**
 * MetaData of one chunk.
 */
public class ChunkMetaDataV2 {

  private static final Logger LOG = LoggerFactory.getLogger(ChunkMetaDataV2.class);


  private String measurementId;

  /**
   * Byte offset of the corresponding data in the file Notice: include the chunk header and marker.
   */
  private long offsetOfChunkHeader;

  private TSDataType tsDataType;

  /**
   * version is used to define the order of operations(insertion, deletion, update). version is set
   * according to its belonging ChunkGroup only when being queried, so it is not persisted.
   */
  private long version;

  /**
   * All data with timestamp <= deletedAt are considered deleted.
   */
  private long deletedAt = Long.MIN_VALUE;

  private Statistics statistics;

  private ChunkMetaDataV2() {
  }

  /**
   * constructor of ChunkMetaData.
   *
   * @param measurementUid measurement id
   * @param tsDataType time series data type
   * @param fileOffset file offset
   * @param statistics value statistics
   */
  public ChunkMetaDataV2(String measurementId, TSDataType tsDataType, long fileOffset,
    Statistics statistics) {
    this.measurementId = measurementId;
    this.tsDataType = tsDataType;
    this.offsetOfChunkHeader = fileOffset;
    this.statistics = statistics;
  }

  @Override
  public String toString() {
    return String.format("numPoints %d", statistics.getCount());
  }

  public long getNumOfPoints() {
    return statistics.getCount();
  }

  /**
   * get offset of chunk header.
   *
   * @return Byte offset of header of this chunk (includes the marker)
   */
  public long getOffsetOfChunkHeader() {
    return offsetOfChunkHeader;
  }

  public String getMeasurementId() {
    return measurementId;
  }

  public Statistics getStatistics() {
    return statistics;
  }

  public long getStartTime() {
    return statistics.getStartTime();
  }

  public long getEndTime() {
    return statistics.getEndTime();
  }

  public TSDataType getDataType() {
    return tsDataType;
  }

  /**
   * serialize to outputStream.
   *
   * @param outputStream outputStream
   * @return length
   * @throws IOException IOException
   */
  public int serializeTo(OutputStream outputStream) throws IOException {
    int byteLen = 0;

    byteLen += ReadWriteIOUtils.write(measurementId, outputStream);
    byteLen += ReadWriteIOUtils.write(offsetOfChunkHeader, outputStream);
    byteLen += ReadWriteIOUtils.write(tsDataType, outputStream);
    byteLen += statistics.serialize(outputStream);
    return byteLen;
  }

  /**
   * deserialize from ByteBuffer.
   *
   * @param buffer ByteBuffer
   * @return ChunkMetaData object
   */
  public static ChunkMetaDataV2 deserializeFrom(ByteBuffer buffer) {
    ChunkMetaDataV2 chunkMetaData = new ChunkMetaDataV2();

    chunkMetaData.measurementId = ReadWriteIOUtils.readString(buffer);
    chunkMetaData.offsetOfChunkHeader = ReadWriteIOUtils.readLong(buffer);
    chunkMetaData.tsDataType = ReadWriteIOUtils.readDataType(buffer);

    chunkMetaData.statistics = Statistics.deserialize(buffer, chunkMetaData.tsDataType);

    return chunkMetaData;
  }

  public long getVersion() {
    return version;
  }

  public void setVersion(long version) {
    this.version = version;
  }

  public long getDeletedAt() {
    return deletedAt;
  }

  public void setDeletedAt(long deletedAt) {
    this.deletedAt = deletedAt;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    ChunkMetaDataV2 that = (ChunkMetaDataV2) o;
    return offsetOfChunkHeader == that.offsetOfChunkHeader &&
        version == that.version &&
        deletedAt == that.deletedAt &&
        Objects.equals(measurementId, that.measurementId) &&
        tsDataType == that.tsDataType &&
        Objects.equals(statistics, that.statistics);
  }
}
