package org.apache.iotdb.tsfile.file.metadataV2;

import java.io.IOException;
import java.io.OutputStream;
import java.util.List;

import org.apache.iotdb.tsfile.file.metadataV2.ChunkMetaDataV2;
import org.apache.iotdb.tsfile.utils.ReadWriteIOUtils;

public class TimeseriesMetaData {
  
  private long startOffsetOfTimeseries;
  private long endOffsetOfTimeseries;
  private List<ChunkMetaDataV2> chunkMetaDataList;
  private long version;

  /**
   * serialize to outputStream.
   *
   * @param outputStream outputStream
   * @return byte length
   * @throws IOException IOException
   */
  public int serializeTo(OutputStream outputStream) throws IOException {
    int byteLen = 0;
    byteLen += ReadWriteIOUtils.write(deviceID, outputStream);
    byteLen += ReadWriteIOUtils.write(startOffsetOfTimeseries, outputStream);
    byteLen += ReadWriteIOUtils.write(endOffsetOfTimeseries, outputStream);
    byteLen += ReadWriteIOUtils.write(version, outputStream);

    byteLen += ReadWriteIOUtils.write(chunkMetaDataList.size(), outputStream);
    for (ChunkMetaDataV2 chunkMetaData : chunkMetaDataList) {
      byteLen += chunkMetaData.serializeTo(outputStream);
    }
    return byteLen;
  }

}
