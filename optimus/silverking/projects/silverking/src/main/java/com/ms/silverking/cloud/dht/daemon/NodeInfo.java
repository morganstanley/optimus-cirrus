/*
 * Morgan Stanley makes this available to you under the Apache License, Version 2.0 (the "License").
 * You may obtain a copy of the License at http://www.apache.org/licenses/LICENSE-2.0.
 * See the NOTICE file distributed with this work for additional information regarding copyright ownership.
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.ms.silverking.cloud.dht.daemon;

import com.ms.silverking.numeric.NumConversion;

public class NodeInfo {
  private final long fsTotalBlocks;
  private final long fsUsedBlocks;
  private final long fsFreeBlocks;
  private final int fsBlockSize;

  private static final int fsTotalBlocksOffset = 0;
  private static final int fsUsedBlocksOffset = fsTotalBlocksOffset + NumConversion.BYTES_PER_LONG;
  private static final int fsFreeBlocksOffset = fsUsedBlocksOffset + NumConversion.BYTES_PER_LONG;
  private static final int fsBlockSizeOffset = fsFreeBlocksOffset + NumConversion.BYTES_PER_LONG;
  private static final int serializedSizeBytes = fsBlockSizeOffset + NumConversion.BYTES_PER_INT;

  public NodeInfo(long fsTotalBlocks, long fsUsedBlocks, long fsFreeBlocks, int fsBlockSize) {
    this.fsTotalBlocks = fsTotalBlocks;
    this.fsUsedBlocks = fsUsedBlocks;
    this.fsFreeBlocks = fsFreeBlocks;
    this.fsBlockSize = fsBlockSize;
  }

  public long getFSTotalBlocks() {
    return fsTotalBlocks;
  }

  public long getFSUsedBlocks() {
    return fsUsedBlocks;
  }

  public long getFSFreeBlocks() {
    return fsFreeBlocks;
  }

  public long getFSTotalBytes() {
    return fsTotalBlocks * fsBlockSize;
  }

  public long getFSUsedBytes() {
    return fsUsedBlocks * fsBlockSize;
  }

  public long getFSFreeBytes() {
    return getFSFreeBlocks() * fsBlockSize;
  }

  public int getFSBlockSize() {
    return fsBlockSize;
  }

  public String toString() {
    return fsTotalBlocks + ":" + fsUsedBlocks + ":" + fsFreeBlocks + ":" + fsBlockSizeOffset;
  }

  public byte[] toArray() {
    byte[] b;

    b = new byte[serializedSizeBytes];
    NumConversion.longToBytes(fsTotalBlocks, b, fsTotalBlocksOffset);
    NumConversion.longToBytes(fsUsedBlocks, b, fsUsedBlocksOffset);
    NumConversion.longToBytes(fsFreeBlocks, b, fsFreeBlocksOffset);
    NumConversion.intToBytes(fsBlockSize, b, fsBlockSizeOffset);
    return b;
  }

  public static NodeInfo fromArray(byte[] b) {
    long fsTotalBlocks;
    long fsUsedBlocks;
    long fsFreeBlocks;
    int fsBlockSize;

    fsTotalBlocks = NumConversion.bytesToLong(b, fsTotalBlocksOffset);
    fsUsedBlocks = NumConversion.bytesToLong(b, fsUsedBlocksOffset);
    fsFreeBlocks = NumConversion.bytesToLong(b, fsFreeBlocksOffset);
    fsBlockSize = NumConversion.bytesToInt(b, fsBlockSizeOffset);
    return new NodeInfo(fsTotalBlocks, fsUsedBlocks, fsFreeBlocks, fsBlockSize);
  }
}
