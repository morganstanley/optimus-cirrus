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
package com.ms.silverking.cloud.dht.client.impl;

import com.ms.silverking.cloud.dht.CreationTime;
import com.ms.silverking.cloud.dht.ValueCreator;
import com.ms.silverking.cloud.dht.client.ChecksumType;
import com.ms.silverking.cloud.dht.client.Compression;
import com.ms.silverking.cloud.dht.client.MetaData;
import com.ms.silverking.cloud.dht.common.MetaDataUtil;
import com.ms.silverking.cloud.dht.common.SystemTimeUtil;

public class SimpleMetaData implements MetaData {
  private final int storedLength;
  private final int uncompressedLength;
  private final long version;
  private final long creationTime;
  private final ValueCreator valueCreator;
  private final short lockSeconds;
  //TODO (OPTIMUS-43326): Remove userdata. It should be part of value rather than metadata
  private final byte[] userData;
  private final byte[] checksum;
  private final Compression compression;
  private final ChecksumType checksumType;

  public SimpleMetaData(int storedLength, int uncompressedLength, long version, long creationTime,
      ValueCreator valueCreator, short lockSeconds, byte[] userData, byte[] checksum, Compression compression,
      ChecksumType checksumType) {
    this.storedLength = storedLength;
    this.uncompressedLength = uncompressedLength;
    this.version = version;
    this.creationTime = creationTime;
    this.valueCreator = valueCreator;
    this.lockSeconds = lockSeconds;
    this.userData = userData;
    this.checksum = checksum;
    this.compression = compression;
    this.checksumType = checksumType;
  }

  public SimpleMetaData(MetaData metaData) {
    this(metaData.getStoredLength(), metaData.getUncompressedLength(), metaData.getVersion(),
        metaData.getCreationTime().inNanos(), metaData.getCreator(), metaData.getLockSeconds(), metaData.getUserData(),
        metaData.getChecksum(), metaData.getCompression(), metaData.getChecksumType());
  }

  public SimpleMetaData setValueCreator(ValueCreator _valueCreator) {
    return new SimpleMetaData(storedLength, uncompressedLength, version, creationTime, _valueCreator, lockSeconds,
        userData, checksum, compression, checksumType);
  }

  public SimpleMetaData setStoredLength(int _storedLength) {
    return new SimpleMetaData(_storedLength, uncompressedLength, version, creationTime, valueCreator, lockSeconds,
        userData, checksum, compression, checksumType);
  }

  public SimpleMetaData setUncompressedLength(int _uncompressedLength) {
    return new SimpleMetaData(storedLength, _uncompressedLength, version, creationTime, valueCreator, lockSeconds,
        userData, checksum, compression, checksumType);
  }

  public SimpleMetaData setChecksumTypeAndChecksum(ChecksumType checksumType, byte[] checksum) {
    return new SimpleMetaData(storedLength, uncompressedLength, version, creationTime, valueCreator, lockSeconds,
        userData, checksum, compression, checksumType);
  }

  @Override
  public int getStoredLength() {
    return storedLength;
  }

  @Override
  public int getUncompressedLength() {
    return uncompressedLength;
  }

  @Override
  public long getVersion() {
    return version;
  }

  @Override
  public CreationTime getCreationTime() {
    return new CreationTime(creationTime);
  }

  @Override
  public ValueCreator getCreator() {
    return valueCreator;
  }

  @Override
  public short getLockSeconds() {
    return lockSeconds;
  }

  @Override
  public long getLockMillisRemaining() {
    long nanosRemaining;
    long millisRemaining;

    nanosRemaining =
        (creationTime + (long) lockSeconds * 1_000_000_000L) - SystemTimeUtil.skSystemTimeSource.absTimeNanos();
    millisRemaining = nanosRemaining / 1_000_000L;
    if (millisRemaining > 0) {
      return millisRemaining;
    } else {
      // Ensure that we don't round down to zero. We want to ensure that a non-zero time in nanos
      // is interpreted as locked, not free.
      if (nanosRemaining > 0) {
        return 1L;
      } else {
        return 0;
      }
    }
  }

  @Override
  public boolean isLocked() {
    return SystemTimeUtil.skSystemTimeSource.absTimeNanos() <= creationTime + (long) lockSeconds * 1_000_000_000L;
  }

  @Override
  public boolean isInvalidation() {
    return MetaDataUtil.isInvalidation(checksumType, checksum);
  }

  @Override
  public byte[] getUserData() {
    return userData;
  }

  @Override
  public byte[] getChecksum() {
    return checksum;
  }

  @Override
  public String toString(boolean labeled) {
    return MetaDataTextUtil.toMetaDataString(this, labeled);
  }

  @Override
  public String toString() {
    return toString(true);
  }

  @Override
  public Compression getCompression() {
    return compression;
  }

  @Override
  public ChecksumType getChecksumType() {
    return checksumType;
  }
}
