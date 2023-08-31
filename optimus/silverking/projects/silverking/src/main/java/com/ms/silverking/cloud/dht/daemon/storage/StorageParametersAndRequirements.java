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
package com.ms.silverking.cloud.dht.daemon.storage;

import com.ms.silverking.cloud.dht.client.Compression;
import com.ms.silverking.cloud.dht.common.CCSSUtil;
import com.ms.silverking.cloud.dht.serverside.SSStorageParametersAndRequirements;

public class StorageParametersAndRequirements extends StorageParameters {
  private final long requiredPreviousVersion;

  public StorageParametersAndRequirements(
      long version,
      int uncompressedSize,
      int compressedSize,
      short ccss,
      byte[] checksum,
      byte[] valueCreator,
      long creationTime,
      long requiredPreviousVersion,
      short lockSeconds) {
    super(
        version,
        uncompressedSize,
        compressedSize,
        lockSeconds,
        ccss,
        checksum,
        valueCreator,
        creationTime);
    this.requiredPreviousVersion = requiredPreviousVersion;
  }

  public static StorageParametersAndRequirements fromSSStorageParametersAndRequirements(
      SSStorageParametersAndRequirements sp) {
    if (sp instanceof StorageParameters) {
      return (StorageParametersAndRequirements) sp;
    } else {
      return new StorageParametersAndRequirements(
          sp.getVersion(),
          sp.getUncompressedSize(),
          sp.getCompressedSize(),
          CCSSUtil.createCCSS(sp.getCompression(), sp.getChecksumType(), sp.getStorageState()),
          sp.getChecksum(),
          sp.getValueCreator(),
          sp.getCreationTime(),
          sp.getRequiredPreviousVersion(),
          sp.getLockSeconds());
    }
  }

  public static StorageParametersAndRequirements fromSSStorageParametersAndRequirements(
      SSStorageParametersAndRequirements sp,
      int uncompressedSize,
      int compressedSize,
      Compression compression) {
    if (sp instanceof StorageParameters) {
      return (StorageParametersAndRequirements) sp;
    } else {
      return new StorageParametersAndRequirements(
          sp.getVersion(),
          uncompressedSize,
          compressedSize,
          CCSSUtil.createCCSS(compression, sp.getChecksumType(), sp.getStorageState()),
          sp.getChecksum(),
          sp.getValueCreator(),
          sp.getCreationTime(),
          sp.getRequiredPreviousVersion(),
          sp.getLockSeconds());
    }
  }

  public long getRequiredPreviousVersion() {
    return requiredPreviousVersion;
  }

  @Override
  public String toString() {
    return super.toString() + ":" + requiredPreviousVersion;
  }
}
