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
package com.ms.silverking.cloud.dht.client;

import com.ms.silverking.cloud.dht.CreationTime;
import com.ms.silverking.cloud.dht.ValueCreator;

/**
 * Meta data associated with a stored value.
 */
public interface MetaData {
  /**
   * Length in bytes of the stored value. This length includes metadata.
   *
   * @return stored value length, in bytes
   */
  public int getStoredLength();

  /**
   * Length in bytes of the actual value stored ignoring compression.
   *
   * @return stored value length, ignoring compression, in bytes
   */
  public int getUncompressedLength();

  /**
   * Version of the value stored.
   *
   * @return version of the value stored
   */
  public long getVersion();

  /**
   * Time that value was created.
   *
   * @return time that value was created
   */
  public CreationTime getCreationTime();

  /**
   * The ValueCreator responsible for storing the value.
   *
   * @return ValueCreator responsible for storing the value
   */
  public ValueCreator getCreator();

  /**
   * Seconds to maintain advisory lock
   *
   * @return seconds to maintain advisory lock
   */
  public short getLockSeconds();

  /**
   * Check time remaining on advisory lock
   *
   * @return microseconds of lock remaining
   */
  public long getLockMillisRemaining();

  /**
   * Check advisory lock
   *
   * @return true if advisory lock is locked
   */
  public boolean isLocked();

  /**
   * Check if this is an invalidation
   *
   * @return true if this is an invalidation
   */
  public boolean isInvalidation();

  /**
   * User data associated with a value.
   *
   * @return user data associated with a value
   */
  //TODO (OPTIMUS-43326): Remove userdata. It should be part of value rather than metadata
  public byte[] getUserData();

  /**
   * A string representation of this MetaData.
   *
   * @param labeled specifies whether or not to label each MetaData member
   * @return string representation of this MetaData
   */
  public String toString(boolean labeled);

  /**
   * The stored checksum of this value.
   *
   * @return stored checksum of this value
   */
  public byte[] getChecksum();

  /**
   * The Compression used to stored this value.
   *
   * @return compression used to stored this value
   */
  public Compression getCompression();

  /**
   * The ChecksumType used to checksum this value.
   *
   * @return ChecksumType used to checksum this value
   */
  public ChecksumType getChecksumType();
}
