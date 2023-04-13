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
package com.ms.silverking.cloud.dht.net.protocol;

import java.nio.ByteBuffer;

import com.ms.silverking.cloud.dht.ValueCreator;
import com.ms.silverking.cloud.dht.client.ChecksumType;
import com.ms.silverking.cloud.dht.common.CCSSUtil;
import com.ms.silverking.numeric.NumConversion;

public class PutBaseMessageFormat extends KeyValueMessageFormat {
  // options buffer
  public static final int versionSize = NumConversion.BYTES_PER_LONG;
  public static final int lockSecondsSize = NumConversion.BYTES_PER_SHORT;
  public static final int ccssSize = 2;
  public static final int valueCreatorSize = ValueCreator.BYTES;

  public static final int versionOffset = 0;
  public static final int requiredPreviousVersionOffset = versionOffset + versionSize;
  public static final int lockSecondsOffset = requiredPreviousVersionOffset + versionSize;
  public static final int ccssOffset = lockSecondsOffset + lockSecondsSize;
  public static final int valueCreatorOffset = ccssOffset + ccssSize;

  public static ChecksumType getChecksumType(ByteBuffer optionsByteBuffer) {
    return CCSSUtil.getChecksumType(optionsByteBuffer.getShort(ccssOffset));
  }

  public static byte getStorageState(ByteBuffer optionsByteBuffer) {
    return CCSSUtil.getStorageState(optionsByteBuffer.getShort(ccssOffset));
  }
}
