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

import com.ms.silverking.cloud.dht.PutOptions;
import com.ms.silverking.cloud.dht.client.ChecksumType;
import com.ms.silverking.cloud.dht.net.SecondaryTargetSerializer;
import com.ms.silverking.numeric.NumConversion;

public class PutMessageFormat extends PutBaseMessageFormat {
  // key buffer entry

  public static final int uncompressedValueLengthSize = NumConversion.BYTES_PER_INT;
  public static final int compressedValueLengthSize = NumConversion.BYTES_PER_INT;

  public static final int uncompressedValueLengthOffset = KeyValueMessageFormat.size;
  public static final int compressedValueLengthOffset = uncompressedValueLengthOffset + uncompressedValueLengthSize;
  public static final int checksumOffset = compressedValueLengthOffset + compressedValueLengthSize;

  public static int size(ChecksumType checksumType) {
    return checksumOffset + checksumType.length();
  }

  // options buffer

  public static final int secondaryTargetDataOffset = valueCreatorOffset + valueCreatorSize;

  private static final int optionBaseBytes = versionSize * 2 + lockSecondsSize + ccssSize + valueCreatorSize;

  // TODO (OPTIMUS-43373): Remove this legacy comment once client side is using new puts
  // if messageType is LEGACY_PUT or LEGACY_PUT_TRACE, then the offset returned is the actual userdata's offset
  // else the userdata length's offset is returned
  public static final int userDataLengthOffset(int stLength) {
    return secondaryTargetDataOffset + stLength + NumConversion.BYTES_PER_SHORT;
  }

  public static final int getOptionsBufferLength(PutOptions putOptions) {
    // TODO (OPTIMUS-43373): Add authorizationUser into calculations
    return optionBaseBytes +
           NumConversion.BYTES_PER_SHORT +
           SecondaryTargetSerializer.serializedLength(putOptions.getSecondaryTargets()) +
           getUserDataLength(putOptions);
  }

  private static final int getUserDataLength(PutOptions putOptions) {
    int userDataLength = putOptions.getUserData() == null ? 0 : putOptions.getUserData().length;
    // 1 because userDataLength occupies 1 byte
    // TODO (OPTIMUS-43373): Add 1 when we change the format and add authorizationUser into calculations
    return userDataLength;
  }

  private static final int getAuthorizationUserLength(PutOptions putOptions) {
    int authorizationUserLength = putOptions.getAuthorizationUser() == null ? 0 : putOptions.getAuthorizationUser().length;
    // NumConversion.BYTES_PER_INT because authorizationUserLength occupies 4 bytes
    return NumConversion.BYTES_PER_INT + authorizationUserLength;
  }
}