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

import com.ms.silverking.cloud.dht.RetrievalOptions;
import com.ms.silverking.cloud.dht.net.SecondaryTargetSerializer;
import com.ms.silverking.numeric.NumConversion;

public class RetrievalMessageFormat extends KeyedMessageFormat {
  public static final int stDataOffset = RetrievalResponseMessageFormat.optionBytesSize;

  /*
   * Calculate the length of the byte buffer required to store the retrieval options
   * data is stored in the following order with specific offsets.
   * Offsets are point in the byte[] that tells where data for a specific value is stored (start index)
   * i.e. secondaryTargetLengthOffset 28 means that data for the length is stored from index 28 (inclusive) onwards
   *
   * | Size (bytes) |                                               |
   * ---------------------------------------------------------------|
   * |     2       | secondaryTarget length (short)                 |
   * |    ANY_1    | secondaryTarget data                           |
   * |     4       | user options length (int)                      |
   * |    ANY_2    | user options                                   |
   * |    4        | authorization user length (int)                |
   * |    ANY_3    | authorization user                             |
   * |    4        | zone id length (int)                           |
   * |    ANY_4    | zone id                                        |
   * */
  public static final int getOptionsBufferLength(RetrievalOptions retrievalOptions) {

    int secondaryTargetLength =
        NumConversion.BYTES_PER_SHORT
            + SecondaryTargetSerializer.serializedLength(retrievalOptions.getSecondaryTargets());

    int userOptionsLength = NumConversion.BYTES_PER_INT;
    if (retrievalOptions.getUserOptions() != RetrievalOptions.noUserOptions)
      userOptionsLength += retrievalOptions.getUserOptions().length;

    int authorizationUserLength = NumConversion.BYTES_PER_INT;
    if (retrievalOptions.getAuthorizationUser() != RetrievalOptions.noAuthorizationUser)
      authorizationUserLength += retrievalOptions.getAuthorizationUser().length;

    int zoneIdLength = NumConversion.BYTES_PER_INT;
    if (retrievalOptions.getZoneId() != null && !retrievalOptions.getZoneId().isEmpty())
      zoneIdLength += retrievalOptions.getZoneId().length();

    return stDataOffset
        + secondaryTargetLength
        + userOptionsLength
        + authorizationUserLength
        + zoneIdLength;
  }
}
