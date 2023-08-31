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

import com.ms.silverking.numeric.NumConversion;

public class RetrievalResponseMessageFormat extends KeyValueMessageFormat {
  // key buffer

  public static final int resultLengthSize = NumConversion.BYTES_PER_INT;

  public static final int resultLengthOffset = KeyValueMessageFormat.size;
  public static final int size = resultLengthOffset + resultLengthSize;

  // options buffer

  public static final int retrievalTypeWaitModeSize = 1;
  public static final int internalOptionsSize = 1;
  public static final int vcMinSize = NumConversion.BYTES_PER_LONG;
  public static final int vcMaxSize = NumConversion.BYTES_PER_LONG;
  public static final int vcModeSize = 1;
  public static final int vcMaxStorageTimeSize = NumConversion.BYTES_PER_LONG;

  public static final int retrievalTypeWaitModeOffset = 0;
  public static final int miscOptionsOffset =
      retrievalTypeWaitModeOffset + retrievalTypeWaitModeSize;
  public static final int vcMinOffset = miscOptionsOffset + internalOptionsSize;
  public static final int vcMaxOffset = vcMinOffset + vcMinSize;
  public static final int vcModeOffset = vcMaxOffset + vcMaxSize;
  public static final int vcMaxStorageTimeOffset = vcModeOffset + vcModeSize;

  public static final int optionBytesSize =
      retrievalTypeWaitModeSize
          + internalOptionsSize
          + vcMinSize
          + vcMaxSize
          + vcModeSize
          + vcMaxStorageTimeSize;
}
