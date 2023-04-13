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

import com.ms.silverking.cloud.dht.ValueCreator;
import com.ms.silverking.cloud.dht.net.MessageGroupGlobals;
import com.ms.silverking.numeric.NumConversion;

public abstract class MessageFormat {
  public static final int preambleSize = MessageGroupGlobals.preamble.length;
  public static final int protocolVersionSize = 1;
  public static final int lengthSize = NumConversion.BYTES_PER_INT;
  public static final int typeSize = 1;
  public static final int optionsSize = 3;
  public static final int uuidMSLSize = NumConversion.BYTES_PER_LONG;
  public static final int uuidLSLSize = NumConversion.BYTES_PER_LONG;
  public static final int contextSize = NumConversion.BYTES_PER_LONG;
  public static final int originatorSize = ValueCreator.BYTES;
  public static final int deadlineRelativeMillisSize = NumConversion.BYTES_PER_INT;
  public static final int forwardSize = 1;

  public static final int preambleOffset = 0;
  public static final int protocolVersionOffset = preambleOffset + preambleSize;
  public static final int lengthOffset = protocolVersionOffset + protocolVersionSize;
  public static final int typeOffset = lengthOffset + lengthSize;
  public static final int optionsOffset = typeOffset + typeSize;
  public static final int uuidMSLOffset = optionsOffset + optionsSize;
  public static final int uuidLSLOffset = uuidMSLOffset + uuidMSLSize;
  public static final int contextOffset = uuidLSLOffset + uuidLSLSize;
  public static final int originatorOffset = contextOffset + contextSize;
  public static final int deadlineRelativeMillisOffset = originatorOffset + originatorSize;
  public static final int forwardOffset = deadlineRelativeMillisOffset + deadlineRelativeMillisSize;
  public static final int leadingBufferEndOffset = forwardOffset + forwardSize;
  // end is exclusive of the buffer

  public static final int leadingBufferSize = leadingBufferEndOffset;
}
