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

public class KeyValueMessageFormat extends KeyedMessageFormat {
  public static final int bufferIndexSize = NumConversion.BYTES_PER_INT;
  public static final int bufferOffsetSize = NumConversion.BYTES_PER_INT;

  public static final int bufferIndexOffset = KeyedMessageFormat.size;
  public static final int bufferOffsetOffset = bufferIndexOffset + bufferIndexSize;

  public static final int size = KeyedMessageFormat.size + bufferIndexSize + bufferOffsetSize;

  // buffer format
}
