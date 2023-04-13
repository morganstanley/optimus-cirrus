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
package com.ms.silverking.cloud.dht.daemon.storage.fsm;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.HashSet;
import java.util.Set;

import com.google.common.collect.ImmutableSet;
import com.ms.silverking.numeric.NumConversion;

public class InvalidatedOffsetsElement extends LTVElement {
  private static final boolean debugPersistence = false;

  public InvalidatedOffsetsElement(ByteBuffer buf) {
    super(buf);
  }

  public Set<Integer> getInvalidatedOffsets() {
    ByteBuffer valBuf;
    HashSet<Integer> invalidatedOffsets;

    valBuf = getValueBuffer();
    valBuf.order(ByteOrder.nativeOrder());
    invalidatedOffsets = new HashSet<>();
    while (valBuf.hasRemaining()) {
      invalidatedOffsets.add(valBuf.getInt());
    }
    return ImmutableSet.copyOf(invalidatedOffsets);
  }

  ////////////////////////////////////

  /*
   * Length   (4)
   * Type     (4)
   * Value:
   *  InvalidatedOffsets
   */

  public static InvalidatedOffsetsElement create(Set<Integer> invalidatedOffsets) {
    ByteBuffer elementBuf;
    int elementSize;
    int invalidatedOffsetsSizeBytes;

    invalidatedOffsetsSizeBytes = invalidatedOffsets.size() * NumConversion.BYTES_PER_INT;
    elementSize = NumConversion.BYTES_PER_INT * 2   // length + type
        + invalidatedOffsetsSizeBytes;

    elementBuf = ByteBuffer.allocate(elementSize);
    elementBuf = elementBuf.order(ByteOrder.nativeOrder());

    elementBuf.putInt(elementSize);
    elementBuf.putInt(FSMElementType.InvalidatedOffsets.ordinal());

    for (Integer io : invalidatedOffsets) {
      elementBuf.putInt(io);
    }

    elementBuf.rewind();

    return new InvalidatedOffsetsElement(elementBuf);
  }
}
