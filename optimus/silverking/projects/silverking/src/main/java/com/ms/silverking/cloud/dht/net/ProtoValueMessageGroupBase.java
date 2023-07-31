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
package com.ms.silverking.cloud.dht.net;

import java.nio.ByteBuffer;

import com.ms.silverking.cloud.dht.ForwardingMode;
import com.ms.silverking.cloud.dht.common.DHTConstants;
import com.ms.silverking.cloud.dht.common.MessageType;
import com.ms.silverking.cloud.dht.trace.SkTraceId;
import com.ms.silverking.id.UUIDBase;

/**
 * ProtoMessageGroup for messages that contains values e.g. put and retrieval response.
 * Values are stored in a list of ByteBuffers. Smaller values may be copied into a shared ByteBuffer.
 * Larger values are stored in dedicated ByteBuffers, possibly by creating a zero-copy ByteBuffer
 * wrapping existing storage of the value.
 */
abstract class ProtoValueMessageGroupBase extends ProtoKeyedMessageGroup {
  protected final int opSize;
  protected ByteBuffer valueBuffer;
  protected int curMultiValueBufferIndex;
  protected int totalValueBytes;

  // TODO (OPTIMUS-0000): valueBufferSize should be computed based on some external information
  protected static final int valueBufferSize = 16 * 1024;
  // Maximum number of value bytes to allow in a single message. The sum of the length of all values in
  // a message must be less than this limit. Operations that need to transfer more bytes over the
  // network will use multiple messages to do so.
  // maxValueSegmentSize is used below, because this needs to be at least that, but
  // we could consider higher and make that a minimum.
  public static final int maxValueBytesPerMessage = DHTConstants.defaultFragmentationThreshold + 1024 * 1024;
  // Values below this size are copied into a common buffer. Values above this size are contained in dedicated
  // buffers, possibly with zero-copy
  protected static final int dedicatedBufferSizeThreshold = 16 * 1024;

  static {
    // Values without dedicated buffers need to be able to fit into the shared buffer
    if (dedicatedBufferSizeThreshold > valueBufferSize) {
      throw new RuntimeException("dedicatedBufferSizeThreshold > valueBufferSize not supported");
    }
  }

  public ProtoValueMessageGroupBase(MessageType type,
                                    UUIDBase uuid,
                                    long context,
                                    int opSize,
                                    ByteBuffer optionsByteBuffer,
                                    int additionalBytesPerKey,
                                    byte[] originator,
                                    int deadlineRelativeMillis,
                                    ForwardingMode forward,
                                    SkTraceId maybeTraceID) {
    super(
        type,
        uuid,
        context,
        optionsByteBuffer,
        opSize,
        additionalBytesPerKey,
        originator,
        deadlineRelativeMillis,
        forward,
        maybeTraceID);
    this.opSize = opSize;
    bufferList.add(optionsByteBuffer);
    valueBuffer = null;
    curMultiValueBufferIndex = -1;
  }

  static int getOptionsByteBufferBaseOffset(boolean hasTraceID) {
    // No extra thing is added at this layer, so baseOffset = 0 + previousLayerOffset
    return 0 + ProtoKeyedMessageGroup.getOptionsByteBufferBaseOffset(hasTraceID);
  }

  public boolean canBeAdded(int valueSize) {
    // Check that we are below maxValueBytesPerMessage
    // Exception is granted for single values of any length
    // (If fragmentation threshold is very high, we need to handle very large values)
    return (totalValueBytes == 0) || (totalValueBytes + valueSize < maxValueBytesPerMessage);
  }

  public int currentValueBytes() {
    return totalValueBytes;
  }

  protected boolean addDedicatedBuffer(ByteBuffer buffer) {
    if (bufferList.size() >= Integer.MAX_VALUE) {
      return false;
    } else {
      bufferList.add(buffer);
      return true;
    }
  }

  protected boolean addMultiValueBuffer(ByteBuffer buffer) {
    if (bufferList.size() >= Integer.MAX_VALUE) {
      return false;
    } else {
      curMultiValueBufferIndex = bufferList.size();
      bufferList.add(buffer);
      return true;
    }
  }
}
