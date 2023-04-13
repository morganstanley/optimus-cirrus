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
import java.util.ArrayList;
import java.util.List;

import com.ms.silverking.cloud.dht.ForwardingMode;
import com.ms.silverking.cloud.dht.common.DHTKey;
import com.ms.silverking.cloud.dht.common.MessageType;
import com.ms.silverking.cloud.dht.common.SimpleKey;
import com.ms.silverking.cloud.dht.daemon.storage.KeyAndVersionChecksum;
import com.ms.silverking.cloud.dht.daemon.storage.convergence.AbstractChecksumNode;
import com.ms.silverking.cloud.dht.daemon.storage.convergence.ChecksumNode;
import com.ms.silverking.cloud.dht.daemon.storage.convergence.ConvergencePoint;
import com.ms.silverking.cloud.dht.daemon.storage.convergence.LeafChecksumNode;
import com.ms.silverking.cloud.dht.daemon.storage.convergence.NonLeafChecksumNode;
import com.ms.silverking.cloud.dht.daemon.storage.convergence.RingID;
import com.ms.silverking.cloud.dht.daemon.storage.convergence.RingIDAndVersionPair;
import com.ms.silverking.cloud.ring.RingRegion;
import com.ms.silverking.collection.Pair;
import com.ms.silverking.id.UUIDBase;
import com.ms.silverking.numeric.NumConversion;

public class ProtoChecksumTreeMessageGroup extends ProtoMessageGroup {
  private final ByteBuffer dataByteBuffer;

  private static final int dataBufferIndex = 0;
  private static final int dataBufferSize = 6 * NumConversion.BYTES_PER_LONG + RingID.BYTES;
  private static final int uuidMSLOffset = 0;
  private static final int uuidLSLOffset = uuidMSLOffset + NumConversion.BYTES_PER_LONG;
  private static final int dhtConfigVersionOffset = uuidLSLOffset + NumConversion.BYTES_PER_LONG;
  private static final int ringIDOffset = dhtConfigVersionOffset + NumConversion.BYTES_PER_LONG;
  private static final int ringConfigVersionOffset = ringIDOffset + RingID.BYTES;
  private static final int configInstanceVersionOffset = ringConfigVersionOffset + NumConversion.BYTES_PER_LONG;
  private static final int versionOffset = configInstanceVersionOffset + NumConversion.BYTES_PER_LONG;

  private static final int deadlineRelativeMillis = 10 * 60 * 1000;

  public ProtoChecksumTreeMessageGroup(UUIDBase uuid,
                                       long context,
                                       ConvergencePoint cp,
                                       byte[] originator,
                                       ChecksumNode root,
                                       int bufferSize) {
    super(MessageType.CHECKSUM_TREE, uuid, context, originator, deadlineRelativeMillis, ForwardingMode.FORWARD);

    dataByteBuffer = ByteBuffer.allocate(dataBufferSize + bufferSize);
    bufferList.add(dataByteBuffer);
    dataByteBuffer.putLong(uuid.getMostSignificantBits());
    dataByteBuffer.putLong(uuid.getLeastSignificantBits());
    dataByteBuffer.putLong(cp.getDHTConfigVersion());
    cp.getRingIDAndVersionPair().getRingID().writeToBuffer(dataByteBuffer);
    dataByteBuffer.putLong(cp.getRingIDAndVersionPair().getRingVersionPair().getV1());
    dataByteBuffer.putLong(cp.getRingIDAndVersionPair().getRingVersionPair().getV2());
    dataByteBuffer.putLong(cp.getDataVersion());
    serialize(dataByteBuffer, root);
  }

  @Override
  public boolean isNonEmpty() {
    return true;
  }

  private static long getDHTConfigVersion(MessageGroup messageGroup) {
    return messageGroup.getBuffers()[dataBufferIndex].getLong(dhtConfigVersionOffset);
  }

  private static RingID getRingID(MessageGroup messageGroup) {
    return RingID.readFromBuffer(messageGroup.getBuffers()[dataBufferIndex], ringIDOffset);
  }

  private static Pair<Long, Long> getRingVersionPair(MessageGroup messageGroup) {
    return new Pair(messageGroup.getBuffers()[dataBufferIndex].getLong(ringConfigVersionOffset),
                    messageGroup.getBuffers()[dataBufferIndex].getLong(configInstanceVersionOffset));
  }

  private static RingIDAndVersionPair getRingIDAndVersion(MessageGroup messageGroup) {
    return new RingIDAndVersionPair(getRingID(messageGroup), getRingVersionPair(messageGroup));
  }

  private static long getVersion(MessageGroup messageGroup) {
    return messageGroup.getBuffers()[dataBufferIndex].getLong(versionOffset);
  }

  public static ConvergencePoint getConvergencePoint(MessageGroup messageGroup) {
    return new ConvergencePoint(
        getDHTConfigVersion(messageGroup),
        getRingIDAndVersion(messageGroup),
        getVersion(messageGroup));
  }

  // Serialization

  enum NodeType {NON_LEAF, LEAF, NULL}

  public static void serialize(ByteBuffer buffer, ChecksumNode node) {
    if (node == null) {
      writeNullRoot(buffer); // only supported for root of tree
    } else {
      if (node instanceof NonLeafChecksumNode) {
        writeNodeHeader(buffer, NodeType.NON_LEAF, node);
        List<? extends ChecksumNode> children = node.getChildren();
        for (ChecksumNode child : children) {
          serialize(buffer, child);
        }
      } else if (node instanceof LeafChecksumNode) {
        writeNodeHeader(buffer, NodeType.LEAF, node);
        writeKeyAndVersionChecksums(buffer, (LeafChecksumNode) node);
      } else {
        throw new RuntimeException("Unsupported node type: " + node.getClass());
      }
    }
  }

  private static void writeKeyAndVersionChecksums(ByteBuffer buffer, LeafChecksumNode node) {
    List<KeyAndVersionChecksum> kvcList = node.getKeyAndVersionChecksums();
    buffer.putInt(kvcList.size());
    for (KeyAndVersionChecksum kvc : kvcList) {
      writeKeyValueChecksum(buffer, kvc);
    }
  }

  private static void writeKeyValueChecksum(ByteBuffer buffer, KeyAndVersionChecksum kvc) {
    writeKey(buffer, kvc.getKey());
    buffer.putLong(kvc.getVersionChecksum());
    buffer.putLong(kvc.getSegmentNumber());
  }

  private static void writeNodeHeader(ByteBuffer buffer, NodeType type, ChecksumNode node) {
    buffer.put((byte) type.ordinal());
    writeRegion(buffer, node.getRegion());
    buffer.putInt(node.getChildren().size());
  }

  private static void writeNullRoot(ByteBuffer buffer) {
    buffer.put((byte) NodeType.NULL.ordinal()); // only supported for root of tree
  }

  private static void writeRegion(ByteBuffer buffer, RingRegion region) {
    buffer.putLong(region.getStart());
    buffer.putLong(region.getEnd());
  }

  private static void writeKey(ByteBuffer buffer, DHTKey key) {
    buffer.putLong(key.getMSL());
    buffer.putLong(key.getLSL());
  }

  // Deserialization

  public static ChecksumNode deserialize(MessageGroup messageGroup) {
    ByteBuffer dataBuffer = messageGroup.getBuffers()[dataBufferIndex];
    dataBuffer.getLong(); // uuid msl
    dataBuffer.getLong(); // uuid lsl
    dataBuffer.getLong(); // dhtConfigVersion msb
    dataBuffer.getLong(); // ringID msb
    dataBuffer.getLong(); // ringID lsb
    dataBuffer.getLong(); // ringVersion
    dataBuffer.getLong(); // ringVersion
    dataBuffer.getLong(); // version
    return deserialize(dataBuffer);
  }

  public static ChecksumNode deserialize(ByteBuffer buffer) {
    RingRegion region = null;
    int numChildren = 0;
    AbstractChecksumNode root;

    NodeType type = NodeType.values()[buffer.get()];
    if (type != NodeType.NULL) {
      region = deserializeRegion(buffer);
      numChildren = buffer.getInt();
    }
    switch (type) {
      case NON_LEAF:
        List<ChecksumNode> children = new ArrayList(numChildren);
        for (int i = 0; i < numChildren; i++) {
          children.add(deserialize(buffer));
        }
        root = new NonLeafChecksumNode(region, children);
        break;
      case LEAF:
        List<KeyAndVersionChecksum> keyAndVersionChecksums = deserializeKeyAndVersionChecksums(buffer);
        root = new LeafChecksumNode(region, keyAndVersionChecksums);
        break;
      case NULL:
        root = null;
        break;
      default:
        throw new RuntimeException("Unsupported NodeType " + type);
    }
    if (root != null) {
      root.freezeIfNotFrozen();
    }
    return root;
  }

  private static RingRegion deserializeRegion(ByteBuffer buffer) {
    return new RingRegion(buffer.getLong(), buffer.getLong());
  }

  private static DHTKey deserializeKey(ByteBuffer buffer) {
    return new SimpleKey(buffer.getLong(), buffer.getLong());
  }

  private static List<KeyAndVersionChecksum> deserializeKeyAndVersionChecksums(ByteBuffer buffer) {
    int kvcListSize = buffer.getInt();
    List<KeyAndVersionChecksum> kvcList = new ArrayList(kvcListSize);
    for (int i = 0; i < kvcListSize; i++) {
      kvcList.add(deserializeKeyAndVersionChecksum(buffer));
    }
    return kvcList;
  }

  private static KeyAndVersionChecksum deserializeKeyAndVersionChecksum(ByteBuffer buffer) {
    DHTKey key = deserializeKey(buffer);
    long versionChecksum = buffer.getLong();
    long segmentNumber = buffer.getLong();
    return new KeyAndVersionChecksum(key, versionChecksum, segmentNumber);
  }
}
