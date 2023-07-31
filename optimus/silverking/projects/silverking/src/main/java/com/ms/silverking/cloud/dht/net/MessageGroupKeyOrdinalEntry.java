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

import com.ms.silverking.cloud.dht.common.DHTKey;
import com.ms.silverking.cloud.dht.common.SimpleKey;
import com.ms.silverking.numeric.NumConversion;


/*
 * FUTURE - think about why this isn't a child of MessageGroupKeyEntry
 * think about GC etc.
 * Also, think about GC is MessageGroup*Entry classes and overall organization
 * of these classes, usage, etc.
 *
 * do we need equals and hashCode?
 */

public class MessageGroupKeyOrdinalEntry extends MessageGroupKeyEntry {
  private final DHTKey dhtKey;
  private final byte ordinal;

  public static final int bytesPerEntry = 2 * NumConversion.BYTES_PER_LONG + 1;

  private static final int lslOffset = NumConversion.BYTES_PER_LONG;
  private static final int responseOffset = lslOffset + NumConversion.BYTES_PER_LONG;

  // TODO (OPTIMUS-0000): probably eliminate the locally stored key since MessageGroupKeyEntry
  // can act as a key directly
  // also examine usage and getKey() usage in particular to see if we can
  // eliminate that call

  public MessageGroupKeyOrdinalEntry(long msl, long lsl, byte ordinal) {
    super(msl, lsl);
    this.dhtKey = new SimpleKey(msl, lsl);
    this.ordinal = ordinal;
  }

  public MessageGroupKeyOrdinalEntry(ByteBuffer buffer, int offset) {
    this(buffer.getLong(offset), buffer.getLong(offset + lslOffset), buffer.get(offset + responseOffset));
  }

  public MessageGroupKeyOrdinalEntry(DHTKey key, byte ordinal) {
    this(key.getMSL(), key.getLSL(), ordinal);
  }

  public DHTKey getKey() {
    return dhtKey;
  }
    
    /*
    public MessageGroupKeyOrdinalEntry(long msl, long lsl, byte ordinal) {
        this.dhtKey = new SimpleKey(msl, lsl);
        this.ordinal = ordinal;
    }

    public MessageGroupKeyOrdinalEntry(ByteBuffer buffer, int offset) {
        dhtKey = new SimpleKey(buffer.getLong(offset), buffer.getLong(offset + lslOffset));
        ordinal = buffer.get(offset + responseOffset);
    }

    public MessageGroupKeyOrdinalEntry(DHTKey key, byte ordinal) {
        this(key.getMSL(), key.getLSL(), ordinal);
    }
    
    public DHTKey getKey() {
        return dhtKey;
    }
    */

  public byte getOrdinal() {
    return ordinal;
  }

  @Override
  public String toString() {
    return dhtKey + ":" + ordinal;
  }
}
