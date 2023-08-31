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

import java.util.Set;

import com.google.common.collect.ImmutableSet;
import com.ms.silverking.cloud.dht.SecondaryTarget;
import com.ms.silverking.cloud.dht.client.SecondaryTargetType;
import com.ms.silverking.numeric.NumConversion;
import it.unimi.dsi.fastutil.bytes.ByteArrayList;
import it.unimi.dsi.fastutil.bytes.ByteList;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SecondaryTargetSerializer {

  private static final int initialBufferSize = 64;

  private static Logger log = LoggerFactory.getLogger(SecondaryTargetSerializer.class);

  public static int serializedLength(Set<SecondaryTarget> specs) {
    return specs == null ? 0 : serialize(specs).length;
  }

  public static byte[] serialize(Set<SecondaryTarget> specs) {
    ByteList list;

    list = new ByteArrayList(initialBufferSize);
    for (SecondaryTarget spec : specs) {
      byte[] targetBytes;

      list.add((byte) spec.getType().ordinal());
      targetBytes = spec.getTarget().getBytes();
      list.addElements(list.size(), NumConversion.shortToBytes((short) targetBytes.length));
      list.addElements(list.size(), targetBytes);
    }
    return list.toByteArray();
  }

  public static Set<SecondaryTarget> deserialize(byte[] multiDef) {
    try {
      ImmutableSet.Builder<SecondaryTarget> specs;
      int i;

      specs = ImmutableSet.builder();
      i = 0;
      while (i < multiDef.length) {
        SecondaryTargetType type;
        int targetSize;
        byte[] targetBytes;
        String target;

        type = SecondaryTargetType.values()[multiDef[i++]];
        targetSize = NumConversion.bytesToShort(multiDef, i);
        i += NumConversion.BYTES_PER_SHORT;
        targetBytes = new byte[targetSize];
        System.arraycopy(multiDef, i, targetBytes, 0, targetSize);
        i += targetSize;
        target = new String(targetBytes);
        specs.add(new SecondaryTarget(type, target));
      }
      return specs.build();
    } catch (Exception e) {
      log.error("", e);
      return ImmutableSet.of();
    }
  }
}
