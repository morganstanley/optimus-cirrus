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
package com.ms.silverking.cloud.dht.daemon.storage.convergence;

import com.ms.silverking.collection.Pair;
import com.ms.silverking.collection.Triple;
import com.ms.silverking.numeric.NumConversion;
import com.ms.silverking.numeric.NumUtil;
import com.ms.silverking.util.IncomparableException;

/**
 * Groups a RingID (hash of the ring name) and version pair of the ring into a single object.
 */
public class RingIDAndVersionPair implements Comparable<RingIDAndVersionPair> {
  private final RingID ringID;
  private final Pair<Long, Long> ringVersionPair;

  public static final int BYTES = RingID.BYTES + NumConversion.BYTES_PER_LONG * 2;

  public RingIDAndVersionPair(RingID ringID, Pair<Long, Long> ringVersionPair) {
    this.ringID = ringID;
    this.ringVersionPair = ringVersionPair;
  }

  public static RingIDAndVersionPair fromRingNameAndVersionPair(Triple<String, Long, Long> ringNameAndVersionPair) {
    return new RingIDAndVersionPair(RingID.nameToRingID(ringNameAndVersionPair.getV1()),
        new Pair<>(ringNameAndVersionPair.getV2(), ringNameAndVersionPair.getV3()));
  }

  public RingID getRingID() {
    return ringID;
  }

  public Pair<Long, Long> getRingVersionPair() {
    return ringVersionPair;
  }

  @Override
  public int hashCode() {
    return ringID.hashCode() ^ ringVersionPair.hashCode();
  }

  @Override
  public boolean equals(Object other) {
    RingIDAndVersionPair oRingIDAndVersion;

    oRingIDAndVersion = (RingIDAndVersionPair) other;
    return ringID.equals(oRingIDAndVersion.ringID) && ringVersionPair.equals(oRingIDAndVersion.ringVersionPair);
  }

  @Override
  public String toString() {
    return ringID.toString() + ":" + ringVersionPair.getV1() + ":" + ringVersionPair.getV2();
  }

  @Override
  public int compareTo(RingIDAndVersionPair o) {
    if (!this.ringID.equals(o.ringID)) {
      throw new IncomparableException("RingIDs not equal");
    } else {
      return NumUtil.compare(this.getRingVersionPair(), o.getRingVersionPair());
    }
  }
}
