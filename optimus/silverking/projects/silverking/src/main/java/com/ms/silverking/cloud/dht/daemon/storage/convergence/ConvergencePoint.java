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

import java.nio.ByteBuffer;

import com.ms.silverking.collection.Pair;
import com.ms.silverking.numeric.NumConversion;
import com.ms.silverking.numeric.NumUtil;
import com.ms.silverking.util.IncomparableException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Objective of convergence: DHT configuration version, RingID, ring version, data version. */
public class ConvergencePoint implements Comparable<ConvergencePoint> {
  private final long dhtConfigVersion;
  private final RingIDAndVersionPair ringIDAndVersionPair;
  private final long dataVersion;

  private static Logger log = LoggerFactory.getLogger(ConvergencePoint.class);

  public ConvergencePoint(
      long dhtConfigVersion, RingIDAndVersionPair ringIDAndVersion, long dataVersion) {
    // this.dhtConfigVersion = dhtConfigVersion;
    // dhtConfigVersion is to be deprecated
    this.dhtConfigVersion =
        16; // dhtConfigVersion hasn't proven to be useful as the remaining members are
    // sufficient to describe a CP.
    this.ringIDAndVersionPair = ringIDAndVersion;
    this.dataVersion = dataVersion;
  }

  public ConvergencePoint dataVersion(long dataVersion) {
    return new ConvergencePoint(dhtConfigVersion, ringIDAndVersionPair, dataVersion);
  }

  public long getDHTConfigVersion() {
    return dhtConfigVersion;
  }

  public RingIDAndVersionPair getRingIDAndVersionPair() {
    return ringIDAndVersionPair;
  }

  public long getDataVersion() {
    return dataVersion;
  }

  @Override
  public int hashCode() {
    return NumUtil.longHashCode(dhtConfigVersion)
        ^ ringIDAndVersionPair.hashCode()
        ^ NumUtil.longHashCode(dataVersion);
  }

  @Override
  public boolean equals(Object other) {
    ConvergencePoint oCP;

    oCP = (ConvergencePoint) other;
    return dhtConfigVersion == oCP.dhtConfigVersion
        && ringIDAndVersionPair.equals(oCP.ringIDAndVersionPair)
        && dataVersion == oCP.dataVersion;
  }

  @Override
  public String toString() {
    return dhtConfigVersion + ":" + ringIDAndVersionPair.toString() + ":" + dataVersion;
  }

  @Override
  public int compareTo(ConvergencePoint o) {
    int c;

    c = Long.compare(dhtConfigVersion, o.dhtConfigVersion);
    if (c != 0) {
      return c;
    } else {
      try {
        return this.getRingIDAndVersionPair().compareTo(o.getRingIDAndVersionPair());
      } catch (IncomparableException ie) {
        // This should not happen since any time the ring name changes, it should
        // have been caused by a corresponding dhtConfig change which should
        // be caught above
        log.error("Unexpected IncomparableException", ie);
        throw ie;
      }
    }
  }

  //////////////////
  // serialization

  private static final int dhtConfigVersionOffset = 0;
  private static final int ringIDOffset = dhtConfigVersionOffset + NumConversion.BYTES_PER_LONG;
  private static final int ringConfigVersionOffset = ringIDOffset + RingID.BYTES;
  private static final int configInstanceVersionOffset =
      ringConfigVersionOffset + NumConversion.BYTES_PER_LONG;
  private static final int versionOffset =
      configInstanceVersionOffset + NumConversion.BYTES_PER_LONG;

  public static final int serializedSizeBytes =
      RingIDAndVersionPair.BYTES + NumConversion.BYTES_PER_LONG * 2;

  public void writeToBuffer(ByteBuffer dataByteBuffer) {
    dataByteBuffer.putLong(getDHTConfigVersion());
    getRingIDAndVersionPair().getRingID().writeToBuffer(dataByteBuffer);
    dataByteBuffer.putLong(getRingIDAndVersionPair().getRingVersionPair().getV1());
    dataByteBuffer.putLong(getRingIDAndVersionPair().getRingVersionPair().getV2());
    dataByteBuffer.putLong(getDataVersion());
  }

  public static ConvergencePoint readFromBuffer(ByteBuffer buf, int offset) {
    long dhtConfigVersion;
    RingID ringID;
    long ringConfigVersion;
    long configInstanceVersion;
    RingIDAndVersionPair ringIDAndVersion;
    long dataVersion;

    dhtConfigVersion = buf.getLong(offset + dhtConfigVersionOffset);
    ringID = RingID.readFromBuffer(buf, offset + ringIDOffset);
    ringConfigVersion = buf.getLong(offset + ringConfigVersionOffset);
    configInstanceVersion = buf.getLong(offset + configInstanceVersionOffset);
    ringIDAndVersion =
        new RingIDAndVersionPair(ringID, new Pair<>(ringConfigVersion, configInstanceVersion));
    dataVersion = buf.getLong(offset + versionOffset);
    return new ConvergencePoint(dhtConfigVersion, ringIDAndVersion, dataVersion);
  }
}
