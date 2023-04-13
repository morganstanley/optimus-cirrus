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
package com.ms.silverking.cloud.dht.daemon.storage.convergence.management;

import com.ms.silverking.cloud.ring.RingRegion;
import com.ms.silverking.id.UUIDBase;
import com.ms.silverking.net.IPAndPort;

class ReplicaSyncRequest extends Action {
  private final long uuidMSL;
  private final long uuidLSL;
  private final long ns;
  private final long rrStart;
  private final long rrEnd;
  private final long newOwner;
  private final long oldOwner;
  private long sendTimeMillis;

  private ReplicaSyncRequest(UUIDBase uuid, long ns, RingRegion region, IPAndPort newOwner, IPAndPort oldOwner,
      Action[] upstreamDependencies) {
    super(uuid.getMostSignificantBits(), uuid.getLeastSignificantBits(), upstreamDependencies);
    this.uuidMSL = uuid.getMostSignificantBits();
    this.uuidLSL = uuid.getLeastSignificantBits();
    this.ns = ns;
    this.rrStart = region.getStart();
    this.rrEnd = region.getEnd();
    this.newOwner = newOwner.toLong();
    this.oldOwner = oldOwner.toLong();
  }

  public static ReplicaSyncRequest of(long ns, RingRegion region, IPAndPort newOwner, IPAndPort oldOwner,
      Action upstreamDependency) {
    Action[] upstreamDependencies;
    UUIDBase uuid;

    if (upstreamDependency != null) {
      upstreamDependencies = new Action[1];
      upstreamDependencies[0] = upstreamDependency;
    } else {
      upstreamDependencies = new Action[0];
    }
    uuid = UUIDBase.random(); // random to allow for easy use in hashCode()
    return new ReplicaSyncRequest(uuid, ns, region, newOwner, oldOwner, upstreamDependencies);
  }

  public static ReplicaSyncRequest of(long ns, RingRegion region, IPAndPort newOwner, IPAndPort oldOwner) {
    return of(ns, region, newOwner, oldOwner, null);
  }

  public UUIDBase getUUID() {
    return new UUIDBase(uuidMSL, uuidLSL);
  }

  long getNS() {
    return ns;
  }

  RingRegion getRegion() {
    return new RingRegion(rrStart, rrEnd);
  }

  IPAndPort getNewOwner() {
    return IPAndPort.fromLong(newOwner);
  }

  IPAndPort getOldOwner() {
    return IPAndPort.fromLong(oldOwner);
  }

  void setSendTime(long sendTimeMillis) {
    this.sendTimeMillis = sendTimeMillis;
  }

  long getSendTime() {
    return sendTimeMillis;
  }

  boolean containsOwner(IPAndPort owner) {
    return owner.equals(newOwner) || owner.equals(oldOwner);
  }

  // All current use of this class uses reference equality for performance

  @Override
  public int hashCode() {
    return (int) uuidLSL;
    //return super.hashCode();
  }

  @Override
  public boolean equals(Object obj) {
    return this == obj;
    //return super.equals(obj);
        /*
        ReplicaSyncRequest    o;
        
        o = (ReplicaSyncRequest)obj;
        return this.ns == o.ns && this.region.equals(o.region) && this.newOwner.equals(o.newOwner) && this.oldOwner
        .equals(o.oldOwner);
        */
  }

  @Override
  public String toString() {
    return String.format("%s:%d:%s:%s<=%s:%d", getUUID(), ns, getRegion(), IPAndPort.fromLong(newOwner),
        IPAndPort.fromLong(oldOwner), sendTimeMillis);
  }
}