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

import com.ms.silverking.cloud.dht.common.SystemTimeUtil;
import com.ms.silverking.cloud.ring.RingRegion;
import com.ms.silverking.net.IPAndPort;

public class ChecksumTreeRequest {
  private final ConvergencePoint targetCP;
  private final ConvergencePoint curCP;
  private final RingRegion region;
  private final IPAndPort replica;
  private long sendTime;

  private static final long checksumTreeRequestTimeout = 1 * 60 * 1000;

  public ChecksumTreeRequest(ConvergencePoint targetCP, ConvergencePoint curCP, RingRegion region, IPAndPort replica) {
    this.targetCP = targetCP;
    this.curCP = curCP;
    this.region = region;
    this.replica = replica;
  }

  public ConvergencePoint getTargetCP() {
    return targetCP;
  }

  public ConvergencePoint getCurCP() {
    return curCP;
  }

  public RingRegion getRegion() {
    return region;
  }

  public IPAndPort getReplica() {
    return replica;
  }

  public void setSent() {
    sendTime = SystemTimeUtil.skSystemTimeSource.absTimeMillis();
  }

  public boolean hasTimedOut() {
    return SystemTimeUtil.skSystemTimeSource.absTimeMillis() > sendTime + checksumTreeRequestTimeout;
  }

  @Override
  public String toString() {
    return replica + " " + region;
  }
}