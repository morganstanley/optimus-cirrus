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
package com.ms.silverking.cloud.dht.daemon;

import java.util.Arrays;

import com.google.common.collect.ImmutableList;
import com.ms.silverking.net.IPAndPort;
import com.ms.silverking.text.StringUtil;

public class ReplicaHealthPrioritizer implements ReplicaPrioritizer {
  private final PeerHealthMonitor peerHealthMonitor;

  public ReplicaHealthPrioritizer(PeerHealthMonitor peerHealthMonitor) {
    this.peerHealthMonitor = peerHealthMonitor;
  }

  @Override
  public int compare(IPAndPort r1, IPAndPort r2) {
    if (peerHealthMonitor.isStrongSuspect(r1)) {
      if (peerHealthMonitor.isStrongSuspect(r2)) {
        return compareLastErrorTimes(r1, r2);
      } else {
        return 1;
      }
    } else {
      if (peerHealthMonitor.isStrongSuspect(r2)) {
        return -1;
      } else {
        return compareLastErrorTimes(r1, r2);
      }
    }
  }

  private int compareLastErrorTimes(IPAndPort r1, IPAndPort r2) {
    return 0; // use of weak errors deprecated for now
  /*
    long t1;
    long t2;

    // Note that peer health monitor will timeout errors so that nodes
    // without recent errors come back as Long.MAX_VALUE here
    t1 = peerHealthMonitor.getLastWeakErrorTime(r1);
    t2 = peerHealthMonitor.getLastWeakErrorTime(r2);
    if (t1 < t2) {
      return -1;
    } else {
      if (t1 > t2) {
        return 1;
      } else {
        return 0;
      }
    }
   */
  }

  public static void main(String[] args) {
    try {
      PeerHealthMonitor p;
      IPAndPort[] suspects;
      IPAndPort[] replicas;

      replicas = IPAndPort.list(ImmutableList.copyOf(StringUtil.splitAndTrim(args[0], ","))).toArray(new IPAndPort[0]);
      suspects = IPAndPort.list(ImmutableList.copyOf(StringUtil.splitAndTrim(args[1], ","))).toArray(new IPAndPort[0]);
      p = new PeerHealthMonitor(null, null, null);
      for (IPAndPort suspect : suspects) {
        p.addSuspect(suspect, PeerHealthIssue.OpTimeout);
      }
      Arrays.sort(replicas, new ReplicaHealthPrioritizer(p));
      System.out.println(IPAndPort.arrayToString(replicas));
    } catch (Exception e) {
      e.printStackTrace();
    }
  }
}