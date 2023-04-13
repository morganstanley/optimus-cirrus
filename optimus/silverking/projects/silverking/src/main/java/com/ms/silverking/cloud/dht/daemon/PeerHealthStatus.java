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

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import com.google.common.collect.ImmutableSet;
import com.ms.silverking.cloud.dht.common.SystemTimeUtil;
import com.ms.silverking.collection.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Tracks the health of a peer.
 */
public class PeerHealthStatus {
  private final Map<PeerHealthIssue, Long> healthReports;
  private long healthyTimeMillis;

  /*
  As we will have an instance of this class for every replica, we make some effort to keep memory
  consumption low. We use the object monitor in place of a lock to avoid creating a new object and to avoid
  the overhead of concurrent structures.
  */

  private static final long weakErrorTimeoutMillis = 5 * 60 * 1000;

  private static Logger log = LoggerFactory.getLogger(PeerHealthStatus.class);

  public PeerHealthStatus() {
    healthReports = new HashMap<>(PeerHealthIssue.values().length);
  }

  public void addIssue(PeerHealthIssue issue, long timeMillis) {
    synchronized (this) {
      healthReports.put(issue, timeMillis);
    }
  }

  public void setHealthy(long timeMillis) {
    synchronized (this) {
      this.healthyTimeMillis = timeMillis;
    }
  }

  public boolean isStrongSuspect() {
    return getCurrentIssues(SystemTimeUtil.timerDrivenTimeSource.absTimeMillis()).getV1().size() > 0;
  }

  public Pair<Set<PeerHealthIssue>, Set<PeerHealthIssue>> getCurrentIssues(long curTimeMillis) {
    synchronized (this) {
      return getIssues(curTimeMillis, healthyTimeMillis + 1);
    }
  }

  /**
   * Return issues that have occurred >= sinceTimeMillis. curTimeMillis is used to compute
   * weak error timeouts
   *
   * @param curTimeMillis
   * @param sinceTimeMillis
   * @return
   */
  private Pair<Set<PeerHealthIssue>, Set<PeerHealthIssue>> getIssues(long curTimeMillis, long sinceTimeMillis) {
    Set<PeerHealthIssue> strongIssues;
    Set<PeerHealthIssue> weakIssues;

    strongIssues = new HashSet<>();
    weakIssues = new HashSet<>();
    for (Map.Entry<PeerHealthIssue, Long> report : healthReports.entrySet()) {
      if (report.getValue() >= sinceTimeMillis) {
        if (report.getKey().isStrongIssue()) {
          strongIssues.add(report.getKey());
        } else if (curTimeMillis - report.getValue() <= weakErrorTimeoutMillis) {
          weakIssues.add(report.getKey());
        }
      }
    }
    if (log.isDebugEnabled()) {
      log.warn("strongIssues: {}", strongIssues);
      log.warn("weakIssues: {}", weakIssues);
    }
    return Pair.of(ImmutableSet.copyOf(strongIssues), ImmutableSet.copyOf(weakIssues));
  }
}
