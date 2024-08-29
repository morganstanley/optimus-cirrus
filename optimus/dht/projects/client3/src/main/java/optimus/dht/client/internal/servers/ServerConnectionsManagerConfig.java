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
package optimus.dht.client.internal.servers;

import java.time.Duration;

public class ServerConnectionsManagerConfig {

  private final int slotsPerServer;
  private final Duration orphanedNodeTimeout;
  private final Duration initialRetryInterval;
  private final Duration maxRetryInterval;
  private final float retryIntervalExpRatio;
  private final boolean eagerDnsResolution;

  public ServerConnectionsManagerConfig(
      int slotsPerServer,
      Duration orphanedNodeTimeout,
      Duration initialRetryInterval,
      Duration maxRetryInterval,
      float retryIntervalExpRatio,
      boolean eagerDnsResolution) {
    this.slotsPerServer = slotsPerServer;
    this.orphanedNodeTimeout = orphanedNodeTimeout;
    this.initialRetryInterval = initialRetryInterval;
    this.maxRetryInterval = maxRetryInterval;
    this.retryIntervalExpRatio = retryIntervalExpRatio;
    this.eagerDnsResolution = eagerDnsResolution;
  }

  public int slotsPerServer() {
    return slotsPerServer;
  }

  public Duration getOrphanedNodeTimeout() {
    return orphanedNodeTimeout;
  }

  public Duration initialRetryInterval() {
    return initialRetryInterval;
  }

  public Duration maxRetryInterval() {
    return maxRetryInterval;
  }

  public float retryIntervalExpRatio() {
    return retryIntervalExpRatio;
  }

  public boolean eagerDnsResolution() {
    return eagerDnsResolution;
  }
}
