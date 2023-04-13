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

import java.util.HashSet;
import java.util.Set;

import com.google.common.collect.ImmutableSet;
import com.ms.silverking.cloud.dht.client.impl.NamespaceCreator;
import com.ms.silverking.cloud.dht.client.impl.SimpleNamespaceCreator;
import com.ms.silverking.collection.CollectionUtil;
import org.kohsuke.args4j.Option;

public class DHTRingManagerOptions {
  @Option(name = "-g", usage = "GridConfig", required = true)
  public String gridConfig;
  @Option(name = "-i", usage = "intervalSeconds", required = false)
  public int watchIntervalSeconds = 10;
  @Option(name = "-m", usage = "Mode", required = false)
  public Mode mode = Mode.Manual;
  @Option(name = "-pcm", usage = "PassiveConvergenceMode", required = false)
  public PassiveConvergenceMode passiveConvergenceMode = PassiveConvergenceMode.FullSync_FailOnFailure;
  @Option(name = "-ignoreNamespacesHex", usage = "ignoreNamespacesHex", required = false)
  public String ignoreNamespacesHex;

  public Set<Long> getIgnoredNamespaces() {
    if (ignoreNamespacesHex == null) {
      return ImmutableSet.of();
    } else {
      NamespaceCreator nsc;
      HashSet<Long> ignoredNamespaces;

      nsc = new SimpleNamespaceCreator();
      ignoredNamespaces = new HashSet<>();
      for (String ns : CollectionUtil.parseSet(ignoreNamespacesHex, ",")) {
        ignoredNamespaces.add(Long.decode(ns));
      }
      return ignoredNamespaces;
    }
  }
}
