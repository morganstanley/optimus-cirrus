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
package com.ms.silverking.collection;

import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.ConcurrentMap;

import org.slf4j.LoggerFactory;
import org.slf4j.Logger;

/** Iterates through all entries of a nested concurrent map and removes entries that are empty. */
public class NestedConcurrentMapCleaner<K, L, V> extends TimerTask {
  private final ConcurrentMap<K, ConcurrentMap<L, V>> map;
  private final boolean verbose;
  private final Set<K> candidates;

  private static Logger log = LoggerFactory.getLogger(NestedConcurrentMapCleaner.class);

  public NestedConcurrentMapCleaner(
      ConcurrentMap<K, ConcurrentMap<L, V>> map, Timer timer, long periodMillis, boolean verbose) {
    this.map = map;
    timer.scheduleAtFixedRate(this, periodMillis, periodMillis);
    this.verbose = verbose;
    candidates = new HashSet<>();
  }

  public NestedConcurrentMapCleaner(
      ConcurrentMap<K, ConcurrentMap<L, V>> map, Timer timer, long periodMillis) {
    this(map, timer, periodMillis, false);
  }

  public void run() {
    int size;

    if (verbose) {
      // System.gc();
      log.info("Cleaning: {}", map);
    }
    size = 0;
    for (Map.Entry<K, ConcurrentMap<L, V>> entry : map.entrySet()) {
      ConcurrentMap<L, V> nestedMap;

      size++;
      nestedMap = entry.getValue();
      if (nestedMap.size() == 0) {
        if (verbose) {
          log.info("Found empty nested map. Removing key: {}", entry.getKey());
        }
        // "Candidate" code is used to work around race condition between
        // map creation and map population. This only works for maps which are quickly populated
        if (candidates.contains(entry.getKey())) {
          candidates.remove(entry.getKey());
          map.remove(entry.getKey());
        } else {
          candidates.add(entry.getKey());
        }
      }
    }
    if (verbose) {
      log.info("size: {}", size);
      // System.gc();
      // Log.info("MB Free: "+ (Runtime.getRuntime().freeMemory() / (1024 * 1024)));
    }
  }
}
