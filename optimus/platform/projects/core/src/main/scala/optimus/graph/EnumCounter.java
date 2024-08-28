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
package optimus.graph;

import java.util.EnumMap;
import java.util.HashMap;
import java.util.Map;

public class EnumCounter<K extends Enum<K>> {
  private final EnumMap<K, long[]> counters;
  private K[] keys;
  private final Class<K> clz;

  private EnumCounter(EnumCounter<K> em, boolean copyCounts) {
    clz = em.clz;
    counters = new EnumMap<K, long[]>(clz);
    keys = em.keys;
    if (copyCounts)
      for (var e : em.counters.entrySet()) counters.put(e.getKey(), new long[] {e.getValue()[0]});
    else for (var k : keys) counters.put(k, new long[] {0});
  }

  public EnumCounter(Class<K> c) {
    clz = c;
    try {
      var keys = c.getEnumConstants();
      counters = new EnumMap<K, long[]>(c);
      for (var k : keys) counters.put(k, new long[] {0L});
    } catch (Exception x) {
      throw new RuntimeException("Catastrophic error creating EnumCounter", x);
    }
  }

  public void add(K k, long n) {
    counters.get(k)[0] += n;
  }

  public EnumCounter<K> subtract(EnumCounter<K> ec) {
    for (var e : ec.counters.entrySet()) counters.get(e.getKey())[0] -= e.getValue()[0];
    return this;
  }

  public void add(EnumCounter<K> ec) {
    for (var e : ec.counters.entrySet()) counters.get(e.getKey())[0] += e.getValue()[0];
  }

  public EnumCounter<K> snap() {
    return new EnumCounter<>(this, true);
  }

  public Map<K, Long> toMap() {
    var m = new HashMap<K, Long>();
    for (var e : counters.entrySet()) m.put(e.getKey(), e.getValue()[0]);
    return m;
  }

  public long total() {
    long total = 0;
    for (var v : counters.values()) total += v[0];
    return total;
  }

  public void clear() {
    for (var v : counters.values()) v[0] = 0;
  }
}
