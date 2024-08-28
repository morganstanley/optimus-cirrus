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
package optimus.graph.diagnostics;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

import optimus.core.ArrayListEx;
import optimus.graph.NodeTaskInfo;

public final class PNodeTaskInfoUtils {
  /** Merges 2 PNodeTaskInfoLight arrays, referring at time to the underlying values */
  public static PNodeTaskInfoLight[] merge(PNodeTaskInfoLight[] a, PNodeTaskInfoLight[] b) {
    if (a == null || a.length == 0) return b;
    if (b == null || b.length == 0) return a;
    PNodeTaskInfoLight[] r = new PNodeTaskInfoLight[Math.max(a.length, b.length)];
    for (int i = 0; i < r.length; i++) {
      PNodeTaskInfoLight av = i < a.length ? a[i] : null;
      PNodeTaskInfoLight bv = i < b.length ? b[i] : null;
      if (av == null) r[i] = bv;
      else if (bv == null) r[i] = av;
      else {
        r[i] = new PNodeTaskInfoLight(av.id);
        r[i].merge(av);
        r[i].merge(bv);
      }
    }
    return r;
  }

  /**
   * For now we don't check the modifier string itself because we don't have cases where the same
   * underlying node is wrapped by two different modifier nodes - if we ever do, we'll have to
   * change this algorithm
   */
  private static boolean isModifiedNodeOf(PNodeTaskInfo a, PNodeTaskInfo b) {
    return !a.modifier.isEmpty()
        && Objects.equals(a.pkgName, b.pkgName)
        && Objects.equals(a.name, b.name);
  }

  public static boolean isProxyOf(PNodeTaskInfo a, PNodeTaskInfo b) {
    if (!a.isProfilerProxy()) return false;
    return isModifiedNodeOf(a, b) && a.modifier.equals(NodeTaskInfo.NAME_MODIFIER_PROXY);
  }

  /** Ordering: modified always after original */
  public static int compare(PNodeTaskInfo a, PNodeTaskInfo b) {
    int pkgCmp = compare(a.pkgName, b.pkgName);
    if (pkgCmp != 0) return pkgCmp;
    int nameCmp = compare(a.name, b.name);
    if (nameCmp != 0) return nameCmp;
    return compare(a.modifier, b.modifier);
  }

  private static int compare(String a, String b) {
    return a == null ? (b == null ? 0 : -1) : (b == null ? 1 : a.compareTo(b));
  }

  private interface Aggregator {
    void aggregate(PNodeTaskInfo a, PNodeTaskInfo b);

    static Aggregator of(String modifier) {
      if (modifier == null) return null;
      return switch (modifier) {
        case NodeTaskInfo.NAME_MODIFIER_PROXY -> cacheProxy;
        case NodeTaskInfo.NAME_MODIFIER_BOOTSTRAP -> bootstrap;
        default -> null;
      };
    }

    Aggregator cacheProxy = PNodeTaskInfo::mergeWithProxy;
    Aggregator bootstrap = PNodeTaskInfo::mergeWithBootstrap;
  }

  public static ArrayList<PNodeTaskInfo> aggregate(List<PNodeTaskInfo> pntis) {
    ArrayListEx<PNodeTaskInfo> r = new ArrayListEx<>(pntis);
    if (r.size() == 1) return r;

    int copyTo = 0;
    r.sort(PNodeTaskInfoUtils::compare);
    for (int i = 1; i < r.size(); i++) {
      PNodeTaskInfo owner = r.get(i - 1); // prev (corresponding to the owner pnti)
      PNodeTaskInfo maybeProxy = r.get(i); // curr (corresponding to owner pnti proxy)

      var aggregator = Aggregator.of(maybeProxy.modifier);
      PNodeTaskInfo combined = owner;
      if (aggregator != null && isModifiedNodeOf(maybeProxy, owner)) {
        combined = owner.dup();
        aggregator.aggregate(combined, maybeProxy);
        i++; // consumes 2 items (owner and proxy pairs)
      }
      r.set(copyTo++, combined);
      if (i == r.size() - 1) r.set(copyTo++, r.get(i));
    }
    r.shrinkTo(copyTo);
    return r;
  }
}
