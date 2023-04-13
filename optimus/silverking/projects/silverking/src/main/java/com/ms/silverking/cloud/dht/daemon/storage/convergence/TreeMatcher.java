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

import java.util.Iterator;

import com.ms.silverking.cloud.dht.common.DHTKey;
import com.ms.silverking.cloud.dht.common.UnsignedKeyComparator;
import com.ms.silverking.cloud.dht.daemon.storage.KeyAndVersionChecksum;
import com.ms.silverking.cloud.ring.RingRegion;

public class TreeMatcher {
  public static MatchResult match(ChecksumNode n0, ChecksumNode n1) {
    MatchResult result;
    Iterator<KeyAndVersionChecksum> i0;
    Iterator<KeyAndVersionChecksum> i1;
    KeyAndVersionChecksum kvc0;
    KeyAndVersionChecksum kvc1;
    boolean advance0;
    boolean advance1;

    RingRegion.ensureIdentical(n0.getRegion(), n1.getRegion());
    result = new MatchResult();
    i0 = n0.iterator();
    i1 = n1.iterator();
    kvc0 = null;
    kvc1 = null;
    advance0 = true;
    advance1 = true;
    while (i0.hasNext() && i1.hasNext()) {
      if (advance0) {
        kvc0 = i0.next();
      }
      if (advance1) {
        kvc1 = i1.next();
      }
      if (kvc0.equals(kvc1)) {
        // match
        advance0 = true;
        advance1 = true;
      } else {
        DHTKey k0;
        DHTKey k1;
        int comparisonResult;

        k0 = kvc0.getKey();
        k1 = kvc1.getKey();
        comparisonResult = UnsignedKeyComparator.instance.compare(k0, k1);
        if (comparisonResult == 0) {
          // keys match, version checksums must not
          result.addChecksumMismatch(kvc0);
          advance0 = true;
          advance1 = true;
        } else {
          // keys don't match; somebody has an entry not present in the other
          if (comparisonResult < 0) {
            // local key not present in other
            result.addSourceNotInDest(kvc0);
            advance0 = true;
            advance1 = false;
          } else { // > 0
            // other key not present locally
            result.addDestNotInSource(kvc1);
            advance0 = false;
            advance1 = true;
          }
        }
      }
    }
    // One or both of the iterators are done.
    // Check for the case where one index is not at the end.
    if (i0.hasNext()) {
      while (i0.hasNext()) {
        result.addSourceNotInDest(i0.next());
      }
    } else {
      while (i1.hasNext()) {
        result.addDestNotInSource(i1.next());
      }
    }
    return result;
  }
}
