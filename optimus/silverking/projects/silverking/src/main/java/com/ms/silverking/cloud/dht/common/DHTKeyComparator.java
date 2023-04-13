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
package com.ms.silverking.cloud.dht.common;

import java.util.Comparator;

public class DHTKeyComparator implements Comparator<DHTKey> {
  public static DHTKeyComparator dhtKeyComparator = new DHTKeyComparator();

  public DHTKeyComparator() {
  }

  @Override
  public int compare(DHTKey k1, DHTKey k2) {
    if (k1.getMSL() < k2.getMSL()) {
      return -1;
    } else if (k1.getMSL() > k2.getMSL()) {
      return 1;
    } else {
      if (k1.getLSL() < k2.getLSL()) {
        return -1;
      } else if (k1.getLSL() > k2.getLSL()) {
        return 1;
      } else {
        return 0;
      }
    }
  }
}
