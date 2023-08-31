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
package com.ms.silverking.net.analysis.iperf;

import java.util.Comparator;

public class BPSComparator implements Comparator<Measurement> {
  public BPSComparator() {}

  @Override
  public int compare(Measurement o1, Measurement o2) {
    long bps1;
    long bps2;

    bps1 = o1.getBitsPerSecond();
    bps2 = o2.getBitsPerSecond();
    if (bps1 < bps2) {
      return -1;
    } else if (bps1 > bps2) {
      return 1;
    } else {
      return 0;
    }
  }
}
