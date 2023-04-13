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
package com.ms.silverking.cloud.toporing;

import java.util.Comparator;

import com.ms.silverking.cloud.ring.RingRegion;

public class RingEntryPositionComparator implements Comparator<RingEntry> {
  public static final RingEntryPositionComparator instance = new RingEntryPositionComparator();

  public RingEntryPositionComparator() {
  }

  @Override
  public int compare(RingEntry o1, RingEntry o2) {
    return RingRegion.positionComparator.compare(o1.getRegion(), o2.getRegion());
  }
}
