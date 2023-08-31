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
package com.ms.silverking.cloud.dht.daemon.storage;

import java.util.Comparator;

public class KeyAndVersionChecksumSegmentNumberComparator
    implements Comparator<KeyAndVersionChecksum> {
  private final int order;

  public static final KeyAndVersionChecksumSegmentNumberComparator ascendingSort =
      new KeyAndVersionChecksumSegmentNumberComparator(1);
  public static final KeyAndVersionChecksumSegmentNumberComparator descendingSort =
      new KeyAndVersionChecksumSegmentNumberComparator(-1);

  private KeyAndVersionChecksumSegmentNumberComparator(int order) {
    this.order = order;
  }

  @Override
  public int compare(KeyAndVersionChecksum k1, KeyAndVersionChecksum k2) {
    return order * Long.compare(k1.getSegmentNumber(), k2.getSegmentNumber());
  }
}
