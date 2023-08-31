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
package com.ms.silverking.cloud.ring;

import java.util.Comparator;

import com.ms.silverking.cloud.dht.common.DHTKeyComparator;
import com.ms.silverking.cloud.dht.common.KeyUtil;
import com.ms.silverking.cloud.dht.daemon.storage.KeyAndVersionChecksum;

/** Orders KeyAndVersionChecksums according to their coordinates with a given region. */
public class KeyAndVersionChecksumCoordinateComparator
    implements Comparator<KeyAndVersionChecksum> {
  private final Comparator<Long> positionComparator;

  public KeyAndVersionChecksumCoordinateComparator(RingRegion region) {
    positionComparator = region.positionComparator();
  }

  @Override
  public int compare(KeyAndVersionChecksum kvc0, KeyAndVersionChecksum kvc1) {
    int result;

    result =
        positionComparator.compare(
            KeyUtil.keyToCoordinate(kvc0.getKey()), KeyUtil.keyToCoordinate(kvc1.getKey()));
    if (result == 0) {
      result = DHTKeyComparator.dhtKeyComparator.compare(kvc0.getKey(), kvc1.getKey());
      if (result == 0) {
        result = Long.compare(kvc0.getVersionChecksum(), kvc1.getVersionChecksum());
        return result;
      } else {
        return result;
      }
    } else {
      return result;
    }
  }
}
