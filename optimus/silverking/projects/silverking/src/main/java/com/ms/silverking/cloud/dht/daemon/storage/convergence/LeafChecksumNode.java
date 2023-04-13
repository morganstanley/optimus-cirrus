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

import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;

import com.google.common.collect.ImmutableList;
import com.ms.silverking.cloud.dht.common.DHTKey;
import com.ms.silverking.cloud.dht.daemon.storage.KeyAndVersionChecksum;
import com.ms.silverking.cloud.ring.KeyAndVersionChecksumCoordinateComparator;
import com.ms.silverking.cloud.ring.RingRegion;
import com.ms.silverking.text.StringUtil;
import com.ms.silverking.util.Mutability;

public class LeafChecksumNode extends AbstractChecksumNode {
  private ConvergenceChecksum checksum;
  // List is used only during the mutable build phase. Once frozen, it is nulled out
  private List<KeyAndVersionChecksum> keyAndVersionChecksums;
  // The array is used to store the list of keys and checksums efficiently
  private long[] _keyAndVersionChecksums;

  private LeafChecksumNode(RingRegion ringRegion, List<KeyAndVersionChecksum> keyAndVersionChecksums,
      Mutability mutability) {
    super(ringRegion, mutability);
    this.keyAndVersionChecksums = keyAndVersionChecksums;
    if (mutability == Mutability.Immutable) {
      _freeze();
    }
  }

  public LeafChecksumNode(RingRegion ringRegion, List<KeyAndVersionChecksum> keyAndVersionChecksums) {
    super(ringRegion, Mutability.Immutable);
    this.keyAndVersionChecksums = keyAndVersionChecksums;
    _freeze();
  }

  public LeafChecksumNode(RingRegion ringRegion) {
    this(ringRegion, new ArrayList<KeyAndVersionChecksum>(), Mutability.Mutable);
  }

  public void freeze() {
    super.freeze();
    _freeze();
  }

  private void _freeze() {
    Collections.sort(keyAndVersionChecksums, new KeyAndVersionChecksumCoordinateComparator(ringRegion));
    this.checksum = computeChecksum(keyAndVersionChecksums);
    _keyAndVersionChecksums = KeyAndVersionChecksum.listToArray(keyAndVersionChecksums);
    keyAndVersionChecksums = null;
  }

  private static ConvergenceChecksum computeChecksum(List<KeyAndVersionChecksum> keyAndVersionChecksums) {
    ConvergenceChecksum checksum;

    //checksum = new byte[keyValueChecksums.get(0).getValueChecksum().length];
    checksum = null;
    for (KeyAndVersionChecksum keyAndVersionChecksum : keyAndVersionChecksums) {
      DHTKey key;

      key = keyAndVersionChecksum.getKey();
      if (checksum == null) {
        checksum = new ConvergenceChecksum(keyAndVersionChecksum);
      } else {
        checksum = checksum.xor(new ConvergenceChecksum(keyAndVersionChecksum));
      }
    }
    return checksum;
  }

  @Override
  public ConvergenceChecksum getChecksum() {
    mutability.ensureImmutable();
    return checksum;
  }

  @Override
  public List<? extends ChecksumNode> getChildren() {
    return ImmutableList.of();
  }

  @Override
  public ChecksumNode duplicate() {
    mutability.ensureImmutable();
    //return new LeafChecksumNode(ringRegion, new ArrayList<>(keyAndVersionChecksums));
    return new LeafChecksumNode(ringRegion, KeyAndVersionChecksum.arrayToList(_keyAndVersionChecksums));
  }

  public List<KeyAndVersionChecksum> getKeyAndVersionChecksums() {
    mutability.ensureImmutable();
    return KeyAndVersionChecksum.arrayToList(_keyAndVersionChecksums);
  }

  public void addChecksum(KeyAndVersionChecksum kvc) {
    mutability.ensureMutable();
    keyAndVersionChecksums.add(kvc);
  }

  @Override
  public int estimatedKeys() {
    if (mutability == Mutability.Mutable) {
      return keyAndVersionChecksums.size();
    } else {
      return KeyAndVersionChecksum.entriesInArray(_keyAndVersionChecksums);
    }
  }

  @Override
  public ChecksumNode getNodeForRegion(RingRegion region) {
    if (ringRegion.equals(region)) {
      return this;
    } else {
      return null;
    }
  }

  public long[] getKeyAndVersionChecksumsAsArray() {
    return _keyAndVersionChecksums;
  }

  @Override
  public void toString(StringBuilder sb, int depth) {
    super.toString(sb, depth);
    if (mutability == Mutability.Mutable) {
      for (KeyAndVersionChecksum keyAndVersionChecksum : keyAndVersionChecksums) {
        sb.append(String.format("%s[%s]\n", StringUtil.replicate('\t', depth + 1), keyAndVersionChecksum));
      }
    } else {
      Iterator<KeyAndVersionChecksum> iterator;

      iterator = iterator();
      while (iterator.hasNext()) {
        KeyAndVersionChecksum keyAndVersionChecksum;

        keyAndVersionChecksum = iterator.next();
        sb.append(String.format("%s[%s]\n", StringUtil.replicate('\t', depth + 1), keyAndVersionChecksum));
      }
    }
  }

  @Override
  public Iterator<KeyAndVersionChecksum> iterator() {
    mutability.ensureImmutable();
    //return keyAndVersionChecksums.iterator();
    return KeyAndVersionChecksum.getKVCArrayIterator(_keyAndVersionChecksums);
  }
}
