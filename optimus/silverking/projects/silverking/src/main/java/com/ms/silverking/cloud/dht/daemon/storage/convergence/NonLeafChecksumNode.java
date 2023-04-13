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
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;

import com.ms.silverking.cloud.dht.daemon.storage.KeyAndVersionChecksum;
import com.ms.silverking.cloud.ring.RingRegion;
import com.ms.silverking.util.Mutability;

public class NonLeafChecksumNode extends AbstractChecksumNode {
  private final List<? extends ChecksumNode> children;

  public NonLeafChecksumNode(RingRegion ringRegion, List<? extends ChecksumNode> children) {
    super(ringRegion, Mutability.Immutable);
    verifyListOrder(children);
    this.children = children;
  }

  @Override
  public ConvergenceChecksum getChecksum() {
    ConvergenceChecksum checksum;

    checksum = children.get(0).getChecksum();
    for (int i = 1; i < children.size(); i++) {
      checksum = checksum.xor(children.get(i).getChecksum());
    }
    return checksum;
  }

  @Override
  public List<? extends ChecksumNode> getChildren() {
    return children;
  }

  @Override
  public ChecksumNode duplicate() {
    List<ChecksumNode> dupChildren;

    dupChildren = new ArrayList<>(children.size());
    for (int i = 0; i < children.size(); i++) {
      dupChildren.add(children.get(i).duplicate());
    }
    return new NonLeafChecksumNode(ringRegion, dupChildren);
  }

  @Override
  public Iterator<KeyAndVersionChecksum> iterator() {
    return new KVCIterator();
  }

  private class KVCIterator implements Iterator<KeyAndVersionChecksum> {
    private int curChild;
    private Iterator<KeyAndVersionChecksum> curChildIterator;

    KVCIterator() {
      mutability.ensureImmutable();
      if (children.size() > 0) {
        curChild = 0;
        curChildIterator = children.get(0).iterator();
      } else {
        curChild = -1;
      }
    }

    private void findNonEmptyChild() {
      while (curChild >= 0 && !curChildIterator.hasNext()) {
        ++curChild;
        if (curChild >= children.size()) {
          curChild = -1;
        } else {
          curChildIterator = children.get(curChild).iterator();
        }
      }
    }

    @Override
    public boolean hasNext() {
      findNonEmptyChild();
      return curChild >= 0;
    }

    @Override
    public KeyAndVersionChecksum next() {
      findNonEmptyChild();
      if (curChild >= 0) {
        return curChildIterator.next();
      } else {
        throw new NoSuchElementException();
      }
    }

    @Override
    public void remove() {
      throw new UnsupportedOperationException();
    }

  }

  @Override
  public int estimatedKeys() {
    int sum;

    sum = 0;
    for (ChecksumNode child : children) {
      sum += child.estimatedKeys();
    }
    return sum;
  }

  private int binarySearchForRegionIndex(RingRegion region) {
    int low = 0;
    int high = children.size() - 1;

    while (low <= high) {
      int mid = (low + high) >>> 1;
      ChecksumNode midVal = children.get(mid);
      int cmp = RingRegion.positionComparator.compare(midVal.getRegion(), region);

      if (cmp < 0) {
        low = mid + 1;
      } else if (cmp > 0) {
        high = mid - 1;
      } else {
        return mid; // key found
      }
    }
    return -(low + 1);  // key not found
  }

  @Override
  public ChecksumNode getNodeForRegion(RingRegion region) {
    int index;

    index = binarySearchForRegionIndex(region);
    if (index < 0) {
      int insertionPoint;

      insertionPoint = -(index + 1);
      if (insertionPoint == 0) {
        return null;
      } else {
        return children.get(insertionPoint - 1).getNodeForRegion(region);
      }
    } else {
      return children.get(index);
    }
  }
}
