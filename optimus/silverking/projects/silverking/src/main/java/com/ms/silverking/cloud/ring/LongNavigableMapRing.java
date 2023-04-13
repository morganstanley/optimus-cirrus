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

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.SortedMap;
import java.util.TreeMap;

import com.ms.silverking.util.Mutability;

/**
 * Basic navigable ring. Mutable during creation; immutable afterwards.
 */
public class LongNavigableMapRing<T> implements Ring<Long, T> {
  private final NavigableMap<Long, T> ringMap;
  private Mutability mutability;

  public LongNavigableMapRing() {
    ringMap = new TreeMap<Long, T>();
    mutability = Mutability.Mutable;
  }

  public void freeze() {
    if (mutability == Mutability.Mutable) {
      mutability = Mutability.Immutable;
    } else {
      throw new RuntimeException("freeze() called multiple times");
    }
  }

  public void ensureMutable() {
    mutability.ensureMutable();
  }

  public void ensureImmutable() {
    mutability.ensureImmutable();
  }

  @Override
  public void put(Long key, T member) {
    T oldMember;

    ensureMutable();
    oldMember = ringMap.put(key, member);
    if (oldMember != null) {
      throw new RuntimeException("Attempted member mutation");
    }
  }

  private Map.Entry<Long, T> getEntry(Long key) {
    Map.Entry<Long, T> entry;

    entry = ringMap.ceilingEntry(key);
    if (entry == null) {
      entry = ringMap.firstEntry();
    }
    return entry;
  }

  @Override
  public List<T> getOwners(Long minKey, Long maxKey) {
    List<T> owners;
    SortedMap<Long, T> intervalMap;
    Map.Entry<Long, T> entry;

    owners = new ArrayList<>();
    intervalMap = ringMap.subMap(minKey, true, maxKey, true);
    for (T owner : intervalMap.values()) {
      owners.add(owner);
    }
    // Above might miss the last entry, handle below...
    entry = ringMap.ceilingEntry(maxKey);
    if (entry == null) {
      entry = ringMap.firstEntry();
    }
    if (!owners.contains(entry.getValue())) {
      owners.add(entry.getValue());
    }
    return owners;
  }

  @Override
  public T getOwner(Long key) {
    Map.Entry<Long, T> entry;

    entry = getEntry(key);
    return entry.getValue();
  }

  @Override
  public T removeOwner(Long key) {
    Map.Entry<Long, T> entry;

    ensureMutable();
    entry = ringMap.ceilingEntry(key);
    if (entry == null) {
      entry = ringMap.firstEntry();
    }
    ringMap.remove(entry.getKey());
    return entry.getValue();
  }
    
    /*
    public enum Mode {SUBSEQUENT, ROTATE};
    @Override
    public List<T> get(Long key, int numMembers) {
        Map.Entry<Long, T> entry;
        List<T>    members;
        
        if (numMembers < 1) {
            throw new RuntimeException("numMembers < 1");
        }
        members = new ArrayList<T>(numMembers);
        entry = getEntry(key);
        members.add(entry.getValue());
        switch (mode) {
        case SUBSEQUENT: addSubsequent(members, entry, numMembers - 1); break;
        case ROTATE: addRotated(members, entry, numMembers - 1); break;
        default: throw new RuntimeException("panic");
        }
        return members;
    }
    
    private void addSubsequent(List<T> members, Map.Entry<Long,T> entry, int numSubsequent) {
        for (int i = 0; i < numSubsequent; i++) {
            entry = ringMap.higherEntry(entry.getKey());
            if (entry == null) {
                entry = ringMap.firstEntry();
            }
            members.add(entry.getValue());
        }
    }
    
    private void addRotated(List<T> members, Map.Entry<Long,T> entry, int numSubsequent) {
        for (int i = 0; i < numSubsequent; i++) {
            Long    rotated;
            
            rotated = Long.rotateRight(entry.getKey(), 1);
            entry = getEntry(rotated);
            members.add(entry.getValue());
        }
    }
    */

  @Override
  public Collection<T> getMembers() {
    return ringMap.values();
  }

  @Override
  public int numMembers() {
    ensureImmutable();
    return ringMap.size();
  }

  @Override
  public String toString() {
    StringBuilder sb;

    sb = new StringBuilder();
    for (Map.Entry<Long, T> entry : ringMap.entrySet()) {
      sb.append(entry.getValue());
      sb.append('\n');
    }
    return sb.toString();
  }
}
