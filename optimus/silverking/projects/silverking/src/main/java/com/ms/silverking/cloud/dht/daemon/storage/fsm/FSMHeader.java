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
package com.ms.silverking.cloud.dht.daemon.storage.fsm;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.google.common.collect.ImmutableList;
import com.ms.silverking.collection.IntegerComparator;
import com.ms.silverking.collection.Pair;
import com.ms.silverking.collection.TupleUtil;

/**
 * Instantiated representation of FSMHeader used to access the header.
 * <p>
 * A header contains entries that describe the type and location of
 * LTVElements in the meta data segment.
 * <p>
 * The header does not contain the LTVElements themselves.
 */
public class FSMHeader {
  /**
   * A header consists of several entries. Each
   * entry maps an FSMElementType to the offset of the
   * LTVElement in the meta data segment.
   */
  private final Map<FSMElementType, Integer> entries;

  public FSMHeader(Map<FSMElementType, Integer> entries) {
    this.entries = entries;
  }

  public static FSMHeader create(List<LTVElement> elements) {
    Map<FSMElementType, Integer> entries;
    int offset;

    offset = 0;
    entries = new HashMap<>();
    for (LTVElement element : elements) {
      FSMElementType type;

      type = FSMElementType.typeForOrdinal(element.getType());
      entries.put(type, offset);
      offset += element.getLength();
    }
    return new FSMHeader(entries);
  }

  public static FSMHeader create(LTVElement... elements) {
    return create(ImmutableList.copyOf(elements));
  }

  public List<Pair<FSMElementType, Integer>> getEntriesByAscendingOffset() {
    List<Pair<FSMElementType, Integer>> l;

    l = new ArrayList<>();
    for (Map.Entry<FSMElementType, Integer> e : entries.entrySet()) {
      l.add(new Pair<>(e.getKey(), e.getValue()));
    }
    return (List<Pair<FSMElementType, Integer>>) TupleUtil.copyAndSort(l, FSMHeaderElement.offsetFieldIndex,
        IntegerComparator.instance);
  }

  public int getElementOffset(FSMElementType type) {
    Integer offset;

    offset = entries.get(type);
    if (offset == null) {
      return -1;
    } else {
      return offset;
    }
  }

  public int getNumEntries() {
    return entries.size();
  }

  @Override
  public String toString() {
    StringBuffer sb;

    sb = new StringBuffer();
    for (Map.Entry<FSMElementType, Integer> e : entries.entrySet()) {
      sb.append(String.format("%s\t%s\n", e.getKey(), e.getValue()));
    }
    return sb.toString();
  }
}
