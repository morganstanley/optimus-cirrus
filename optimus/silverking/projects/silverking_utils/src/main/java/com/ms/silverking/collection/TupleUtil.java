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
package com.ms.silverking.collection;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;

import com.google.common.collect.ImmutableList;

public class TupleUtil {
  static List<Object> parse(String def, String pattern, int expectedLength, String... typeNames) {
    List<String> rawList;
    List<Object> cookedList;

    def = def.trim();
    rawList = ImmutableList.copyOf(def.split(pattern));
    if (typeNames.length != expectedLength) {
      throw new RuntimeException("Expected typesNames.length, expected.Length");
    }
    if (rawList.size() != expectedLength) {
      throw new RuntimeException(
          "Incorrect values from rawList.size(),expectedLength, def. Expected value is %d != %d from %s"
              + rawList.size()
              + expectedLength
              + def);
    }
    cookedList = new ArrayList<>(rawList.size());
    for (int i = 0; i < typeNames.length; i++) {
      String typeName;

      typeName = typeNames[i];
      if (typeName.equals(java.lang.String.class.getName())) {
        cookedList.add(rawList.get(i));
      } else if (typeName.equals(java.lang.Long.class.getName())) {
        cookedList.add(Long.parseLong(rawList.get(i)));
      } else if (typeName.equals(java.lang.Integer.class.getName())) {
        cookedList.add(Integer.parseInt(rawList.get(i)));
      } else if (typeName.equals(java.lang.Double.class.getName())) {
        cookedList.add(Double.parseDouble(rawList.get(i)));
      } else {
        throw new RuntimeException("Unsupported type: " + typeName);
      }
    }
    return cookedList;
  }

  public static List<? extends TupleBase> copyAndSort(
      List<? extends TupleBase> unsortedList, int fieldIndex, Comparator elementComparator) {
    List<TupleBase> sortedList;

    sortedList = new ArrayList<>();
    sortedList.addAll(unsortedList);
    sort(sortedList, fieldIndex, elementComparator);
    return sortedList;
  }

  public static void sort(
      List<? extends TupleBase> list, int elementIndex, Comparator elementComparator) {
    Collections.sort(list, new TupleElementComparator(elementIndex, elementComparator));
  }
}
