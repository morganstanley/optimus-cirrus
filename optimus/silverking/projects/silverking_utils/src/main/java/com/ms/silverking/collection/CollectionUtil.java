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

import java.util.Collection;
import java.util.EnumSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;

import com.google.common.collect.HashMultimap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.SetMultimap;

public class CollectionUtil {
  public static final char defaultSeparator = ':';
  public static final String defaultStartBrace = "{";
  public static final String defaultEndBrace = "}";
  public static final String defaultEmptyDef = "<empty>";
  static final String defaultMapString = " -> ";
  static final String defaultMapEntrySeparator = "\n";

  public static <K> String toString(Collection<K> c) {
    return toString(c, defaultSeparator);
  }

  public static <K> String toString(Collection<K> c, char separator) {
    return toString(c, defaultStartBrace, defaultEndBrace, separator, defaultEmptyDef);
  }

  public static <K> String toString(
      Collection<K> c, String startBrace, String endBrace, char separator, String emptyDef) {
    if (c.isEmpty()) return emptyDef;

    StringBuilder sb = new StringBuilder();
    sb.append(startBrace);

    for (K member : c) {
      sb.append(member);
      sb.append(separator);
    }

    int separatorLength = 1;
    sb.deleteCharAt(sb.length() - separatorLength);
    sb.append(endBrace);

    return sb.toString();
  }

  public static <K> boolean containsNull(Collection<K> c) {
    for (K item : c) {
      if (item == null) {
        return true;
      }
    }
    return false;
  }

  public static <K, V> String mapToString(Map<K, V> map) {
    return mapToString(
        map,
        defaultStartBrace,
        defaultEndBrace,
        defaultMapString,
        defaultMapEntrySeparator,
        defaultEmptyDef);
  }

  public static <K, V> String mapToString(
      Map<K, V> map,
      String startBrace,
      String endBrace,
      String mapString,
      String separator,
      String emptyDef) {
    if (map.isEmpty()) return emptyDef;

    StringBuilder sb = new StringBuilder();
    sb.append(startBrace);

    for (Map.Entry<K, V> entry : map.entrySet()) {
      sb.append(entry.getKey() + mapString + entry.getValue());
      sb.append(separator);
    }

    sb.delete(sb.length() - separator.length(), sb.length());
    sb.append(endBrace);

    return sb.toString();
  }

  public static <K> Set<String> stringSet(Set<K> s) {
    ImmutableSet.Builder<String> ss;

    ss = ImmutableSet.builder();
    for (K k : s) {
      ss.add(k.toString());
    }
    return ss.build();
  }

  public static <K, V> SetMultimap<V, K> transposeSetMultimap(SetMultimap<K, V> m) {
    SetMultimap<V, K> tm;

    tm = HashMultimap.create();
    for (K k : m.keySet()) {
      for (V v : m.get(k)) {
        tm.put(v, k);
      }
    }
    return tm;
  }

  public static Set<String> parseSet(String def, String pattern) {
    if (def != null) {
      def = def.trim();
      if (def.startsWith(defaultStartBrace)) {
        def = def.substring(defaultStartBrace.length());
        if (def.endsWith(defaultEndBrace)) {
          def = def.substring(0, def.length() - defaultEndBrace.length());
        }
      }
      return ImmutableSet.copyOf(def.split(pattern));
    } else {
      return ImmutableSet.of();
    }
  }

  public static <K extends Enum<K>> EnumSet<K> arrayToEnumSet(K[] a) {
    return EnumSet.copyOf(ImmutableSet.copyOf(a));
  }

  public static <T, V> List<V> applyFunction(List<T> list, Function<T, V> function) {
    return list.stream().map(function).collect(Collectors.toList());
  }

  public static <T extends Number> String numberSetToHexString(Set<T> set) {
    return numberSetToHexString(set, defaultStartBrace, defaultEndBrace, defaultSeparator);
  }

  public static <T extends Number> String numberSetToHexString(
      Set<T> set, String startBrace, String endBrace, char separator) {
    StringBuffer sb;
    boolean initial;

    initial = true;
    sb = new StringBuffer();
    sb.append(startBrace);
    for (Number n : set) {
      if (initial) {
        initial = false;
      } else {
        sb.append(separator);
      }
      sb.append(String.format("%x", n));
    }
    sb.append(endBrace);
    return sb.toString();
  }
}
