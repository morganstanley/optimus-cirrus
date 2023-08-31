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

import java.util.List;

import com.ms.silverking.text.CoreStringUtil;

public class Triple<T1, T2, T3> extends TupleBase {
  private final T1 v1;
  private final T2 v2;
  private final T3 v3;

  private static final long serialVersionUID = -4851691484874117524L;

  private static final int SIZE = 3;

  public Triple(T1 v1, T2 v2, T3 v3) {
    super(SIZE);
    this.v1 = v1;
    this.v2 = v2;
    this.v3 = v3;
  }

  public static <T1, T2, T3> Triple<T1, T2, T3> of(T1 v1, T2 v2, T3 v3) {
    return new Triple<>(v1, v2, v3);
  }

  public static <T1, T2, T3> Triple<T1, T2, T3> of(T1 v1, Pair<T2, T3> p1) {
    return new Triple<>(v1, p1.getV1(), p1.getV2());
  }

  public static <T1, T2, T3> Triple<T1, T2, T3> of(Pair<T1, T2> p1, T3 v3) {
    return new Triple<>(p1.getV1(), p1.getV2(), v3);
  }

  public Object getElement(int index) {
    if (index == 0) {
      return v1;
    } else if (index == 1) {
      return v2;
    } else if (index == 2) {
      return v3;
    } else {
      throw new IndexOutOfBoundsException(Integer.toString(index));
    }
  }

  public T1 getV1() {
    return v1;
  }

  public T2 getV2() {
    return v2;
  }

  public T3 getV3() {
    return v3;
  }

  public T1 getHead() {
    return v1;
  }

  public Pair<T2, T3> getTail() {
    return getPairAt2();
  }

  public Pair<T1, T2> getPairAt1() {
    return new Pair<>(v1, v2);
  }

  public Pair<T2, T3> getPairAt2() {
    return new Pair<>(v2, v3);
  }

  @Override
  public int hashCode() {
    return v1.hashCode() ^ v2.hashCode() ^ v3.hashCode();
  }

  @Override
  public boolean equals(Object other) {
    if (other == this) {
      return true;
    } else {
      Triple<T1, T2, T3> oTriple;

      oTriple = (Triple<T1, T2, T3>) other;
      return v1.equals(oTriple.v1) && v2.equals(oTriple.v2) && v3.equals(oTriple.v3);
    }
  }

  @Override
  public String toString() {
    return CoreStringUtil.nullSafeToString(v1)
        + ":"
        + CoreStringUtil.nullSafeToString(v2)
        + ":"
        + CoreStringUtil.nullSafeToString(v3);
  }

  public static <T1, T2, T3> Triple<T1, T2, T3> parseDefault(String def, String... typeNames) {
    return parse(def, TupleBase.defaultTupleParsePattern, typeNames);
  }

  public static <T1, T2, T3> Triple<T1, T2, T3> parse(
      String def, String pattern, String... typeNames) {
    List<Object> l;

    l = TupleUtil.parse(def, pattern, SIZE, typeNames);
    return new Triple(l.get(0), l.get(1), l.get(2));
  }
}
