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

import com.ms.silverking.text.StringUtil;

public class Quintuple<T1, T2, T3, T4, T5> extends TupleBase {
  private final T1 v1;
  private final T2 v2;
  private final T3 v3;
  private final T4 v4;
  private final T5 v5;

  private static final long serialVersionUID = -660039701880105184L;

  private static final int SIZE = 5;

  public Quintuple(T1 v1, T2 v2, T3 v3, T4 v4, T5 v5) {
    super(SIZE);
    this.v1 = v1;
    this.v2 = v2;
    this.v3 = v3;
    this.v4 = v4;
    this.v5 = v5;
  }

  public static <T1, T2, T3, T4, T5> Quintuple<T1, T2, T3, T4, T5> of(
      T1 v1, T2 v2, T3 v3, T4 v4, T5 v5) {
    return new Quintuple<>(v1, v2, v3, v4, v5);
  }

  public static <T1, T2, T3, T4, T5> Quintuple<T1, T2, T3, T4, T5> of(
      T1 v1, Quadruple<T2, T3, T4, T5> t1) {
    return new Quintuple<>(v1, t1.getV1(), t1.getV2(), t1.getV3(), t1.getV4());
  }

  public static <T1, T2, T3, T4, T5> Quintuple<T1, T2, T3, T4, T5> of(
      Quadruple<T1, T2, T3, T4> t1, T5 v5) {
    return new Quintuple<>(t1.getV1(), t1.getV2(), t1.getV3(), t1.getV4(), v5);
  }

  public Object getElement(int index) {
    if (index == 0) {
      return v1;
    } else if (index == 1) {
      return v2;
    } else if (index == 2) {
      return v3;
    } else if (index == 3) {
      return v4;
    } else if (index == 4) {
      return v5;
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

  public T4 getV4() {
    return v4;
  }

  public T5 getV5() {
    return v5;
  }

  public T1 getHead() {
    return v1;
  }

  public Quadruple<T2, T3, T4, T5> getTail() {
    return getQuadrupleAt2();
  }

  public Pair<T1, T2> getPairAt1() {
    return new Pair<>(v1, v2);
  }

  public Pair<T2, T3> getPairAt2() {
    return new Pair<>(v2, v3);
  }

  public Pair<T3, T4> getPairAt3() {
    return new Pair<>(v3, v4);
  }

  public Pair<T4, T5> getPairAt4() {
    return new Pair<>(v4, v5);
  }

  public Triple<T1, T2, T3> getTripleAt1() {
    return new Triple<>(v1, v2, v3);
  }

  public Triple<T2, T3, T4> getTripleAt2() {
    return new Triple<>(v2, v3, v4);
  }

  public Triple<T3, T4, T5> getTripleAt3() {
    return new Triple<>(v3, v4, v5);
  }

  public Quadruple<T1, T2, T3, T4> getQuadrupleAt1() {
    return new Quadruple<>(v1, v2, v3, v4);
  }

  public Quadruple<T2, T3, T4, T5> getQuadrupleAt2() {
    return new Quadruple<>(v2, v3, v4, v5);
  }

  @Override
  public int hashCode() {
    return v1.hashCode() ^ v2.hashCode() ^ v3.hashCode() ^ v4.hashCode() ^ v5.hashCode();
  }

  @Override
  public boolean equals(Object other) {
    if (other == this) {
      return true;
    } else {
      Quintuple<T1, T2, T3, T4, T5> o;

      o = (Quintuple<T1, T2, T3, T4, T5>) other;
      return v1.equals(o.v1) && v2.equals(o.v2) && v3.equals(o.v3) && v4.equals(o.v4);
    }
  }

  @Override
  public String toString() {
    return StringUtil.nullSafeToString(v1)
        + ":"
        + StringUtil.nullSafeToString(v2)
        + ":"
        + StringUtil.nullSafeToString(v3)
        + ":"
        + StringUtil.nullSafeToString(v4)
        + ":"
        + StringUtil.nullSafeToString(v5);
  }

  public static <T1, T2, T3, T4, T5> Quintuple<T1, T2, T3, T4, T5> parse(
      String def, String pattern, String... typeNames) {
    List<Object> l;

    l = TupleUtil.parse(def, pattern, SIZE, typeNames);
    return new Quintuple(l.get(0), l.get(1), l.get(2), l.get(3), l.get(4));
  }
}
