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

import com.ms.silverking.cloud.dht.client.gen.OmitGeneration;
import com.ms.silverking.text.CoreStringUtil;

@OmitGeneration
public class Pair<T1, T2> extends TupleBase {
  private final T1 v1;
  private final T2 v2;

  private static final long serialVersionUID = 4842255422592843585L;

  private static final int SIZE = 2;

  public Pair(T1 v1, T2 v2) {
    super(SIZE);
    this.v1 = v1;
    this.v2 = v2;
  }

  public static <T1, T2> Pair<T1, T2> of(T1 v1, T2 v2) {
    return new Pair<>(v1, v2);
  }

  public Object getElement(int index) {
    if (index == 0) {
      return v1;
    } else if (index == 1) {
      return v2;
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

  @Override
  public int hashCode() {
    return v1.hashCode() ^ v2.hashCode();
  }

  @Override
  public boolean equals(Object other) {
    if (other == this) {
      return true;
    } else {
      Pair<T1, T2> oPair;

      oPair = (Pair<T1, T2>) other;
      return v1.equals(oPair.v1) && v2.equals(oPair.v2);
    }
  }

  @Override
  public String toString() {
    return CoreStringUtil.nullSafeToString(v1) + ":" + CoreStringUtil.nullSafeToString(v2);
  }
}
