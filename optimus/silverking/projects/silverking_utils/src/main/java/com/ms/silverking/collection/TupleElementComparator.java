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

import java.util.Comparator;

public class TupleElementComparator implements Comparator<TupleBase> {

  private final int elementIndex;

  private final Comparator elementComparator;

  public TupleElementComparator(int elementIndex, Comparator elementComparator) {

    this.elementComparator = elementComparator;

    this.elementIndex = elementIndex;
  }

  @Override
  public int compare(TupleBase o1, TupleBase o2) {

    return elementComparator.compare(o1.getElement(elementIndex), o2.getElement(elementIndex));
  }
}
