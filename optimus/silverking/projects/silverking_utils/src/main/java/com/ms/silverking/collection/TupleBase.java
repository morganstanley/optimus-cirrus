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

import java.io.Serializable;

public abstract class TupleBase implements Serializable {
  private static final long serialVersionUID = 7624118038203845139L;

  private final int size;

  static final String defaultTupleParsePattern = ",";

  TupleBase(int size) {
    this.size = size;
  }

  public int getSize() {
    return size;
  }

  public abstract Object getElement(int index);
}
