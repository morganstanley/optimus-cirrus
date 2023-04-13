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
package com.ms.silverking.numeric;

public class MutableInteger {
  private int value;

  public MutableInteger(int value) {
    this.value = value;
  }

  public MutableInteger() {
    this(0);
  }

  public int getValue() {
    return value;
  }

  public void increment() {
    value++;
  }

  public void decrement() {
    value--;
  }

  @Override
  public int hashCode() {
    return Integer.hashCode(value);
  }

  @Override
  public boolean equals(Object obj) {
    MutableInteger o;

    o = (MutableInteger) obj;
    return o.value == value;
  }

  @Override
  public String toString() {
    return Integer.toString(value);
  }
}
