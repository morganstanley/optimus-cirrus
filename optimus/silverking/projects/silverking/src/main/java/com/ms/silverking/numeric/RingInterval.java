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

public class RingInterval {
  private final RingInteger min;
  private final RingInteger max;

  public RingInterval(RingInteger min, RingInteger max) {
    this.min = min;
    this.max = max;
  }

  public boolean contains(RingInteger x) {
    RingInteger.ensureRingShared(min, max);
    RingInteger.ensureRingShared(min, x);

    if (max.value > min.value) {
      return x.value >= min.value && x.value <= max.value;
    } else if (max.value < min.value) {
      return x.value <= max.value || x.value >= min.value;
    } else {
      return x.value == max.value;
    }
  }
}
