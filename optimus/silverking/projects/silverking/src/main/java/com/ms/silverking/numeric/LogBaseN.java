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

public final class LogBaseN {
  private final double base;
  private final double logBase;

  public static final LogBaseN log2 = new LogBaseN(2.0);
  public static final LogBaseN log10 = new LogBaseN(10.0);

  public LogBaseN(double base) {
    this.base = base;
    this.logBase = Math.log(base);
  }

  public double log(double x) {
    return Math.log(x) / logBase;
  }

  public String toString() {
    return "log " + base;
  }
}
