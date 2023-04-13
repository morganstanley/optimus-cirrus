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
package com.ms.silverking.net.analysis.iperf;

public class TimeInterval {
  private final double start;
  private final double end;

  public TimeInterval(double start, double end) {
    this.start = start;
    this.end = end;
  }

  public static TimeInterval parse(String s) {
    int i;
    String[] defs;

    i = s.indexOf("sec");
    if (i >= 0) {
      s = s.substring(0, i);
    }
    defs = s.split("-");
    if (defs.length != 2) {
      throw new RuntimeException("Invalid format: " + s);
    }
    return new TimeInterval(Double.parseDouble(defs[0]), Double.parseDouble(defs[1]));
  }

  public double getStart() {
    return start;
  }

  public double getEnd() {
    return end;
  }

  @Override
  public String toString() {
    return start + "-" + end + " sec";
  }
}
