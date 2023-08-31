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

public class SummaryStat {
  private final long total;
  private final long mean;
  private final long median;
  private final long max;
  private final long min;

  public static String[] stats = {"total", "mean", "median", "max", "min"};

  public SummaryStat(long total, long mean, long median, long max, long min) {
    this.total = total;
    this.mean = mean;
    this.median = median;
    this.max = max;
    this.min = min;
  }

  public long getStat(String stat) {
    switch (stat) {
      case "total":
        return getTotal();
      case "mean":
        return getMean();
      case "median":
        return getMedian();
      case "max":
        return getMax();
      case "min":
        return getMin();
      default:
        throw new RuntimeException("Unknown stat: " + stat);
    }
  }

  public long getTotal() {
    return total;
  }

  public long getMean() {
    return mean;
  }

  public long getMedian() {
    return median;
  }

  public long getMax() {
    return max;
  }

  public long getMin() {
    return min;
  }
}
