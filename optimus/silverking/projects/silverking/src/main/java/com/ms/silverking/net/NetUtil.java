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
package com.ms.silverking.net;

import java.math.BigDecimal;
import java.math.MathContext;

import com.ms.silverking.time.Stopwatch;

public class NetUtil {
  private static final double megaBase10 = 1000000.0;

  public static double calcMbps(long bytes, double seconds) {
    return calc_bps(bytes, seconds) / megaBase10;
  }

  public static double calc_bps(long bytes, double seconds) {
    return new BigDecimal(bytes, MathContext.DECIMAL128)
        .multiply(new BigDecimal(8, MathContext.DECIMAL128))
        .divide(new BigDecimal(seconds, MathContext.DECIMAL128), MathContext.DECIMAL128)
        .doubleValue();
    // return (double)(bytes * 8) / seconds;
  }

  public static double calcMbps(long bytes, Stopwatch sw) {
    return calcMbps(bytes, sw.getElapsedSeconds());
  }

  public static double calc_bps(long bytes, Stopwatch sw) {
    return calc_bps(bytes, sw.getElapsedSeconds());
  }

  public static double calcMBps(long bytes, double seconds) {
    return calcMbps(bytes, seconds) / 8.0;
  }

  public static double calcMBps(long bytes, Stopwatch sw) {
    return calcMbps(bytes, sw) / 8.0;
  }
}
