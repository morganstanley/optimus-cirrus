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
package optimus.dht.common.util;

import java.util.Arrays;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;
import java.util.function.Function;
import java.util.stream.Collectors;

import org.HdrHistogram.Histogram;

import com.google.common.io.BaseEncoding;

public class StringTool {

  private static final BaseEncoding BASE16_LOWERCASE = BaseEncoding.base16().lowerCase();

  public static final int ONE_MB = 1 << 20;

  public static String formatMem(long used) {
    return String.format("%.2f MB", ((double) used) / ONE_MB);
  }

  public static String formatMemUsage(long used, long max) {
    return String.format("%.2f MB (%.2f%%)", ((double) used) / ONE_MB, ((double) used / max * 100));
  }

  public static String formatPercent(long used, long max) {
    return formatPercent(((double) used) / max);
  }

  public static String formatPercent(double fraction) {
    return String.format("%.2f %%", fraction * 100);
  }

  public static String formatNanosAsMillisFraction(long nanosDuration) {
    return String.format("%.3f ms", (double) nanosDuration / 1_000_000);
  }

  public static String formatByteHash(byte[] byteHash) {
    return formatByteHash(byteHash, 16);
  }

  public static String formatByteHash(byte[] byteHash, int visibleLength) {
    return (byteHash.length < visibleLength)
        ? BASE16_LOWERCASE.encode(byteHash)
        : BASE16_LOWERCASE.encode(byteHash, 0, visibleLength);
  }

  public static String formatByteHashArray(byte[][] byteHashArray) {
    return Arrays.stream(byteHashArray)
        .map(StringTool::formatByteHash)
        .collect(Collectors.joining(","));
  }

  public static <T> void appendHistStats(
      StringBuilder sb,
      String prefix,
      boolean isNs,
      Map<String, T> intervalStats,
      T totalStats,
      Function<T, Histogram> extractor) {
    Iterator<Entry<String, T>> iterator = intervalStats.entrySet().iterator();
    while (iterator.hasNext()) {
      Entry<String, T> entry = iterator.next();
      Histogram histogram = extractor.apply(entry.getValue());
      append(sb, prefix + "_" + entry.getKey(), isNs, histogram);
    }
    append(sb, prefix + "_TOTAL", isNs, extractor.apply(totalStats));
    sb.append("\n");
  }

  private static void append(StringBuilder sb, String name, boolean isNs, Histogram histogram) {
    if (histogram.getTotalCount() > 0) {
      sb.append("  ");
      sb.append(name).append("_count=").append(histogram.getTotalCount()).append(", ");
      sb.append(name).append("_avg=").append(format((long) histogram.getMean(), isNs)).append(", ");
      sb.append(name).append("_min=").append(format(histogram.getMinValue(), isNs)).append(", ");
      sb.append(name)
          .append("_p25=")
          .append(format(histogram.getValueAtPercentile(25), isNs))
          .append(", ");
      sb.append(name)
          .append("_p50=")
          .append(format(histogram.getValueAtPercentile(50), isNs))
          .append(", ");
      sb.append(name)
          .append("_p75=")
          .append(format(histogram.getValueAtPercentile(75), isNs))
          .append(", ");
      sb.append(name)
          .append("_p90=")
          .append(format(histogram.getValueAtPercentile(90), isNs))
          .append(", ");
      sb.append(name)
          .append("_p99=")
          .append(format(histogram.getValueAtPercentile(99), isNs))
          .append(", ");
      sb.append(name)
          .append("_p999=")
          .append(format(histogram.getValueAtPercentile(99.9), isNs))
          .append(", ");
      sb.append(name).append("_max=").append(format(histogram.getMaxValue(), isNs)).append("\n");
    }
  }

  private static String format(long value, boolean isNs) {
    return isNs ? StringTool.formatNanosAsMillisFraction(value) : String.valueOf(value);
  }

  public static <T> void appendDataStats(
      StringBuilder sb,
      String prefix,
      Map<String, T> intervalStats,
      T totalStats,
      Function<T, Long> extractor) {
    Long totalData = extractor.apply(totalStats);
    if (totalData == 0) {
      return;
    }
    sb.append("  ");
    Iterator<Entry<String, T>> iterator = intervalStats.entrySet().iterator();
    while (iterator.hasNext()) {
      Entry<String, T> entry = iterator.next();
      Long categoryData = extractor.apply(entry.getValue());
      if (categoryData == 0) {
        continue;
      }
      sb.append(prefix)
          .append("_")
          .append(entry.getKey())
          .append("=")
          .append(StringTool.formatMem(categoryData))
          .append(", ");
    }
    sb.append(prefix).append("_TOTAL=").append(StringTool.formatMem(totalData)).append("\n");
  }
}
