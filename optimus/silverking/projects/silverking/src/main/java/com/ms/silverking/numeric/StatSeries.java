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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;

import com.google.common.collect.ImmutableList;
import com.ms.silverking.util.ArrayUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public final class StatSeries {
  private final double[] samples;
  private final double mean;
  private final double median;
  private final double variance;
  private final double standardDeviation;
  private final double min;
  private final double max;
  private final double sum;

  private static Logger log = LoggerFactory.getLogger(StatSeries.class);

  public StatSeries(Collection<Double> sourceSamples) {
    int i;
    double _sum;
    double vsum;

    samples = new double[sourceSamples.size()];
    i = 0;
    for (double sample : sourceSamples) {
      samples[i++] = sample;
    }

    // compute stats
    _sum = 0.0;
    for (double sample : samples) {
      _sum += sample;
    }
    sum = _sum;
    mean = _sum / (double) samples.length;

    vsum = 0.0;
    for (double sample : samples) {
      vsum += (sample - mean) * (sample - mean);
    }
    variance = vsum / ((double) samples.length - 1.0);
    standardDeviation = Math.sqrt(variance);

    Arrays.sort(samples);
    if (samples.length > 0) {
      min = samples[0];
      max = samples[samples.length - 1];
      if (samples.length % 2 == 1) {
        median = samples[samples.length / 2];
      } else {
        median = (samples[samples.length / 2] + samples[samples.length / 2 - 1]) / 2.0;
      }
    } else {
      min = Double.NaN;
      max = Double.NaN;
      median = Double.NaN;
    }
  }

  public StatSeries(double[] sourceSamples) {
    this(ImmutableList.copyOf(ArrayUtil.doubleToDouble(sourceSamples)));
  }

  public static StatSeries fromLongSamples(Collection<Long> sourceSamples) {
    Collection<Double> dSamples;

    dSamples = new ArrayList<>(sourceSamples.size());
    for (Long sample : sourceSamples) {
      dSamples.add(new Double(sample));
    }
    return new StatSeries(dSamples);
  }

  public double percentile(double p) {
    if (samples.length > 0) {
      int i;

      i = (int) Math.round(((double) samples.length * (p / 100.0) + 0.5)) - 1;
      return samples[i];
    } else {
      return Double.NaN;
    }
  }

  public double sum() {
    return sum;
  }

  public double median() {
    return median;
  }

  public double mean() {
    return mean;
  }

  public double max() {
    return max;
  }

  public double min() {
    return min;
  }

  public double standardDeviation() {
    return standardDeviation;
  }

  public double variance() {
    return variance;
  }

  public String toString() {
    StringBuilder sb;

    sb = new StringBuilder();
    sb.append(toSummaryString());
    sb.append('\t');
    for (double sample : samples) {
      sb.append(sample);
      sb.append('\t');
    }
    return sb.toString();
  }

  public String toSummaryString() {
    StringBuilder sb;

    sb = new StringBuilder();
    sb.append(mean);
    sb.append('\t');
    sb.append(median);
    sb.append('\t');
    sb.append(variance);
    sb.append('\t');
    sb.append(standardDeviation);
    sb.append('\t');
    sb.append(min);
    sb.append('\t');
    sb.append(max);
    sb.append('\t');
    sb.append(percentile(90));
    sb.append('\t');
    sb.append(percentile(95));
    sb.append('\t');
    sb.append(percentile(99));
    return sb.toString();
  }

  public String toSummaryStringLow() {
    StringBuilder sb;

    sb = new StringBuilder();
    sb.append(mean);
    sb.append('\t');
    sb.append(median);
    sb.append('\t');
    sb.append(variance);
    sb.append('\t');
    sb.append(standardDeviation);
    sb.append('\t');
    sb.append(min);
    sb.append('\t');
    sb.append(max);
    sb.append('\t');
    sb.append(percentile(1));
    sb.append('\t');
    sb.append(percentile(5));
    sb.append('\t');
    sb.append(percentile(10));
    return sb.toString();
  }

  public static String summaryHeaderString() {
    StringBuilder sb;

    sb = new StringBuilder();
    sb.append("mean");
    sb.append('\t');
    sb.append("median");
    sb.append('\t');
    sb.append("variance");
    sb.append('\t');
    sb.append("standardDeviation");
    sb.append('\t');
    sb.append("min");
    sb.append('\t');
    sb.append("max");
    sb.append('\t');
    sb.append("90%");
    sb.append('\t');
    sb.append("95%");
    sb.append('\t');
    sb.append("99%");
    return sb.toString();
  }

  public static String summaryHeaderStringLow() {
    StringBuilder sb;

    sb = new StringBuilder();
    sb.append("mean");
    sb.append('\t');
    sb.append("median");
    sb.append('\t');
    sb.append("variance");
    sb.append('\t');
    sb.append("standardDeviation");
    sb.append('\t');
    sb.append("min");
    sb.append('\t');
    sb.append("max");
    sb.append('\t');
    sb.append("1%");
    sb.append('\t');
    sb.append("5%");
    sb.append('\t');
    sb.append("10%");
    return sb.toString();
  }

  /**
   * @param args
   */
  public static void main(String[] args) {
    StatSeries series;
    List<Double> samples;

    if (args.length == 0) {
      System.out.println("args: <sample...>");
      return;
    }
    samples = new ArrayList<Double>();
    for (String arg : args) {
      samples.add(Double.parseDouble(arg));
    }
    series = new StatSeries(samples);
    System.out.println(series);
    for (double d = 0.0; d < 100.0; d += 5.0) {
      System.out.printf("%f\t%f\n", d, series.percentile(d));
    }
  }
}
