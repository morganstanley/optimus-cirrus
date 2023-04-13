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
package com.ms.silverking.net.analysis.netstat;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import com.ms.silverking.io.StreamParser;
import com.ms.silverking.numeric.StatSeries;

public class Trace {
  private final String name;
  private final List<StatSample> samples;

  private final double intervalOutlierThreshold = 0.05;

  public Trace(String name, List<StatSample> samples) {
    this.name = name;
    this.samples = samples;
  }

  public String getName() {
    return name;
  }

  public List<StatSample> getSamples() {
    return samples;
  }

  public static Trace parse(File file) throws IOException {
    if (file.getName().endsWith(".gz")) {
      return parse(file.getName(), StreamParser.parseGZFileLines(file, StreamParser.TrimMode.noTrim));
    } else {
      return parse(file.getName(), StreamParser.parseFileLines(file, StreamParser.TrimMode.noTrim));
    }
  }

  public static int nextSample(List<String> def, int start) {
    if (start > def.size() - 3) {
      return def.size();
    } else {
      for (int i = start; i < def.size() - 2; i++) {
        if (def.get(i).matches("^\\d*$")) {
          //&& def.get(i).matches("")) {
          return i;
        }
      }
      return def.size();
    }
  }

  public static Trace parse(String name, List<String> def) {
    List<StatSample> samples;
    int sampleStart;

    samples = new ArrayList<>();
    sampleStart = nextSample(def, 0);
    try {
      while (sampleStart < def.size()) {
        int nextSample;

        nextSample = nextSample(def, sampleStart + 1);
        //System.out.printf("sampleStart: %d\tnextSample: %d\n", sampleStart, nextSample);
        if (nextSample > sampleStart + 2) {
          samples.add(StatSample.parse(def.subList(sampleStart, nextSample)));
        }
        sampleStart = nextSample;
      }
    } catch (RuntimeException re) {
      re.printStackTrace();
      // terminate attempt to parse and return what we got
    }
    return new Trace(name, samples);
  }

  public int intervalMillis() {
    List<Double> dSamples;
    long median;
    long maxDeviation;
    long intervalTotal;
    long intervalSamples;

    dSamples = new ArrayList<>(samples.size() - 1);
    for (int i = 0; i < samples.size() - 1; i++) {
      long interval;

      interval = samples.get(i + 1).getAbsTimeMillis() - samples.get(i).getAbsTimeMillis();
      dSamples.add(new Double(interval));
    }
    median = (long) new StatSeries(dSamples).median();
    maxDeviation = (long) ((double) median * intervalOutlierThreshold);

    intervalTotal = 0;
    intervalSamples = 0;
    for (int i = 0; i < samples.size() - 1; i++) {
      long interval;

      interval = samples.get(i + 1).getAbsTimeMillis() - samples.get(i).getAbsTimeMillis();
      if (Math.abs(interval - median) <= maxDeviation) {
        intervalSamples++;
        intervalTotal += interval;
      }
    }
    return (int) (intervalTotal / intervalSamples);
  }

  //public static int intervalMillis(Iterable<Trace> traces) {
  //long    total;

  //    for (Trace trace )
  //}

  @Override
  public String toString() {
    StringBuilder sb;

    sb = new StringBuilder();
    for (StatSample sample : samples) {
      sb.append(sample);
    }
    return sb.toString();
  }

  /**
   * @param args
   */
  public static void main(String[] args) {
    try {
      if (args.length != 1) {
        System.out.println("args: <traceFile>");
      } else {
        File traceFile;

        traceFile = new File(args[0]);
        System.out.println(Trace.parse(traceFile));
      }
    } catch (Exception e) {
      e.printStackTrace();
    }
  }
}
