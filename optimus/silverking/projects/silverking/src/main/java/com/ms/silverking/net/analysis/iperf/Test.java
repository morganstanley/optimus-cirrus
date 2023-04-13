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

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class Test {
  private final List<Measurement> measurements;
  private final SummaryStat summaryStat;

  public Test(List<Measurement> measurements) {
    this.measurements = measurements;
    this.summaryStat = computeSummaryStat(measurements);
  }

  private static SummaryStat computeSummaryStat(List<Measurement> measurements) {
    long total;
    long max;
    long min;
    long median;

    total = 0;
    min = Long.MAX_VALUE;
    max = Long.MIN_VALUE;
    for (Measurement measurement : measurements) {
      long bps;

      bps = measurement.getBitsPerSecond();
      total += bps;
      if (bps > max) {
        max = bps;
      }
      if (bps < min) {
        min = bps;
      }
    }
    Collections.sort(measurements, new BPSComparator());
    if (measurements.size() % 2 == 0) {
      median = (measurements.get(measurements.size() / 2).getBitsPerSecond() + measurements.get(
          measurements.size() / 2 - 1).getBitsPerSecond()) / 2;
    } else {
      median = measurements.get(measurements.size() / 2 - 1).getBitsPerSecond();
    }
    return new SummaryStat(total, total / measurements.size(), median, max, min);
  }

  public List<Measurement> getMeasurements() {
    return measurements;
  }

  public SummaryStat getSummaryStat() {
    return summaryStat;
  }

  public static Test parse(File file) throws IOException {
    return parse(new FileInputStream(file));
  }

  public static Test parse(InputStream inStream) throws IOException {
    return parse(new BufferedReader(new InputStreamReader(inStream)));
  }

  private static Test parse(BufferedReader reader) throws IOException {
    List<Measurement> measurements;
    String line;

    measurements = new ArrayList<>();
    do {
      line = reader.readLine();
      if (line != null) {
        measurements.add(Measurement.parse(line.trim()));
      }
    } while (line != null);
    return new Test(measurements);
  }
}
