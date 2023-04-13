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
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import com.ms.silverking.text.StringUtil;

public class MultiTraceToCSV {
  private final Trace[] traces;
  private final String[] statDefs;
  private final Set<String> columns;
  private final List<Map<String, String>> data;
  private long startTimeMillis;

  private static final String sampleIndexColumn = "Sample";
  private static final String secondsColumn = "Seconds";

  private static final double timeTolerance = 0.05;

  public MultiTraceToCSV(Trace[] traces, String[] statDefs) {
    this.traces = traces;
    this.statDefs = statDefs;
    columns = new HashSet<>();
    data = new ArrayList<>();
    columns.add(sampleIndexColumn);
    columns.add(secondsColumn);
  }

  public void generateCSV() {
    Map<String, Long> initialValues;

    initialValues = new HashMap<>();
    for (Trace trace : traces) {
      for (StatSample statSample : trace.getSamples()) {
        generateSample(initialValues, statSample);
      }
    }
    displayData();
  }

  private void generateSample(Map<String, Long> initialValues, StatSample statSample) {
    Map<String, String> sampleData;
    double timeDelta;

    sampleData = new HashMap<>();
    sampleData.put(sampleIndexColumn, Integer.toString(statSample.getSampleIndex()));
    if (startTimeMillis == 0) {
      startTimeMillis = statSample.getAbsTimeMillis();
      timeDelta = 0.0;
    } else {
      timeDelta = (double) (statSample.getAbsTimeMillis() - startTimeMillis) / 1000.0;
    }
    sampleData.put(secondsColumn, Double.toString(timeDelta));
    for (String statDef : statDefs) {
      generateStats(initialValues, statSample, sampleData, statDef);
    }
    data.add(sampleData);
  }

  private void generateStats(Map<String, Long> initialValues, StatSample statSample, Map<String, String> sampleData,
      String statDef) {
    for (Stat stat : statSample.getMatchingStats(statDef)) {
      long delta;

      if (columns.add(stat.getFullName())) {
        initialValues.put(stat.getFullName(), stat.getValue());
      }
      delta = stat.getValue() - initialValues.get(stat.getFullName());
      sampleData.put(stat.getFullName(), Long.toString(delta));
    }
  }

  private void displayData() {
    System.out.printf("%10s,", "");
    for (String column : columns) {
      System.out.printf("%10s,", column);
    }
    System.out.println();
    for (Map<String, String> sampleData : data) {
      System.out.printf("%10s,", "");
      for (String column : columns) {
        System.out.printf("%10s,", sampleData.get(column));
      }
      System.out.println();
    }
  }

  /**
   * @param args
   */
  public static void main(String[] args) {
    try {
      if (args.length < 2) {
        System.out.println("args: <traceFile...> <statDef...>");
      } else {
        File[] traceFiles;
        MultiTraceToCSV traceToCSV;
        String[] statDefs;
        Trace[] traces;

        traceFiles = new File[args.length - 1];
        for (int i = 0; i < traceFiles.length; i++) {
          traceFiles[i] = new File(args[i]);
        }

        statDefs = StringUtil.splitAndTrim(args[args.length - 1]);
        traces = new Trace[traceFiles.length];
        for (int i = 0; i < traces.length; i++) {
          traces[i] = Trace.parse(traceFiles[i]);
        }
        traceToCSV = new MultiTraceToCSV(traces, statDefs);
        traceToCSV.generateCSV();
      }
    } catch (Exception e) {
      e.printStackTrace();
    }
  }
}
