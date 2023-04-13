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
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class TraceToCSV {
  private final Trace trace;
  private final String[] statDefs;
  private final Set<String> columns;
  private final List<Map<String, String>> data;
  private final Map<String, Long> initialValues;
  private long startTimeMillis;

  private static final String sampleIndexColumn = "Sample";
  private static final String secondsColumn = "Seconds";

  public TraceToCSV(Trace trace, String[] statDefs) {
    this.trace = trace;
    this.statDefs = statDefs;
    columns = new HashSet<>();
    data = new ArrayList<>();
    columns.add(sampleIndexColumn);
    columns.add(secondsColumn);
    initialValues = new HashMap<>();
  }

  public void generateCSV() {
    for (StatSample statSample : trace.getSamples()) {
      generateSample(statSample);
    }
    displayData();
  }

  private void generateSample(StatSample statSample) {
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
      generateStats(statSample, sampleData, statDef);
    }
    data.add(sampleData);
  }

  private void generateStats(StatSample statSample, Map<String, String> sampleData, String statDef) {
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
    List<String> sortedColumns;

    sortedColumns = new ArrayList<>(columns);
    Collections.sort(sortedColumns);
    System.out.printf("%10s,", "");
    for (String column : sortedColumns) {
      System.out.printf("%10s,", column);
    }
    System.out.println();
    for (Map<String, String> sampleData : data) {
      System.out.printf("%10s,", trace.getName());
      for (String column : sortedColumns) {
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
        System.out.println("args: <traceFile> <statDef...>");
      } else {
        File traceFile;
        TraceToCSV traceToCSV;
        String[] statDefs;

        traceFile = new File(args[0]);
        statDefs = new String[args.length - 1];
        for (int i = 0; i < args.length - 1; i++) {
          statDefs[i] = args[i + 1];
        }
        traceToCSV = new TraceToCSV(Trace.parse(traceFile), statDefs);
        traceToCSV.generateCSV();
      }
    } catch (Exception e) {
      e.printStackTrace();
    }
  }
}
