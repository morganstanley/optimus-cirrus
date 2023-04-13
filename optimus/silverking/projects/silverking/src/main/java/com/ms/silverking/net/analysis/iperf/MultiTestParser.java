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

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class MultiTestParser {
  private List<Test> tests;
  private int maxMeasurements;

  public MultiTestParser() {
    tests = new ArrayList<>();
    maxMeasurements = 0;
  }

  public void parse(File file) throws IOException {
    Test test;

    test = Test.parse(file);
    tests.add(test);
    maxMeasurements = Math.max(test.getMeasurements().size(), maxMeasurements);
  }

  public void display() {
    for (int i = 0; i < maxMeasurements; i++) {
      for (Test test : tests) {
        System.out.print(test.getMeasurements().get(i).toIDBandwidthString());
        System.out.print("\t");
      }
      System.out.println();
    }
    for (String stat : SummaryStat.stats) {
      for (Test test : tests) {
        System.out.print(stat);
        System.out.print("\t");
        System.out.print(test.getSummaryStat().getStat(stat));
        System.out.print("\t");
      }
      System.out.println();
    }
  }

  /**
   * @param args
   */
  public static void main(String[] args) {
    try {
      if (args.length == 0) {
        System.out.println("args: <testFile...>");
      } else {
        MultiTestParser multiTestParser;

        multiTestParser = new MultiTestParser();
        for (String fileName : args) {
          multiTestParser.parse(new File(fileName));
        }
        multiTestParser.display();
      }
    } catch (Exception e) {
      e.printStackTrace();
    }
  }
}
