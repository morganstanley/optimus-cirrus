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
package com.ms.silverking.cloud.dht.config;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.HashMap;
import java.util.Map;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class WeightsTable {
  private final Map<String, Double> nodeWeights;

  private static Logger log = LoggerFactory.getLogger(WeightsTable.class);

  public WeightsTable(Map<String, Double> nodeWeights) {
    this.nodeWeights = nodeWeights;
  }

  public static WeightsTable parse(File fileName) throws IOException {
    return parse(new FileInputStream(fileName));
  }

  public static WeightsTable parse(InputStream inStream) throws IOException {
    try {
      BufferedReader reader;
      String line;
      Map<String, Double> nodeWeights;

      nodeWeights = new HashMap<String, Double>();
      reader = new BufferedReader(new InputStreamReader(inStream));
      do {
        line = reader.readLine();
        if (line != null) {
          line = line.trim();
          if (line.length() != 0) {
            String[] tokens;

            tokens = line.split("\\s+");
            if (tokens.length != 2) {
              throw new IOException("Invalid weight line: " + line);
            } else {
              nodeWeights.put(tokens[0], Double.parseDouble(tokens[1]));
            }
          }
        }
      } while (line != null);
      return new WeightsTable(nodeWeights);
    } finally {
      inStream.close();
    }
  }

  public void display() {
    for (Map.Entry<String, Double> entry : nodeWeights.entrySet()) {
      log.info("{}  {}", entry.getKey(), entry.getValue());
    }
  }
}
