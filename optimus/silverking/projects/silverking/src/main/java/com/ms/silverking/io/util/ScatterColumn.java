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
package com.ms.silverking.io.util;

import java.io.File;
import java.io.IOException;
import java.util.List;

import com.ms.silverking.io.StreamParser;
import com.ms.silverking.io.StreamParser.TrimMode;
import org.slf4j.LoggerFactory;
import org.slf4j.Logger;

public class ScatterColumn {

  private static Logger log = LoggerFactory.getLogger(ScatterColumn.class);

  public static void scatter(File inputFile, int scatterSize) throws IOException {
    List<String> lines;

    lines = StreamParser.parseFileLines(inputFile, TrimMode.trim);
    for (int i = 0; i < lines.size(); i++) {
      log.info(lines.get(i) + "\t");
      if (i % scatterSize == scatterSize - 1) {
        System.out.println();
      }
    }
  }

  /**
   * @param args
   */
  public static void main(String[] args) {
    try {
      if (args.length != 2) {
        log.info("args: <file> <scatterSize>");
      } else {
        File file;
        int scatterSize;

        file = new File(args[0]);
        scatterSize = Integer.parseInt(args[1]);
        scatter(file, scatterSize);
      }
    } catch (Exception e) {
      e.printStackTrace();
    }
  }
}
