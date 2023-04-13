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
package com.ms.silverking.cloud.dht.meta;

import java.io.BufferedReader;
import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.ms.silverking.cloud.meta.VersionedDefinition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ClassVars implements VersionedDefinition {
  private final Map<String, String> classVars;
  private final long version;

  private static Logger log = LoggerFactory.getLogger(ClassVars.class);

  private static final String commentEscape = "#";

  public ClassVars(Map<String, String> classVars, long version) {
    this.classVars = classVars;
    this.version = version;
  }

  public Map<String, String> getVarMap() {
    return classVars;
  }

  @Override
  public long getVersion() {
    return version;
  }

  public static ClassVars parse(String def, long version) {
    try {
      return parse(new ByteArrayInputStream(def.getBytes()), version);
    } catch (IOException ioe) {
      throw new RuntimeException("Unexpected exception", ioe);
    }
  }

  public static ClassVars parse(File fileName, long version) throws IOException {
    return parse(new FileInputStream(fileName), version);
  }

  public static ClassVars parse(InputStream inStream, long version) throws IOException {
    try {
      BufferedReader reader;
      String line;
      Map<String, String> map;

      map = new HashMap<>();
      reader = new BufferedReader(new InputStreamReader(inStream));
      do {
        line = reader.readLine();
        readLine(line, map);
      } while (line != null);
      return new ClassVars(map, version);
    } finally {
      inStream.close();
    }
  }

  private static void readLine(String line, Map<String, String> classVars) {
    if (line != null) {
      line = line.trim();
      if (line.length() > 0 && !line.startsWith(commentEscape)) {
        String[] tokens;

        tokens = line.split("=");
        if (tokens.length == 2) {
          classVars.put(tokens[0], tokens[1]);
        } else {
          throw new RuntimeException("Invalid variable definition: " + line);
        }
      }
    }
  }

  @Override
  public String toString() {
    StringBuilder sb;
    List<String> vars;

    vars = new ArrayList<>(classVars.keySet());
    Collections.sort(vars);

    sb = new StringBuilder();
    for (String var : vars) {
      sb.append(var);
      sb.append('=');
      sb.append(classVars.get(var));
      sb.append('\n');
    }
    return sb.toString();
  }

  // for unit testing
  public static void main(String[] args) {
    try {
      if (args.length != 1) {
        log.error("<file>");
      } else {
        ClassVars cv;

        cv = ClassVars.parse(new File(args[0]), 0);
        log.info(cv.toString());
      }
    } catch (IOException ioe) {
      ioe.printStackTrace();
    }
  }

  public ClassVars overrideWith(ClassVars classVars) {
    Map<String, String> newMap;

    newMap = new HashMap<>(this.getVarMap());
    newMap.putAll(classVars.getVarMap());
    return new ClassVars(newMap, classVars.version);
  }
}
