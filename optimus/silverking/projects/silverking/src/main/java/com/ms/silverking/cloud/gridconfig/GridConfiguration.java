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
package com.ms.silverking.cloud.gridconfig;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;

import com.ms.silverking.util.PropertiesHelper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Provides coherent, centralized configuration for grid settings.
 */
public class GridConfiguration implements Serializable {
  private final String name;
  private final Map<String, String> envMap;

  private static Logger log = LoggerFactory.getLogger(GridConfiguration.class);

  public static final String defaultBaseEnvVar = "GC_DEFAULT_BASE";
  static final String defaultBaseProperty = GridConfiguration.class.getCanonicalName() + ".DefaultBase";
  private static File defaultBase;

  public static final String defaultGCEnvVar = "GC_DEFAULT";
  static final String defaultGCProperty = GridConfiguration.class.getCanonicalName() + ".DefaultGC";
  private static String defaultGC;

  public static final String envSuffixEnvVar = "GC_ENV_SUFFIX";
  static final String envSuffixProperty = GridConfiguration.class.getCanonicalName() + ".EnvSuffix";
  public static String envSuffix;
  private static final String defaultEnvSuffix = ".env";

  static {
    staticInit();
  }

  static String getFromEnvOrProperty(String defaultEnvVar, String defaultProperty) {
    String val;

    val = PropertiesHelper.systemHelper.getString(defaultProperty, PropertiesHelper.UndefinedAction.ZeroOnUndefined);
    if (val == null) {
      val = PropertiesHelper.envHelper.getString(defaultEnvVar, PropertiesHelper.UndefinedAction.ZeroOnUndefined);
      if (val == null) {
      } else {
        log.debug("GridConfiguration using val from environment variable {}" , defaultEnvVar);
      }
    } else {
      log.debug("GridConfiguration using val from property {}" , defaultProperty);
    }
    return val;
  }

  static void staticInit() {
    String defaultBaseVal;
    String envSuffixVal;

    defaultBaseVal = getFromEnvOrProperty(defaultBaseEnvVar, defaultBaseProperty);
    if (defaultBaseVal != null && defaultBaseVal.trim().length() > 0) {
      defaultBase = new File(defaultBaseVal);
    } else {
      defaultBase = null;
    }

    defaultGC = getFromEnvOrProperty(defaultGCEnvVar, defaultGCProperty);

    envSuffixVal = PropertiesHelper.systemHelper.getString(envSuffixProperty,
        PropertiesHelper.UndefinedAction.ZeroOnUndefined);
    if (envSuffixVal == null) {
      envSuffixVal = PropertiesHelper.envHelper.getString(envSuffixEnvVar, defaultEnvSuffix);
    }
    envSuffix = envSuffixVal;
  }

  public static File getDefaultBase() {
    if (defaultBase == null) {
      log.info("defaultBase is undefined");
      log.info("Set env var {} or property {}", defaultBaseEnvVar, defaultBaseProperty);
      throw new RuntimeException("defaultBase is undefined");
    } else {
      return defaultBase;
    }
  }

  public static String getDefaultGC() {
    return defaultGC;
  }

  public GridConfiguration(String name, Map<String, String> envMap) {
    this.name = name;
    this.envMap = envMap;
  }

  public static GridConfiguration parseFile(File gcBase, String gcName) throws IOException {
    Map<String, String> envMap;

    if (gcName == null) {
      if (defaultGC != null) {
        gcName = defaultGC;
      } else {
        throw new RuntimeException("No GC defined. Missing -g ?");
      }
    }
    envMap = readEnvFile(new File(gcBase, gcName + envSuffix));
    return new GridConfiguration(gcName, envMap);
  }

  public static GridConfiguration parseFile(String gcName) throws IOException {
    return parseFile(getDefaultBase(), gcName);
  }

  public static Map<String, String> readEnvFile(File envFile) throws IOException {
    Map<String, String> envMap;
    String line;
    BufferedReader reader;

    envMap = new HashMap<String, String>();
    reader = new BufferedReader(new InputStreamReader(new FileInputStream(envFile)));
    do {
      line = reader.readLine();
      if (line != null) {
        String[] tokens;

        tokens = line.split("[\\s=]");
        if (tokens.length != 3 || !tokens[0].equals("export")) {
          log.info("Skipping bad env line: {} {}" , tokens.length ,line);
        } else {
          envMap.put(tokens[1], tokens[2]);
        }
      }
    } while (line != null);

    reader.close();
    return envMap;
  }

  public String getName() {
    return name;
  }

  public Map<String, String> getEnvMap() {
    return envMap;
  }

  public String toEnvString() {
    StringBuilder sb;

    sb = new StringBuilder();
    for (Map.Entry<String, String> entry : envMap.entrySet()) {
      sb.append("export " + entry.getKey() + "=" + entry.getValue() + "\n");
    }
    return sb.toString();
  }

  public String get(String envKey) {
    return envMap.get(envKey);
  }

  @Override
  public int hashCode() {
    return name.hashCode() ^ envMap.hashCode();
  }

  @Override
  public boolean equals(Object o) {
    GridConfiguration other = (GridConfiguration) o;
    return name.equals(other.name) && envMap.equals(other.envMap);
  }

  @Override
  public String toString() {
    StringBuilder sb;

    sb = new StringBuilder();
    sb.append(name);
    return sb.toString();
  }
}
