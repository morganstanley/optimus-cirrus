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
package optimus.stratosphere.bootstrap;

import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Properties;

/**
 * Simple util reading sysloc override file for a given workspace and returns the expected location.
 */
public class FsCampus {

  public static final String defaultOverride = "ln";

  private final Path dirWithSyslocMapping;

  public FsCampus(Path dirWithSyslocMapping) {
    this.dirWithSyslocMapping = dirWithSyslocMapping;
  }

  public String getValue() {
    String currentSysloc = getCurrentSysloc();

    if (currentSysloc == null) {
      return defaultOverride;
    }

    String overriddenCampus = getSyslocMappings().getProperty(currentSysloc);

    if (overriddenCampus != null) {
      return overriddenCampus;
    } else {
      // Take 'na' out of 'ds.ab.na' to give precedence to continent as substitute to a better way
      // to pick
      // geographic network affinity to campuses
      String continent = currentSysloc.substring(currentSysloc.lastIndexOf('.') + 1);
      switch (continent) {
        case "af":
          return "ln";
        case "as":
          return "hk";
        case "au":
          return "hk";
        case "eu":
          return "ln";
        case "na":
          return "ny";
        case "sa":
          return "ny";
        default:
          return defaultOverride;
      }
    }
  }

  public String getValueNoZn() {
    String sysloc = getValue();
    return "zn".equals(sysloc) ? "hk" : sysloc;
  }

  /** May return null! */
  protected String getCurrentSysloc() {
    return System.getenv("SYS_LOC");
  }

  private Properties getSyslocMappings() {
    Properties syslocMappings = new Properties();
    Path syslocFile = dirWithSyslocMapping.resolve(".syslocOverrides");
    try (InputStream is = Files.newInputStream(syslocFile)) {
      syslocMappings.load(is);
    } catch (Exception e) {
      // ignore - the default value will be used
    }
    return syslocMappings;
  }
}
