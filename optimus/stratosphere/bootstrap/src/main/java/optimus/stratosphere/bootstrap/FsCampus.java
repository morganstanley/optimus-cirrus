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

import java.io.File;
import java.nio.file.Path;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;

/**
 * Simple util reading sysloc override file for a given workspace and returns the expected location.
 */
public class FsCampus {

  public final String defaultOverride;

  private final Config mappingConfig;

  private final String envVarName;
  private final Pattern locationPattern;

  public FsCampus(Path configDir) {
    File configFile = configDir.resolve("locationMapping.conf").toFile();
    this.mappingConfig = configFile.exists() ? ConfigFactory.parseFile(configFile) : null;
    this.envVarName =
        mappingConfig != null ? mappingConfig.getString("system-variable-name") : null;
    // location pattern from config
    this.locationPattern =
        mappingConfig != null ? Pattern.compile(mappingConfig.getString("location-pattern")) : null;
    this.defaultOverride =
        mappingConfig != null ? mappingConfig.getString("default-override") : "ln";
  }

  public String getValue() {
    if (mappingConfig == null) return defaultOverride; // no mappings

    String currentSystemLocation = getCurrentSystemLocation();
    if (currentSystemLocation == null) return defaultOverride; // missing

    String region = getRegion(currentSystemLocation);
    if (region == null) return defaultOverride; // malformed

    // if we've defined an explicit location->region mapping, use it
    String mappedRegion =
        mappingConfig.hasPath(currentSystemLocation)
            ? mappingConfig.getString(currentSystemLocation)
            : null;
    if (mappedRegion != null) return mappedRegion;

    // otherwise check the region->region overrides before returning
    String regionMapPath = "region-mappings." + region;
    return mappingConfig.hasPath(regionMapPath)
        ? mappingConfig.getString(regionMapPath)
        : defaultOverride;
  }

  public String getAlternativeMapping(String alternateName) {
    if (mappingConfig == null) return defaultOverride; // no mappings
    String configPath = "region-mappings." + alternateName + "." + getValue();
    return mappingConfig.hasPath(configPath) ? mappingConfig.getString(configPath) : getValue();
  }

  /** May return null! */
  protected String getCurrentSystemLocation() {
    return System.getenv(envVarName);
  }

  protected String getRegion(String systemLocation) {
    // use regex in locationpattern to extract value from systemLocation
    Matcher matcher = locationPattern.matcher(systemLocation);

    return matcher.matches() ? matcher.group(1) : null;
  }
}
