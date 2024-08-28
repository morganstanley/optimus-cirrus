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
package optimus.graph.tracking;

import optimus.graph.DiagnosticSettings;
import org.slf4j.LoggerFactory;
import org.slf4j.Logger;

/** holder of immutable setting set via -D options on the command line */
public class DependencyTrackerSettings {
  private static final String prefix = "optimus.graph.tracking.";
  private static final Logger log = LoggerFactory.getLogger(DependencyTrackerSettings.class);

  public static final int PTRACK_DEFAULT_RESCAN =
      getIntProperty("ptrack.defaultRescan", 2, 0, Integer.MAX_VALUE);

  private static int getIntProperty(String name, int defaultValue, int min, int max) {
    int value = DiagnosticSettings.getIntProperty(prefix + name, defaultValue);
    if (defaultValue < min || defaultValue > max) {
      throw new IllegalStateException(
          "Illegal default setting for "
              + (prefix + name)
              + " of "
              + defaultValue
              + ". Range is "
              + min
              + " .. "
              + max);
    }
    if (value < min || value > max) {
      throw new IllegalStateException(
          "Illegal setting for "
              + (prefix + name)
              + " of "
              + value
              + ". Range is "
              + min
              + " .. "
              + max
              + ". Default is "
              + defaultValue);
    }
    log.info("Settings - " + prefix + name + " = " + value);
    return value;
  }

  private static boolean getBoolProperty(String name, boolean defaultValue) {
    boolean value = DiagnosticSettings.getBoolProperty(prefix + name, defaultValue);
    log.info("Settings - " + prefix + name + " = " + value);
    return value;
  }
}
