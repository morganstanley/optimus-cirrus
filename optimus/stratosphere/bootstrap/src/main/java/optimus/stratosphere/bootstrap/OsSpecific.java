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

import java.nio.file.Path;
import java.nio.file.Paths;

public class OsSpecific {

  public static final String osName = System.getProperty("os.name");

  public static final boolean isWindows = osName.toLowerCase().startsWith("windows");

  public static final boolean isLinux = !isWindows;

  public static final String shellExt = isWindows ? ".cmd" : "";

  public static final String shellExt16 = isWindows ? ".bat" : "";

  public static final String scriptExt = isWindows ? "cmd" : "sh";

  public static final String envSetter = isWindows ? "set " : "export ";

  public static final String platformString = isWindows ? "windows" : "linux";

  public static final String[] fileHandleLimit(Integer n, String execV2Path) {
    return isWindows ? new String[0] : new String[] {execV2Path, "-l", "N=" + n};
  }

  public static String setEnvString(String envName, String value) {
    return isWindows
        ? "set " + envName + "=" + value
        : value.isEmpty() ? "unset " + envName : "export " + envName + "=" + value;
  }

  public static Path stratoUserHome() {
    return userHome().resolve(".stratosphere");
  }

  public static String userHomeEnvName() {
    return isWindows ? "USERPROFILE" : "HOME";
  }

  public static Path userHome() {
    String userTestDir = System.getProperty("stratosphere.userDir");
    return Paths.get(userTestDir != null ? userTestDir : System.getenv(userHomeEnvName()));
  }

  public static boolean isCi() {
    return System.getenv("TRAIN_CI") != null;
  }
}
