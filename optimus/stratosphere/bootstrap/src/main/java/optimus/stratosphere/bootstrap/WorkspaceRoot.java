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

import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;

public class WorkspaceRoot {
  public static final String STRATOSPHERE_CONFIG_FILE = "stratosphere.conf";

  public static Path find() {
    return find(STRATOSPHERE_CONFIG_FILE, getCurrentDir());
  }

  public static Path findIn(Path path) {
    return find(STRATOSPHERE_CONFIG_FILE, path);
  }

  public static Path findOldOrNew() {
    return find(STRATOSPHERE_CONFIG_FILE, getCurrentDir());
  }

  private static Path getCurrentDir() {
    return Paths.get("").toAbsolutePath();
  }

  private static Path find(String fileName, Path currentDir) {
    while (currentDir != null) {
      Path srcDir = currentDir.resolve("src");
      Path versionDescriptor = srcDir.resolve(fileName);

      if (Files.exists(versionDescriptor)) {
        return currentDir;
      } else {
        currentDir = currentDir.getParent();
      }
    }

    return null;
  }
}
