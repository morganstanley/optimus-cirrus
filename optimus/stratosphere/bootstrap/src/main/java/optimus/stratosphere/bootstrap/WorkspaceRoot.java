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
  private static final String SRC_DIR = "src";

  public static Path find() {
    return find(Paths.get("").toAbsolutePath());
  }

  public static Path find(Path currentDir) {
    if (currentDir == null) {
      return null;
    }
    Path srcDir = currentDir.resolve(SRC_DIR);
    if (Files.exists(srcDir)) {
      if (Files.exists(srcDir.resolve(STRATOSPHERE_CONFIG_FILE))) {
        SparseUtils.assertRequiredDirectoriesExist(srcDir);
        return currentDir;
      } else {
        SparseUtils.assertValidSparseConfig(srcDir);
      }
    }
    return find(currentDir.getParent());
  }
}
