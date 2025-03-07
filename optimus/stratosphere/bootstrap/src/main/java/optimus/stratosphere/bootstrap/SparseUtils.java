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

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Optional;
import java.util.stream.Stream;

public abstract class SparseUtils {

  private static final String ROOT_DIR_PATTERN = "/*";
  private static final String SPARSE_CONFIG_PATH = ".git/info/sparse-checkout";
  private static final List<String> REQUIRED_DIRS = List.of("profiles");
  private static final String BROKEN_SPARSE_MESSAGE =
      "Detected malformed sparse configuration. Run following commands to recover:\n\n"
          + "git sparse-checkout disable\n"
          + "stratosphere sparse refresh";

  public static void assertValidSparseConfig(Path srcDir) {
    if (isConfigCorrupted(srcDir))
      throw new RecoverableStratosphereException(BROKEN_SPARSE_MESSAGE);
  }

  private static boolean isConfigCorrupted(Path srcDir) {
    Optional<Stream<String>> config = getSparseConfig(srcDir);
    return config.isPresent() && config.get().noneMatch(ROOT_DIR_PATTERN::equals);
  }

  private static Optional<Stream<String>> getSparseConfig(Path relativePath) {
    return Optional.of(relativePath.resolve(SPARSE_CONFIG_PATH))
        .filter(Files::exists)
        .map(SparseUtils::getLines);
  }

  private static Stream<String> getLines(Path path) {
    try {
      return Files.lines(path);
    } catch (IOException exception) {
      throw new UncheckedIOException(exception);
    }
  }

  public static void assertRequiredDirectoriesExist(Path srcDir) {
    boolean requiredDirsExist = REQUIRED_DIRS.stream().map(srcDir::resolve).allMatch(Files::exists);
    if (!requiredDirsExist) throw new RecoverableStratosphereException(BROKEN_SPARSE_MESSAGE);
  }
}
