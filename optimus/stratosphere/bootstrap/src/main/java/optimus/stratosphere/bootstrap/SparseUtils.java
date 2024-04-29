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
import java.util.Optional;
import java.util.stream.Stream;

public abstract class SparseUtils {

  private static final String ROOT_DIR_PATTERN = "/*";
  private static final String SPARSE_CONFIG_PATH = ".git/info/sparse-checkout";

  public static boolean isConfigCorrupted(Path relativePath) {
    Optional<Stream<String>> config = getSparseConfig(relativePath);
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
}
