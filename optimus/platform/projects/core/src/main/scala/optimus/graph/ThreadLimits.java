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
package optimus.graph;

import java.io.IOException;
import java.nio.file.FileSystem;
import java.nio.file.FileSystems;
import java.nio.file.Files;
import java.nio.file.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

// extracted as a class to make it testable
class ThreadLimits {
  private static final Logger log = LoggerFactory.getLogger(ThreadLimits.class);

  // since Treadmill allows burst capacity, we may want to use more threads than the stated limit
  private final double treadmillCpuFactor;
  private final FileSystem fileSystem;
  private int treadmillCpuLimit = Integer.MIN_VALUE;

  ThreadLimits(double treadmillCpuFactor) {
    this(treadmillCpuFactor, FileSystems.getDefault());
  }

  // Constructor that allows injecting a FileSystem for testing
  ThreadLimits(double treadmillCpuFactor, FileSystem fs) {
    this.treadmillCpuFactor = treadmillCpuFactor;
    this.fileSystem = fs;
  }

  protected int readTreadmillCpuLimit() {
    Path path = fileSystem.getPath("/env/TREADMILL_CPU");
    if (Files.isReadable(path)) {
      try {
        String str = Files.readString(path);
        log.info("Read treadmill CPU limit '" + str + "' from " + path);
        try {
          int limit = (int) Math.ceil(Integer.parseInt(str) * treadmillCpuFactor / 100.0);
          log.info(
              "Interpreted treadmill CPU limit as "
                  + limit
                  + " CPUs (used scaling factor "
                  + treadmillCpuFactor
                  + ")");
          return limit;
        } catch (NumberFormatException e) {
          log.warn("Unable to parse treadmill CPU limit");
          return Integer.MAX_VALUE;
        }
      } catch (IOException e) {
        log.warn("Unable to read treadmill CPU limit file " + path);
        return Integer.MAX_VALUE;
      }
    }
    log.info(
        "Treadmill CPU limit file " + path + " is not present. Assuming we're not on Treadmill.");
    return Integer.MAX_VALUE;
  }

  synchronized int getTreadmillCpuLimit() {
    if (treadmillCpuLimit == Integer.MIN_VALUE) treadmillCpuLimit = readTreadmillCpuLimit();
    return treadmillCpuLimit;
  }

  int actualAvailableProcessors() {
    return Math.min(runtimeAvailableProcessors(), getTreadmillCpuLimit());
  }

  int runtimeAvailableProcessors() {
    return Runtime.getRuntime().availableProcessors();
  }

  int idealThreadCountToCount(int count, int minimumIfNegative, int maximumIfNegative) {
    return (count < 0)
        ? Math.min(
            maximumIfNegative, Math.max(minimumIfNegative, actualAvailableProcessors() / -count))
        : count;
  }

  int idealThreadCountToCount(int count) {
    return idealThreadCountToCount(count, 4, Integer.MAX_VALUE);
  }
}
