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

import java.io.InputStreamReader;
import java.nio.file.Path;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;

/* Read log messages from config, falling back to a reasonable default.
 */
public class Messages {
  private static String baseMessage = "[ERROR] Starting stratosphere failed.";
  private static String existenceMessagePrefix =
      "Version of stratosphere used in this workspace no longer exists at: ";
  private static String existenceMessageSuffix = ".";

  static {
    try (var confReader =
        new InputStreamReader(
            Messages.class
                .getClassLoader()
                .getResourceAsStream("internal/optimus/stratosphere/bootstrap/messages.conf"))) {
      Config config = ConfigFactory.parseReader(confReader);
      baseMessage = config.getString("optimus.stratosphere.bootstrap.messages.baseMessage");
      existenceMessagePrefix =
          config.getString("optimus.stratosphere.bootstrap.messages.existenceMessagePrefix");
      existenceMessageSuffix =
          config.getString("optimus.stratosphere.bootstrap.messages.existenceMessageSuffix");

    } catch (Throwable t) {
      // falls back to defaults
    }
  }

  public static String baseFailureMessage() {
    return baseMessage;
  }

  public static String noLongerExistsMessage(Path infraPath) {
    return existenceMessagePrefix + infraPath + existenceMessageSuffix;
  }
}
