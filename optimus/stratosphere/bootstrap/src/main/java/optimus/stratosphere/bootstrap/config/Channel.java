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
package optimus.stratosphere.bootstrap.config;

import java.util.Collections;
import java.util.List;
import com.typesafe.config.Config;
import optimus.platform.partial.rollout.PartialRollout;

/*
Example channel format
{
  name = "
  users = []
  auto-include.percent = 0
  description = ""
  config {
    list of modified configurations...
  }
}
* */
public record Channel(
    String name, List<String> users, int autoIncludePercent, Config config, String description) {
  public static Channel create(Config config) {
    final String name = config.getString("name");
    final List<String> users =
        config.hasPath("users") ? config.getStringList("users") : Collections.emptyList();
    final int autoIncludePercent =
        config.hasPath("auto-include.percent") ? config.getInt("auto-include.percent") : 0;
    final Config conf = config.getConfig("config");
    final String description = config.hasPath("description") ? config.getString("description") : "";
    return new Channel(name, users, autoIncludePercent, conf, description);
  }

  public boolean checkUserIsAutoIncluded(String userId) {
    return PartialRollout.isEligible(userId, name, autoIncludePercent);
  }
}
