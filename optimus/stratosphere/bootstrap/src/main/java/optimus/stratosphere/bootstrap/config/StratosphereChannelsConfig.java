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

import static optimus.stratosphere.bootstrap.config.StratosphereConfig.CONFIG_USERNAME;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;
import com.typesafe.config.Config;
import optimus.stratosphere.bootstrap.StratosphereException;

final class Channel {
  public final String name;
  public final List<String> users;
  public final Config config;

  public Channel(Config config) {
    this.name = config.getString("name");
    if (config.hasPath("users")) {
      this.users = config.getStringList("users");
    } else {
      this.users = Collections.emptyList();
    }
    this.config = config.getConfig("config");
  }
}

public class StratosphereChannelsConfig {

  public static String configFile = "stratosphereChannels.conf";
  public static String channelsListKey = "internal.channels.list";
  public static String disableStratosphereChannels = "internal.channels.disable";
  public static String stratosphereChannelKey = "stratosphereChannel";

  public static boolean shouldUseStratosphereChannels(
      Config config, String stratosphereInfraOverride) {
    boolean disableChannels =
        config.hasPath(disableStratosphereChannels)
            && config.getBoolean(disableStratosphereChannels);
    return !disableChannels && stratosphereInfraOverride == null;
  }

  public static Channel selectChannel(Config stratosphereConfig) {
    List<Channel> channels =
        stratosphereConfig.getConfigList(channelsListKey).stream()
            .map(Channel::new)
            .collect(Collectors.toList());

    if (stratosphereConfig.hasPath(stratosphereChannelKey)) {
      String selectedChannelName = stratosphereConfig.getString(stratosphereChannelKey);
      return selectChannelByName(channels, selectedChannelName);
    } else {
      String userName = stratosphereConfig.getString(CONFIG_USERNAME);
      return selectEligibleChannelsByUsername(channels, userName);
    }
  }

  private static Channel selectChannelByName(List<Channel> channels, String selectedChannelName) {
    List<Channel> nameChannels =
        channels.stream()
            .filter(channel -> channel.name.equals(selectedChannelName))
            .collect(Collectors.toList());
    if (nameChannels.size() == 1) {
      return nameChannels.get(0);
    } else if (nameChannels.size() > 1) {
      throw new StratosphereException("Duplicated entries for channel: " + selectedChannelName);
    } else {
      throw new StratosphereException(
          "Selected channel doesn't exist: "
              + selectedChannelName
              + " To fix this issue, you need to either update your branch using Git or manually remove the configured channel in <workspace>/config/custom.conf.");
    }
  }

  private static Channel selectEligibleChannelsByUsername(List<Channel> channels, String userName) {
    List<Channel> eligibleChannels =
        channels.stream()
            .filter(config -> config.users.contains(userName))
            .collect(Collectors.toList());
    if (eligibleChannels.size() == 1) {
      return eligibleChannels.get(0);
    } else if (eligibleChannels.size() > 1) {
      throw new StratosphereException(
          String.format(
              "User is added to multiple stratosphere channels: %s. Please make sure to select only one channel\n",
              eligibleChannels.stream().map(channel -> channel.name).collect(Collectors.toList())));
    } else {
      return null;
    }
  }
}
