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
import java.util.ArrayList;
import java.util.Collection;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import com.typesafe.config.ConfigResolveOptions;

public class StratosphereChannelsConfig {

  public static final String configFile = "stratosphereChannels.conf";
  public static final String keyPrefix = "internal.channels";
  public static final String channelsListKey = keyPrefix + ".list";
  public static final String ignoredChannelsKey = keyPrefix + ".ignored.list";
  public static final String userSelectedChannelsKey = keyPrefix + ".selected.list";
  public static final String useAutoIncludeFlagKey = keyPrefix + ".auto-include.enabled";
  public static final String optOutUsersKey = keyPrefix + ".auto-include.opt-out-users";
  public static final String disableStratosphereChannels = keyPrefix + ".disable";
  public static final String legacyStratosphereChannelKey = "stratosphereChannel";

  //  synthetic config entries
  public static final String usedChannelsKey = keyPrefix + ".used";
  public static final String reportedChannelCollisions = keyPrefix + ".collisions";

  public static boolean shouldUseStratosphereChannels(Config config) {
    boolean disableChannels =
        config.hasPath(disableStratosphereChannels)
            && config.getBoolean(disableStratosphereChannels);
    return !disableChannels;
  }

  public static List<Channel> selectAllChannels(Config stratosphereConfig) {
    List<Channel> allChannels =
        stratosphereConfig.getConfigList(channelsListKey).stream()
            .map(Channel::create)
            .collect(Collectors.toList());

    List<String> ignoredChannelNames = stratosphereConfig.getStringList(ignoredChannelsKey);
    String userName = stratosphereConfig.getString(CONFIG_USERNAME);
    boolean shouldUseAutoInclude = stratosphereConfig.getBoolean(useAutoIncludeFlagKey);
    boolean userIsOptOutFromAutoIncludes =
        stratosphereConfig.getStringList(optOutUsersKey).contains(userName);

    List<String> userSelected = stratosphereConfig.getStringList(userSelectedChannelsKey);

    if (stratosphereConfig.hasPath(legacyStratosphereChannelKey)) {
      String selectedChannelName = stratosphereConfig.getString(legacyStratosphereChannelKey);
      userSelected.add(selectedChannelName);
    }
    List<Channel> selectedByUser = channelsSelectedByUser(allChannels, userSelected);
    List<Channel> inUsersList = selectEligibleChannelsByUsernameList(allChannels, userName);
    List<Channel> autoIncluded = new ArrayList<>();
    if (shouldUseAutoInclude && !userIsOptOutFromAutoIncludes) {
      autoIncluded = selectAutoIncludedChannels(allChannels, userName);
    }

    return Stream.of(selectedByUser, inUsersList, autoIncluded)
        .flatMap(Collection::stream)
        .filter(channel -> !ignoredChannelNames.contains(channel.name()))
        .distinct()
        .sorted(Comparator.comparing(allChannels::indexOf))
        .collect(Collectors.toList());
  }

  private static final ConfigResolveOptions allowUnresolved =
      ConfigResolveOptions.defaults().setAllowUnresolved(true);

  public static Map<String, List<String>> configCollisions(List<Channel> channels) {
    Config merged =
        channels.stream()
            .reduce(
                ConfigFactory.empty(),
                (config, channel) -> channel.config().withFallback(config),
                Config::withFallback);

    return merged.entrySet().stream()
        .map(
            entry ->
                Map.entry(
                    entry.getKey(),
                    channels.stream()
                        .filter(channel -> channel.config().hasPath(entry.getKey()))
                        .map(Channel::name)
                        .collect(Collectors.toList())))
        .filter(entry -> entry.getValue().size() > 1)
        .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
  }

  private static List<Channel> selectEligibleChannelsByUsernameList(
      List<Channel> channels, String userName) {
    return channels.stream().filter(channel -> channel.users().contains(userName)).toList();
  }

  private static List<Channel> channelsSelectedByUser(
      List<Channel> channels, List<String> selectedList) {
    return channels.stream().filter(channel -> selectedList.contains(channel.name())).toList();
  }

  private static List<Channel> selectAutoIncludedChannels(List<Channel> channels, String userName) {
    return channels.stream().filter(config -> config.checkUserIsAutoIncluded(userName)).toList();
  }
}
