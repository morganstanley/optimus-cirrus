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
import java.util.List;
import com.typesafe.config.Config;

public class StratosphereEarlyAdoptersConfig {
  public static String showQaBannerProperty = "internal.showQaBanner";
  public static String disableQaVersionProperty = "internal.disableQaVersion";
  public static String earlyAdoptersGroupProperty = "internal.early-adopters";
  public static String qaVersionProperty = "stratosphereQaVersion";
  public static String configFile = "earlyAdopters.conf";

  public static String qaBannerMessage =
      "You are running a QA version of stratosphere infra according to rollout plan specified in 'profiles/earlyAdopters.conf'.\n"
          + "If you wish to override this setting please run:\n"
          + "\tstratosphere config "
          + disableQaVersionProperty
          + " true\n"
          + "If you wish to disable this banner please run:\n"
          + "\tstratosphere config "
          + showQaBannerProperty
          + " false";

  public static boolean eligibleForQA(Config config, String stratosphereInfraOverride) {
    String userName = config.getString(CONFIG_USERNAME);
    List<String> earlyAdoptersGroup = config.getStringList(earlyAdoptersGroupProperty);

    boolean disableQaVersion =
        config.hasPath(disableQaVersionProperty) && config.getBoolean(disableQaVersionProperty);
    boolean stratoQaVersionDefined =
        config.hasPath(StratosphereEarlyAdoptersConfig.qaVersionProperty);
    return earlyAdoptersGroup.contains(userName)
        && !disableQaVersion
        && stratoQaVersionDefined
        && stratosphereInfraOverride == null;
  }

  public static boolean shouldShowEarlyAdoptersBanner(Config config) {
    return config.hasPath(showQaBannerProperty) && config.getBoolean(showQaBannerProperty);
  }
}
