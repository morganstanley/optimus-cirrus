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
package optimus.buildtool.runconf.compile.plugins

import optimus.buildtool.runconf.compile.RunConfSupport.names
import optimus.buildtool.runconf.TestRunConf
import optimus.buildtool.runconf.compile.Messages
import optimus.buildtool.runconf.compile.ResolvedRunConfCompilingState
import optimus.buildtool.runconf.compile.RunConfCompilingState
import com.typesafe.config.Config

class DTCSupport(enableDTC: Boolean, stratoConfig: Config) {

  private lazy val dtcJavaOpts = Seq(
    systemProperty("optimus.monitor.classUsage", "true"),
    systemProperty("stratosphere.home", stratoConfig.getString("stratosphereHome")),
    systemProperty("stratosphere.workspace", stratoConfig.getString("stratosphereWsDir"))
  )

  def validate(conf: RunConfCompilingState): Unit = {
    if (conf.runConfWithoutParents.allowDTC.isDefined && conf.runConf.isApp) {
      conf.reportError.atKey(names.allowDTC, Messages.dtcNotForApps)
    }
  }

  def apply(conf: ResolvedRunConfCompilingState): Unit = {
    val runConf = conf.runConf
    if (enableDTC && runConf.allowDTC.contains(true)) {

      conf.transformTemplate { template =>
        template.copy(javaOpts = template.javaOpts ++ dtcJavaOpts)
      }

      conf.transformRunConf { case test: TestRunConf =>
        test.copy(javaOpts = test.javaOpts ++ dtcJavaOpts)
      }
    }
  }

  private def systemProperty(name: String, value: String): String = {
    s"-D$name=$value"
  }

}
