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
package optimus.buildtool.runconf.compile

import optimus.buildtool.config.ParentId
import optimus.buildtool.runconf.HasScopedName
import optimus.buildtool.runconf.Launcher
import optimus.buildtool.runconf.plugins.EnvInternal
import optimus.buildtool.runconf.plugins.ExtraExecOpts
import optimus.buildtool.runconf.plugins.JavaModule
import optimus.buildtool.runconf.plugins.NativeLibraries
import optimus.buildtool.runconf.plugins.ScriptTemplates
import optimus.buildtool.runconf.plugins.SuiteConfig
import optimus.buildtool.runconf.plugins.TreadmillOpts

import scala.collection.immutable.Seq

final case class UnresolvedRunConf(
    id: ParentId,
    name: String,
    scopeType: Option[String],
    condition: Option[String],
    mainClass: Option[String],
    mainClassArgs: Seq[String],
    javaOpts: Seq[String],
    env: EnvInternal,
    agents: Seq[String],
    isTemplate: Boolean,
    isTest: Boolean,
    parents: Seq[String],
    packageName: Option[String],
    methodName: Option[String],
    launcher: Option[Launcher],
    allowDTC: Option[Boolean],
    includes: Seq[String],
    excludes: Seq[String],
    maxParallelForks: Option[Int],
    forkEvery: Option[Int],
    suites: Map[String, SuiteConfig],
    nativeLibraries: NativeLibraries,
    scriptTemplates: ScriptTemplates,
    extractTestClasses: Option[Boolean],
    appendableEnv: EnvInternal,
    moduleLoads: Seq[String],
    javaModule: JavaModule,
    credentialGuardCompatibility: Option[Boolean],
    debugPreload: Option[Boolean],
    additionalScope: Option[String],
    treadmillOpts: Option[TreadmillOpts],
    extraExecOpts: Option[ExtraExecOpts],
    category: Option[String],
    groups: Set[String],
    owner: Option[String],
    flags: Map[String, String]
) extends HasScopedName {
  def isApp: Boolean = !isTemplate && !isTest
}
