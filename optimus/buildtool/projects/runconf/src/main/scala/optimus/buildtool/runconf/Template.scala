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
package optimus.buildtool.runconf
import optimus.buildtool.config.ModuleId
import optimus.buildtool.config.ParentId
import optimus.buildtool.config.ScopeId
import optimus.buildtool.runconf.plugins.ExtraExecOpts
import optimus.buildtool.runconf.plugins.JavaModule
import optimus.buildtool.runconf.plugins.NativeLibraries
import optimus.buildtool.runconf.plugins.ScriptTemplates
import optimus.buildtool.runconf.plugins.SuiteConfig
import optimus.buildtool.runconf.plugins.TreadmillOpts

import scala.collection.immutable.Seq

final case class Template(
    id: ParentId,
    scopeType: Option[String],
    name: String,
    isTest: Boolean,
    env: Map[String, String],
    javaOpts: Seq[String],
    mainClassArgs: Seq[String],
    packageName: Option[String],
    mainClass: Option[String],
    methodName: Option[String],
    agents: Seq[ModuleRef],
    launcher: Launcher,
    includes: Seq[RunconfPattern],
    excludes: Seq[RunconfPattern],
    maxParallelForks: Int,
    forkEvery: Int,
    nativeLibraries: NativeLibraries,
    extractTestClasses: Boolean,
    moduleLoads: Seq[String],
    javaModule: JavaModule,
    scriptTemplates: ScriptTemplates,
    credentialGuardCompatibility: Boolean,
    debugPreload: Boolean,
    suites: Map[String, SuiteConfig],
    additionalScope: Option[String],
    treadmillOpts: Option[TreadmillOpts],
    extraExecOpts: Option[ExtraExecOpts],
    category: Option[String],
    groups: Set[String],
    owner: Option[String],
    flags: Map[String, String]
) extends HasScopedName
    with HasNativeLibraries {

  def modifyStrings(modify: String => String): Template = {
    copy(
      mainClassArgs = mainClassArgs.map(modify),
      javaOpts = javaOpts.map(modify),
      env = env.map { case (k, v) => k -> modify(v) }
    )
  }

  def asTestRunConf(scopeTypeFallback: => String): Option[TestRunConf] = id match {
    case module: ModuleId if isTest =>
      Some(
        TestRunConf(
          id = module.scope(scopeType.getOrElse(scopeTypeFallback)),
          name = name,
          env = env,
          javaOpts = javaOpts,
          packageName = packageName,
          mainClass = mainClass,
          methodName = methodName,
          agents = agents,
          launcher = launcher,
          includes = includes,
          excludes = excludes,
          maxParallelForks = maxParallelForks,
          forkEvery = forkEvery,
          nativeLibraries = nativeLibraries,
          extractTestClasses = extractTestClasses,
          javaModule = javaModule,
          credentialGuardCompatibility = credentialGuardCompatibility,
          debugPreload = debugPreload,
          suites = suites,
          additionalScope = additionalScope.filter(_.nonEmpty).map(ScopeId.parse),
          treadmillOpts = treadmillOpts,
          extraExecOpts = extraExecOpts,
          category = category,
          groups = groups,
          owner = owner,
          flags = flags
        )
      )
    case _ =>
      None
  }

  override def toString: String = {
    s"""Template(
       |  id = $id
       |  scopeType = $scopeType
       |  name = $name
       |  env = $env
       |  javaOpts = $javaOpts
       |  mainClassArgs = $mainClassArgs
       |  packageName = $packageName
       |  mainClass = $mainClass
       |  methodName = $methodName
       |  agents = $agents
       |  launcher = $launcher
       |  includes = $includes
       |  excludes = $excludes
       |  maxParallelForks = $maxParallelForks
       |  forkEvery = $forkEvery
       |  nativeLibraries = $nativeLibraries
       |  javaModule = $javaModule
       |  scriptTemplates = $scriptTemplates
       |  suites = $suites
       |  treadmillOpts = $treadmillOpts
       |  extraExecOpts = $extraExecOpts
       |  category = $category
       |  groups = $groups
       |  owner = $owner
       |  flags = $flags
       |)""".stripMargin
  }

}
