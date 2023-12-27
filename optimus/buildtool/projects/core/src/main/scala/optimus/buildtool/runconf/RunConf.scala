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

import optimus.buildtool.config.ScopeId
import optimus.buildtool.runconf.plugins.ExtraExecOpts
import optimus.buildtool.utils.CrossPlatformSupport._
import optimus.buildtool.runconf.plugins.JavaModule
import optimus.buildtool.runconf.plugins.NativeLibraries
import optimus.buildtool.runconf.plugins.ScriptTemplates
import optimus.buildtool.runconf.plugins.SuiteConfig
import optimus.buildtool.runconf.plugins.TreadmillOpts

import scala.collection.immutable.Seq

// For apps, name will be the app name (eg. "HelloWorldUIApp")
// For tests, name will be the test group from the .runconf (eg. "uiTest")
final case class RunConfId(scope: ScopeId, name: String) {
  def moduleScoped: ModuleScopedName = ModuleScopedName(scope.fullModule, name)
  def properPath: String = s"${scope.properPath}.$name"
  override def toString: String = properPath
}

trait HasNativeLibraries {
  def nativeLibraries: NativeLibraries
}

sealed trait RunConf extends HasNativeLibraries {
  def id: ScopeId
  def name: String
  def javaOpts: Seq[String]
  def env: Map[String, String]
  def agents: Seq[ModuleRef]
  def launcher: Launcher
  def modifyStrings(modify: String => String): RunConf
  def javaModule: JavaModule
  def credentialGuardCompatibility: Boolean
  def debugPreload: Boolean
  def additionalScope: Option[ScopeId]

  def tpe: RunConfType // this can be made more "type safe" with lots of magic but I trust that we can keep it straight

  def runConfId: RunConfId = RunConfId(id, name)

  // For application scripts where the evaluation is delayed at runtime
  def javaOptsForWindows: Seq[String] = javaOpts.map(convertToWindowsVariables)
  def envForWindows: Map[String, String] = env.map(convertToWindowsVariables)
  def javaOptsForLinux: Seq[String] = javaOpts.map(convertToLinuxVariables)
  def envForLinux: Map[String, String] = env.map(convertToLinuxVariables)

  // For runtime evaluation (e.g. JetFire, OptimusAppRunner, OptimusTestRunner)
  def evalJavaOpts(envVars: Map[String, String]): Seq[String] = javaOpts.map(evaluateVar(_, envVars))
  def evalEnv(envVars: Map[String, String]): Map[String, String] = env.map(evaluateKeyVar(_, envVars))
}

sealed abstract class RunConfType(name: String) {
  type RC <: RunConf
  implicit val tt: reflect.ClassTag[RC]
  override final def toString: String = name
}
object RunConfType {
  def unapply(name: String): Option[RunConfType] = PartialFunction.condOpt(name) {
    case "app"  => AppRunConf
    case "test" => TestRunConf
  }
}

object AppRunConf extends RunConfType("app") {
  override implicit val tt: reflect.ClassTag[RC] = reflect.ClassTag(classOf[RC])
  type RC = AppRunConf
}
final case class AppRunConf(
    id: ScopeId,
    name: String, // app name
    mainClass: String,
    mainClassArgs: Seq[String],
    javaOpts: Seq[String],
    env: Map[String, String],
    agents: Seq[ModuleRef],
    launcher: Launcher,
    nativeLibraries: NativeLibraries,
    moduleLoads: Seq[String],
    javaModule: JavaModule,
    scriptTemplates: ScriptTemplates,
    credentialGuardCompatibility: Boolean,
    debugPreload: Boolean,
    additionalScope: Option[ScopeId]
) extends RunConf {
  def modifyStrings(modify: String => String): AppRunConf = {
    copy(
      mainClassArgs = mainClassArgs.map(modify),
      javaOpts = javaOpts.map(modify),
      env = env.map { case (k, v) => k -> modify(v) }
    )
  }

  // For application scripts where the evaluation is delayed at runtime
  def mainClassArgsForWindows: Seq[String] = mainClassArgs.map(convertToWindowsVariables)
  def mainClassArgsForLinux: Seq[String] = mainClassArgs.map(convertToLinuxVariables)

  // For runtime evaluation (e.g. JetFire, OptimusAppRunner, OptimusTestRunner)
  def evalMainClassArgs(envVars: Map[String, String]): Seq[String] = mainClassArgs.map(evaluateVar(_, envVars))

  override def tpe: RunConfType = AppRunConf

  override def toString: String = {
    s"""AppRunConf(
       |  id = $id
       |  name = $name
       |  mainClass = $mainClass
       |  mainClassArgs = $mainClassArgs
       |  javaOpts = $javaOpts
       |  env = $env
       |  agents = $agents
       |  launcher = $launcher
       |  nativeLibraries = $nativeLibraries
       |  javaModule = $javaModule
       |  scriptTemplates = $scriptTemplates
       |  credentialGuardCompatibility = $credentialGuardCompatibility
       |  debugPreload = $debugPreload
       |  additionalScope = $additionalScope
       |)""".stripMargin
  }

}

object TestRunConf extends RunConfType("test") {
  override implicit val tt: reflect.ClassTag[RC] = reflect.ClassTag(classOf[RC])
  type RC = TestRunConf
}
final case class TestRunConf(
    id: ScopeId,
    name: String, // block name in runconf - typically matches id.tpe but may differ (eg. "testJava8")
    env: Map[String, String],
    javaOpts: Seq[String],
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
    javaModule: JavaModule,
    credentialGuardCompatibility: Boolean,
    debugPreload: Boolean,
    suites: Map[String, SuiteConfig],
    additionalScope: Option[ScopeId],
    treadmillOpts: Option[TreadmillOpts],
    extraExecOpts: Option[ExtraExecOpts],
    category: Option[String],
    groups: Set[String],
    owner: Option[String],
    flags: Map[String, String]
) extends RunConf {

  def modifyStrings(modify: String => String): TestRunConf = {
    copy(
      javaOpts = javaOpts.map(modify),
      env = env.map { case (k, v) => k -> modify(v) }
    )
  }

  def isForModule: Boolean = packageName.isEmpty && mainClass.isEmpty && methodName.isEmpty

  override def tpe: RunConfType = TestRunConf

  override def toString: String = {
    s"""TestRunConf(
       |  id = $id
       |  name = $name
       |  env = $env
       |  javaOpts = $javaOpts
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
       |  extractTestClasses = $extractTestClasses
       |  javaModule = $javaModule
       |  credentialGuardCompatibility = $credentialGuardCompatibility
       |  debugPreload = $debugPreload
       |  suites = $suites
       |  treadmillOpts: $treadmillOpts,
       |  extraExecOpts: $extraExecOpts
       |  category = $category
       |  owner = $owner
       |  flags = $flags
       |)""".stripMargin
  }
}
