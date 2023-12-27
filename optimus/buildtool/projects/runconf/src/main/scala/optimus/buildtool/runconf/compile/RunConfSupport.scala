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

object RunConfSupport {
  object names {
    val templatesBlock: Block = "templates"
    val applicationsBlock: Block = "applications"
    val testsBlock: Block = "tests"

    val allBlocks = Set(templatesBlock, applicationsBlock, testsBlock)

    val RunConfDefaults = "RunConfDefaults"
    val ApplicationDefaults = "ApplicationDefaults"
    val TestDefaults = "TestDefaults"

    val allDefaultTemplates = Set(RunConfDefaults, ApplicationDefaults, TestDefaults)

    object scopeKey {
      val sysProps = "sys"
      val env = "env"
      val config = "config"
      val os = "os"
      val dtcEnabled = "dtcEnabled"
    }

    object os {
      val windows = "windows"
      val linux = "linux"
    }

    object treadmillOptions {
      val mem = "mem"
      val disk = "disk"
      val cpu = "cpu"
      val manifest = "manifest"
      val cell = "cell"
      val app = "app"
      val kuu = "kuu"
      val priority = "priority"
    }

    object extraExecOptions {
      val preBas = "preBas"
      val preReg = "preReg"
      val preDiff = "preDiff"
      val postBas = "postBas"
      val postReg = "postReg"
      val postDiff = "postDiff"
    }

    object crossPlatformEnvVar {
      // Make sure runconf-substitutions.obt in the ignoreList.env section contains these
      val appData = "APP_DATA"
      val defaultMsdeHome = "DEFAULT_MSDE_HOME"
      val hostName = "HOST_NAME"
      val localAppData = "LOCAL_APP_DATA"
      val tempFolder = "TEMP"
      val userHome = "USER_HOME"
      val userName = "USER_NAME"
    }

    val scope = "scope"
    // Backward-compatible version of "scope"
    val moduleName = "moduleName"
    val mainClass = "mainClass"
    val mainClassArgs = "mainClassArgs"
    val javaOpts = "javaOpts"
    val parents = "parents"
    val isTest = "isTest"
    val packageName = "package"
    val methodName = "method"
    val env = "env"
    val agents = "agents"
    val launcher = "launcher"
    val allowDTC = "allowDTC"
    val condition = "condition"
    val includes = "includes"
    val excludes = "excludes"
    val maxParallelForks = "maxParallelForks"
    val forkEvery = "forkEvery"
    val appendableEnv = "appendableEnv"
    val extractTestClasses = "extractTestClasses"
    val moduleLoads = "moduleLoads"
    val credentialGuardCompatibility = "credentialGuardCompatibility"
    val debugPreload = "debugPreload"
    val additionalScope = "additionalScope"
    val treadmillOpts = "treadmillOpts"
    val extraExecOpts = "extraExecOpts"
    val category = "category"
    val groups = "groups"
    val owner = "owner"
    val flags = "flags"
  }
}
