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
package optimus.buildtool.builders.postbuilders.installer
package component

import java.nio.file.Files
import java.nio.file.Paths
import java.nio.file.StandardOpenOption
import optimus.buildtool.config.GenericRunnerConfiguration
import optimus.buildtool.config.MetaBundle
import optimus.buildtool.config.StaticConfig
import optimus.buildtool.files.Directory
import optimus.buildtool.files.FileAsset
import optimus.buildtool.files.InstallPathBuilder
import optimus.buildtool.utils.AssetUtils
import optimus.buildtool.utils.Hashing
import optimus.buildtool.utils.Utils
import optimus.platform._

import scala.collection.immutable
import scala.util.Properties.{ isWin => isWindows }

class GenericRunnerInstaller(
    fingerprints: BundleFingerprintsCache,
    install: InstallPathBuilder,
    config: GenericRunnerConfiguration
) extends ComponentBatchInstaller
    with util.Log {
  import GenericRunnerInstaller._

  final val descriptor = "generic app runner"

  @async override def install(installable: BatchInstallableArtifacts): immutable.Seq[FileAsset] = {
    installable.metaBundles.toList.sorted.apar.flatMap(installOne)
  }

  @async def installOne(mp: MetaBundle): Seq[FileAsset] = {
    val generator = Generator(mp)
    fingerprints.bundleFingerprints(mp).writeIfKeyChanged(FingerprintKey, generator.fingerprint) {
      val target = install.binDir(mp)
      Files.createDirectories(target.path)
      generator.files.map { case (fn, content) =>
        val destination = target.resolveFile(fn)
        AssetUtils.atomicallyWrite(destination, replaceIfExists = true, localTemp = true) { file =>
          Files.writeString(file, content, StandardOpenOption.CREATE_NEW)
          if (!isWindows)
            Files.setPosixFilePermissions(file, Utils.ExecuteBits)
        }
        destination
      }
    }
  }

  @entity class Generator(mp: MetaBundle) {
    @node def fingerprint: String = Hashing.hashStrings(List(linuxContent, windowsContent))

    @node def files: Seq[(String, String)] = // not RelativePath because we can't use them against JimFS paths
      List(
        ScriptName -> linuxContent,
        ScriptName + ".bat" -> windowsContent
      )

    @node def linuxContent: String = {
      val runnerBin = genericRunnerAppDir.resolveFile(RunnerName).pathString
      // we need to find a valid `dirname` somehow.
      // "whence" is a ksh builtin, so this should work irrespective of PATH (as long as your system is at least kinda standard).
      // OAR_COMMON_DIR is understood by the launcher script (see runner.runconf).
      // We don't have ksh on cloud but I'll cross that bridge when I come to it.
      s"""#!${StaticConfig.string("ksh")}
         |DIRNAME_BIN=$$(whence dirname)
         |if [[ ! -x $$DIRNAME_BIN ]]; then DIRNAME_BIN=/usr/bin/dirname; fi
         |if [[ ! -x $$DIRNAME_BIN ]]; then DIRNAME_BIN=/bin/dirname;     fi
         |OAR_COMMON_DIR="$$($$DIRNAME_BIN $$0)/../" exec $runnerBin ${mp.properPath} "$$@"
         |""".stripMargin.trim
    }

    @node def windowsContent: String = {
      val runnerBin = genericRunnerAppDir.resolveFile(s"$RunnerName.bat").pathString
      // interpolation done here to avoid issues with poor syntax highlighting in IDEA
      // see https://youtrack.jetbrains.com/issue/SCL-20422
      val oarCommon = """set OAR_COMMON_DIR=%OAR_PRE_DIRNAME%..\"""
      // as above: this %~dp0 nonsense is 100% cribbed from the internet and the other start scripts (we don't have dirname)
      raw"""for %%F in ("%~dp0") do set OAR_PRE_DIRNAME=%%~dpF
           |$oarCommon
           |$runnerBin ${mp.properPath} %*
           |""".stripMargin.trim
    }

    @node private def genericRunnerAppDir: Directory =
      config.genericRunnerAppDirOverride
        .orElse { sys.env.get("APP_DIR").map(p => Directory(Paths.get(p))) }
        .getOrElse {
          val alternative = install.binDir(mp)
          log.warn(s"No OptimusAppRunner binary path: runner will point to ${alternative.pathString}")
          alternative
        }
  }
}

object GenericRunnerInstaller {
  final val ScriptName = "run"
  final val RunnerName = ".app-runner"
  final val FingerprintKey = "GenericRunnerScript" // our content is our fingerprint
}
