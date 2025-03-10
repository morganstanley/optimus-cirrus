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

import optimus.buildtool.config.GenericRunnerConfiguration
import optimus.buildtool.config.MetaBundle
import optimus.buildtool.config.NamingConventions
import optimus.buildtool.config.StaticConfig
import optimus.buildtool.files.Directory
import optimus.buildtool.files.FileAsset
import optimus.buildtool.files.InstallPathBuilder
import optimus.buildtool.utils.AssetUtils
import optimus.buildtool.utils.Hashing
import optimus.buildtool.utils.Utils
import optimus.platform._

import java.nio.file.Files
import java.nio.file.Path
import java.nio.file.Paths
import java.nio.file.StandardOpenOption
import scala.collection.immutable.Seq
import scala.util.Properties.{isWin => isWindows}
import scala.util.Try

class GenericRunnerInstaller(
    fingerprints: BundleFingerprintsCache,
    install: InstallPathBuilder,
    config: GenericRunnerConfiguration
) extends ComponentBatchInstaller
    with util.Log {
  import GenericRunnerInstaller._

  final val descriptor = "generic app runner"

  @async override def install(installable: BatchInstallableArtifacts): Seq[FileAsset] = {
    installable.metaBundles.toList.sorted.apar.flatMap(installOne)
  }

  @async def installOne(mp: MetaBundle): Seq[FileAsset] = {
    val generator = InstallerGenericRunnerGenerator(mp, install, config)
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
}

@entity class InstallerGenericRunnerGenerator(
    mp: MetaBundle,
    install: InstallPathBuilder,
    config: GenericRunnerConfiguration
) extends BaseGenericRunnerGenerator(install, config) {
  import GenericRunnerInstaller._

  @node def fingerprint: String = Hashing.hashStrings(List(linuxContent, windowsContent))

  // NOTE: This installer is only concerned for the generic runner script for Windows and Linux.
  //       It does not handle Docker containers: ImageBuilder will emit the Docker container's distro-compatible
  //       equivalent.
  @node def files: Seq[(String, String)] = // not RelativePath because we can't use them against JimFS paths
    List(
      ScriptName -> linuxContent,
      ScriptName + ".bat" -> windowsContent
    )

  @node def linuxContent: String = {
    val runnerBin = genericRunnerAppDir(mp).resolveFile(RunnerName).pathString
    // we need to find a valid `dirname` somehow.
    // "whence" is a ksh builtin, so this should work irrespective of PATH (as long as your system is at least kinda standard).
    // OAR_COMMON_DIR is understood by the launcher script (see runner.runconf).
    // We don't have ksh on cloud but I'll cross that bridge when I come to it.
    s"""#!${StaticConfig.string("ksh")}
       |DIRNAME_BIN=$$(whence dirname)
       |if [[ ! -x $$DIRNAME_BIN ]]; then DIRNAME_BIN=/usr/bin/dirname; fi
       |if [[ ! -x $$DIRNAME_BIN ]]; then DIRNAME_BIN=/bin/dirname;     fi
       |$OarCommonDir="$$($$DIRNAME_BIN $$0)/../" exec $runnerBin ${mp.properPath} "$$@"
       |""".stripMargin.trim
  }

  @node def windowsContent: String = {
    val runnerBin = genericRunnerAppDir(mp).resolveFile(s"$RunnerName.${NamingConventions.WindowsBatchExt}").pathString
    // interpolation done here to avoid issues with poor syntax highlighting in IDEA
    // see https://youtrack.jetbrains.com/issue/SCL-20422
    val oarCommon = s"""set $OarCommonDir=%OAR_PRE_DIRNAME%..\\"""
    // as above: this %~dp0 nonsense is 100% cribbed from the internet and the other start scripts (we don't have dirname)
    raw"""for %%F in ("%~dp0") do set OAR_PRE_DIRNAME=%%~dpF
         |$oarCommon
         |$runnerBin ${mp.properPath} %*
         |""".stripMargin.trim
  }
}

@entity class DockerGenericRunnerGenerator(
    install: InstallPathBuilder,
    config: GenericRunnerConfiguration
) extends BaseGenericRunnerGenerator(install, config) {
  import GenericRunnerInstaller._

  def targetObtInstallDir(obtVersion: String): Path = {
    val afsRoot = StaticConfig.string("afsRoot")
    Paths.get(s"${afsRoot}dist/optimus/PROJ/buildtool/$obtVersion/common/")
  }

  // On Docker container, the OBT runner is following AFS path
  @node def dockerContent(obtVersion: String, mp: MetaBundle): String = {
    val runnerBin = targetObtInstallDir(obtVersion).resolve("bin").resolve(RunnerName)
    // OAR_COMMON_DIR is understood by the launcher script (see runner.runconf).
    // We don't have ksh on cloud but I'll cross that bridge when I come to it.
    s"""#!${StaticConfig.string("sh")}
       |$OarCommonDir="$$(/usr/bin/dirname $$0)/../" exec $runnerBin ${mp.properPath} "$$@"
       |""".stripMargin
  }
}

@entity abstract class BaseGenericRunnerGenerator(
    install: InstallPathBuilder,
    config: GenericRunnerConfiguration
) {
  // We want to use OAR from the same version of OBT that built the artifacts: we must use compatible versions
  // for runconf compilation.
  @node def genericRunnerAppDir(mp: MetaBundle): Directory = {
    // In this order:
    //  - Take the override from internal.obt.genericRunnerAppDir (set during development or by OBT Runner build case,
    //    where we have to test for consistency between cutting edge OBT and the runner artifacts it generates)
    //  - Fallback on APP_DIR, which is set by OBT runscript, pointing us to its distribution (most frequent case)
    //  - Ultimately fallback to using the OBT artifacts coming from this build (exposes to risks of being
    //    incompatible with the actual OBT release)
    val runnerInstallPath =
      config.genericRunnerAppDirOverride
        .orElse { sys.env.get("APP_DIR").map(p => Directory(Paths.get(p))) }
        .getOrElse {
          val alternative = install.binDir(mp)
          log.debug(s"No OptimusAppRunner binary path: runner will point to ${alternative.pathString}")
          alternative
        }

    // Do away with symlinks when we generate the bin/run[.bat] scripts
    // We want to stabilize the OBT release we are referencing to
    Try(Directory(runnerInstallPath.path.toRealPath().toAbsolutePath)).toOption.getOrElse(runnerInstallPath)
  }
}

object GenericRunnerInstaller {
  final val ScriptName = "run"
  final val RunnerName = "app-runner"
  final val FingerprintKey = "GenericRunnerScript" // our content is our fingerprint
  final val OarCommonDir = "OAR_COMMON_DIR"
}
