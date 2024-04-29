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
package optimus.buildtool.compilers.venv
import optimus.buildtool.artifacts.CompilationMessage
import optimus.buildtool.artifacts.CompilationMessage.Severity
import optimus.buildtool.config.PythonConfiguration
import optimus.buildtool.dependencies.PythonDefinition
import optimus.buildtool.dependencies.PythonDependencyDefinition
import optimus.buildtool.config.PythonConfiguration.OverriddenCommands
import optimus.buildtool.files.Directory
import optimus.buildtool.utils.AssetUtils
import optimus.buildtool.utils.OsUtils
import optimus.buildtool.utils.Utils
import optimus.platform.async
import optimus.platform.entity
import optimus.platform.node
import optimus.platform.util.Log
import optimus.stratosphere.artifactory.ArtifactoryToolDownloader

import java.nio.file.Files
import java.nio.file.Paths
import java.nio.file.StandardCopyOption
import scala.collection.immutable.Seq
import scala.collection.mutable
import scala.sys.process.Process
import scala.sys.process.ProcessLogger

object AsyncVenvCommandRunner extends Log {

  @async def createVenv(
      venvName: String,
      pythonConfig: PythonConfiguration,
      sandboxBuild: Directory,
      pipCache: Directory,
      venvCache: Directory,
      credentialFile: String): Seq[CompilationMessage] = {

    import VenvCacher._
    import VenvUtils._

    if (pythonConfig.isArtifactory) createRequirements(sandboxBuild, pythonConfig)

    val venvDir = sandboxBuild.resolveDir(venvName)
    val cacheMessages =
      ensureCacheExists(pythonConfig.overriddenCommands, pythonConfig.python, pipCache, venvCache, credentialFile)
    copyCache(pythonConfig.python, venvCache, venvDir)

    def injectVars(cmd: String): String = cmd
      .replace(VenvDir, venvDir.pathString)
      .replace(PipCacheDir, pipCache.pathString)
      .replace(SitePackagesDir, sandboxBuild.resolveDir(sitePackages(venvName, pythonConfig)).pathString)

    val commands = Seq(
      prerequisites(credentialFile) ++
        (if (pythonConfig.isArtifactory)
           Seq(
             pythonConfig.overriddenCommands.pipInstallCmd
               .map(injectVars)
               .getOrElse(installLibs(venvName, pipCache)))
         else Nil)
    ).flatten.mkString(" && ")

    launchWithPython(pythonConfig.python, "Venv", commands, sandboxBuild) ++ cacheMessages
  }
}

object VenvUtils extends Log {
  private[buildtool] val VenvCacheDir = "{venvCacheDir}"
  private[buildtool] val VenvDir = "{venvDir}"
  private[buildtool] val SitePackagesDir = "{sitePackagesDir}"
  private[buildtool] val PipCacheDir = "{pipCacheDir}"
  private[buildtool] val CacheVenvName = "{cacheVenvName}"
  private[buildtool] val TmpCacheVenvName = "{tmpCacheVenvName}"

  @async def launchWithPython(
      python: PythonDefinition,
      prefix: String,
      cmdline: String,
      workingDir: Directory): Seq[CompilationMessage] = {
    python.binPath match {
      case Some(pythonBinPath) =>
        launch(prefix, cmdline, workingDir, Seq(pythonBinPath)) {
          case out if out.contains("ERROR:") =>
            CompilationMessage(None, out, Severity.Error)
        }
      case None => Seq(CompilationMessage(None, python.notFoundMessage, Severity.Error))
    }
  }

  @async def launch(prefix: String, cmdLine: String, workingDir: Directory, addToPath: Seq[String])(
      pf: PartialFunction[String, CompilationMessage]): Seq[CompilationMessage] = {
    val cmds =
      if (Utils.isWindows) Seq("cmd.exe", "/c", cmdLine)
      else Seq("ksh", "-c", cmdLine)

    val pb = new ProcessBuilder(cmds: _*)
    pb.directory(workingDir.path.toFile)

    val pathEnv = {
      val currentPath = pb.environment().getOrDefault("PATH", "")
      val separator = if (Utils.isWindows) ";" else ":"
      val toAdd = addToPath.mkString("", separator, separator)
      toAdd + currentPath
    }
    pb.environment().put("PATH", pathEnv)

    val logging = mutable.Buffer[String]()

    logging += s"Command line: ${cmds.mkString(" ")}"
    val returnCode = Process(pb) ! ProcessLogger { s =>
      logging += s
    }
    logging += s"Return code: $returnCode"

    val messages = logging.collect(pf)
    val failed = returnCode != 0

    if (failed) {
      logging.foreach(l => log.warn(s"$prefix $l"))
      val knownError = messages.exists(_.isError)
      if (!knownError)
        messages += CompilationMessage(
          None,
          s"""Process ${cmds.mkString(" ")} ended up with status code: $returnCode,
             |output: ${logging.mkString("\n")}""".stripMargin,
          Severity.Error
        )
    } else {
      logging.foreach(l => log.debug(s"$prefix $l"))
    }

    messages.toIndexedSeq
  }

  def pythonVenv(tmpVenvName: String, venvName: String, python: PythonDefinition, pipCache: Directory): String = Seq(
    s"python -m venv --system-site-packages --copies $tmpVenvName",
    s"${scripts(tmpVenvName)}python -m pip --disable-pip-version-check install venv-pack2==${python.venvPack} --cache-dir ${pipCache.path}",
    s"${scripts(tmpVenvName)}venv-pack -p $tmpVenvName -o $venvName"
  ).mkString("&&")

  private def scripts(venvPath: String): String =
    if (Utils.isWindows) s"$venvPath\\Scripts\\"
    else s"$venvPath/bin/"

  def libDefinition(lib: PythonDependencyDefinition): String = s"${lib.pypiName}==${lib.version}"

  def createRequirements(sandboxBuild: Directory, pythonConfig: PythonConfiguration): Unit = {
    val requirements = sandboxBuild.resolveFile("requirements.txt")
    Files.writeString(requirements.path, pythonConfig.artifactoryDependencies.map(libDefinition).mkString("\n"))
  }
  def prerequisites(credentialFile: String): Seq[String] = {
    val setEnv = if (OsUtils.isWindows) "set" else "export"
    val pipSetup = {
      if (Files.exists(Paths.get(credentialFile))) {
        Seq(s"$setEnv ${ArtifactoryToolDownloader.PipConfigFile}=$credentialFile")
      } else {
        log.warn(s"${ArtifactoryToolDownloader.PipConfigFile} not set; Pypi Artifactory access may fail")
        Seq.empty
      }
    }
    val homeSetup = if (OsUtils.isWindows) Seq.empty else Seq("export HOME=/var/tmp/$USER")
    pipSetup ++ homeSetup
  }
  def installLibs(venvPath: String, pipCache: Directory): String =
    s"${scripts(venvPath)}python -m pip --disable-pip-version-check install -r requirements.txt --cache-dir ${pipCache.path}"

  def sitePackages(venv: String, pythonConfig: PythonConfiguration): String =
    if (OsUtils.isWindows) s"$venv/Lib/site-packages"
    else s"$venv/lib/python${majorMinorPython(pythonConfig)}/site-packages"

  /* extracts major and minor python version
  "3.10.10" -> "3.10"
  "3.10.10-1 -> "3.10"
   */
  def majorMinorPython(pythonConfig: PythonConfiguration): String =
    pythonConfig.python.version
      .split("\\.")
      .take(2)
      .mkString(".")
}

@entity object VenvCacher {
  import VenvUtils._

  private def cacheName(python: PythonDefinition): String = python.hash

  private def cacheExists(python: PythonDefinition, cacheDir: Directory): Boolean =
    cacheDir.resolveDir(python.hash).exists

  @async def copyCache(python: PythonDefinition, venvCache: Directory, to: Directory): Unit = {
    val src = venvCache.resolveDir(cacheName(python))
    Files.walk(src.path).forEach { source =>
      val target = to.path.resolve(src.path.relativize(source))
      Files.copy(source, target, StandardCopyOption.COPY_ATTRIBUTES)
    }
  }

  @async private def create(
      overriddenCmds: OverriddenCommands,
      python: PythonDefinition,
      pipCache: Directory,
      venvCache: Directory,
      credentialFile: String): Seq[CompilationMessage] = {
    if (!venvCache.exists) Files.createDirectories(venvCache.path)

    val cacheVenvName = cacheName(python)
    val tmpCacheVenvName = cacheVenvName + "-tmp"
    val tmpCacheDir = venvCache.resolveDir(tmpCacheVenvName)

    def injectVars(cmd: String): String = {
      cmd
        .replace(TmpCacheVenvName, tmpCacheVenvName)
        .replace(CacheVenvName, cacheVenvName)
        .replace(PipCacheDir, pipCache.pathString)
        .replace(VenvCacheDir, venvCache.pathString)
    }

    val cmds = Seq(
      prerequisites(credentialFile) ++
        Seq(
          overriddenCmds.pythonVenvCmd
            .map(injectVars)
            .getOrElse(pythonVenv(tmpCacheVenvName, cacheVenvName, python, pipCache)))
    ).flatten.mkString(" && ")

    val result = launchWithPython(python, "Venv", cmds, venvCache)
    AssetUtils.recursivelyDelete(tmpCacheDir)
    result
  }

  /* node is used here in order to synchronize async calls,
     we have to make sure that 2 processes aren't creating the same cache in the same time */
  @node def ensureCacheExists(
      overriddenCmds: OverriddenCommands,
      forPython: PythonDefinition,
      pipCache: Directory,
      venvCacheDir: Directory,
      credentialFile: String): Seq[CompilationMessage] = {
    if (!cacheExists(forPython, venvCacheDir))
      create(overriddenCmds, forPython, pipCache, venvCacheDir, credentialFile)
    else Nil
  }

  import optimus.buildtool.cache.NodeCaching.reallyBigCache
  `ensureCacheExists_info`.setCustomCache(reallyBigCache)
}
