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
package optimus.stratosphere.artifactory

import akka.http.scaladsl.model.Uri
import com.typesafe.config.Config
import com.typesafe.config.ConfigFactory
import optimus.stratosphere.bootstrap.StratosphereException
import optimus.stratosphere.config.StratoWorkspaceCommon
import optimus.stratosphere.filesanddirs.PathsOpts._
import optimus.stratosphere.filesanddirs.Unzip
import optimus.stratosphere.http.client.HttpClientFactory
import optimus.stratosphere.utils.CommonProcess
import optimus.stratosphere.utils.EnvironmentUtils
import spray.json._

import java.io.BufferedInputStream
import java.io.File
import java.io.FileNotFoundException
import java.net.URI
import java.nio.file.Files
import java.nio.file.Path
import scala.collection.compat._
import scala.collection.immutable.Seq
import scala.concurrent.duration._
import scala.util.Try
import scala.util.control.NonFatal
import scala.util.Using
import scala.util.matching.Regex

object ArtifactoryToolDownloader {
  val PipConfigFile = "PIP_CONFIG_FILE"
  val UvConfigFile = "UV_CONFIG_FILE"
  private val SfxNoProgressDialogAttribute: String = """Progress="no"
                                                       |""".stripMargin
  private val ExtractionFlag: String = "--explode"
  private val Jfrog = "jfrog"
  private val JarRepo = "jar-repo"
  private val Pypi = "pypi"

  private val maxRetry: Int = 1

  def installedTools(stratoWorkspace: StratoWorkspaceCommon): Seq[String] =
    for {
      tool <- stratoWorkspace.directoryStructure.toolsDir.dir.listDirs()
      version <- tool.dir.listDirs()
    } yield s"${tool.name} ver. ${version.name}"

  def availableTools(stratoWorkspace: StratoWorkspaceCommon): Set[String] =
    stratoWorkspace.artifactoryTools.availableTools.map(_.takeWhile(_ != '.'))

  private def artifactoryCliEnv(stratoWorkspace: StratoWorkspaceCommon): Map[String, String] =
    Map(
      "JFROG_CLI_OFFER_CONFIG" -> "false",
      // .resolve("") is required to add the trailing '/'
      "HOME" -> stratoWorkspace.internal.jfrog.home.getParent.resolve("").toString,
      "JFROG_CLI_HOME_DIR" -> stratoWorkspace.internal.jfrog.home.resolve("").toString,
      PipConfigFile -> stratoWorkspace.internal.pypi.configFile.toFile.toString,
      UvConfigFile -> stratoWorkspace.internal.pypi.uvConfigFile.toFile.toString,
      // setup_user does not like proxies
      "HTTPS_PROXY" -> "",
      "https_proxy" -> "",
      "HTTP_PROXY" -> "",
      "http_proxy" -> ""
    )

  def presetArtifactoryCredentials(stratoWorkspace: StratoWorkspaceCommon): Unit = {

    def failedMsg(key: String) = s"Setting up $key artifactory credentials failed"

    val jfrogConfigFile = stratoWorkspace.internal.jfrog.configFile
    val pypiConfigFile = stratoWorkspace.internal.pypi.configFile
    val uvConfigFile = stratoWorkspace.internal.pypi.uvConfigFile
    val jarRepoFile = stratoWorkspace.intellij.jarRepoFile
    lazy val credentials = Credential.fromJfrogConfFile(jfrogConfigFile)
    lazy val artifactoryUrl = stratoWorkspace.internal.tools.artifactoryUrl

    if (
      !jfrogConfigFile.exists() || jfrogConfigFile.toFile.length() == 0 || !credentials.exists(c =>
        artifactoryUrl.contains(c.host))
    )
      try {
        ArtifactoryToolDownloader.setupJfrogUser(stratoWorkspace)
      } catch {
        case NonFatal(e) =>
          val msg = failedMsg(Jfrog)
          stratoWorkspace.log.warning(msg)
          stratoWorkspace.log.debug(msg, e)
      }
    if (
      (!jarRepoFile.exists() || jarRepoFile.toFile.length() == 0) && jfrogConfigFile
        .exists() && jfrogConfigFile.toFile.length() > 0
    )
      try {
        ArtifactoryToolDownloader.setupRemoteJarRepo(jfrogConfigFile, jarRepoFile)
      } catch {
        case NonFatal(e) =>
          val msg = failedMsg(JarRepo)
          stratoWorkspace.log.warning(msg)
          stratoWorkspace.log.debug(msg, e)
      }
    if (
      !pypiConfigFile.exists() || pypiConfigFile.toFile.length() == 0 || !uvConfigFile
        .exists() || uvConfigFile.toFile.length() == 0
    )
      try {
        ArtifactoryToolDownloader.setupPypiUser(stratoWorkspace)
      } catch {
        case NonFatal(e) =>
          val msg = failedMsg(Pypi)
          stratoWorkspace.log.warning(msg)
          stratoWorkspace.log.debug(msg, e)
      }
    else stratoWorkspace.log.debug("Artifactory credentials already present")
  }

  def setupUser(stratoWorkspace: StratoWorkspaceCommon): String = {
    val jfrogResult = setupJfrogUser(stratoWorkspace)
    val pypiResult = setupPypiUser(stratoWorkspace)
    jfrogResult + pypiResult
  }

  private def setupCredentialInfo(key: String) = s"Setting up $key artifactory configuration"

  private def setupJfrogUser(stratoWorkspace: StratoWorkspaceCommon): String = {
    stratoWorkspace.log.info(setupCredentialInfo(Jfrog))
    setupGenericUser(
      stratoWorkspace.internal.tools.jfrogArtifactorySetupCmd,
      stratoWorkspace.internal.jfrog.configFile,
      stratoWorkspace)
  }

  def setupPypiUser(stratoWorkspace: StratoWorkspaceCommon): String = {
    stratoWorkspace.log.info(setupCredentialInfo(Pypi))
    setupGenericUser(
      stratoWorkspace.internal.tools.pypiArtifactorySetupCmd,
      stratoWorkspace.internal.pypi.configFile,
      stratoWorkspace)
  }

  private def setupGenericUser(
      setupUserCommand: Seq[String],
      configFile: Path,
      stratoWorkspace: StratoWorkspaceCommon,
      retry: Int = maxRetry): String = {
    val output =
      new CommonProcess(stratoWorkspace).runAndWaitFor(setupUserCommand, env = artifactoryCliEnv(stratoWorkspace))

    if (!configFile.exists() && retry < 1) {
      throw new StratosphereException(
        s"Setting up artifactory configuration failed, tried ${maxRetry + 1} times:\n$output")
    } else if (configFile.exists()) output
    else setupGenericUser(setupUserCommand, configFile, stratoWorkspace, retry - 1)
  }

  private def setupRemoteJarRepo(jfrogConfigFile: Path, jarRepoFile: Path): Unit = {
    val jfrogCfg: Config = ConfigFactory.parseFile(jfrogConfigFile.toFile)
    val artifactoryCfg: Config = jfrogCfg.getConfigList("artifactory").get(0)
    val uri: URI = new URI(artifactoryCfg.getString("url"))
    val portSuffix: String = if (uri.getPort > -1) s":${uri.getPort}" else ""
    val url: String = s"${uri.getHost}${portSuffix}${uri.getPath}"
    val user: String = artifactoryCfg.getString("user")
    val password: String = artifactoryCfg.getString("password")
    val xml: String =
      s"""<?xml version="1.0" encoding="UTF-8"?>
         |<project version="4">
         |  <component name="RemoteRepositoriesConfiguration">
         |    <remote-repository>
         |      <option name="id" value="07e6f448-0934-470b-9c89-bffa7e442ba6" />
         |      <option name="name" value="07e6f448-0934-470b-9c89-bffa7e442ba6" />
         |      <option name="url" value="${uri.getScheme}://$user:$password@${url}maven-all-local-only/" />
         |    </remote-repository>
         |  </component>
         |</project>""".stripMargin
    Files.createDirectories(jarRepoFile.getParent)
    jarRepoFile.file.write(xml)
  }

  def downloadArtifact(
      stratoWorkspace: StratoWorkspaceCommon,
      artifactoryLocation: String,
      downloadLocation: Path,
      extract: Boolean = false): Unit = {

    // we need a file separator at the end for artifactory download to put the file in the dir
    val downloadTo = downloadLocation.toString + File.separator

    def fetchApiKey(): String = {
      val timeout = 30 seconds
      val artifactoryUri = Uri(stratoWorkspace.internal.tools.artifactoryApikeyUrl)
      val httpClient = HttpClientFactory
        .factory(stratoWorkspace)
        .createClient(artifactoryUri, "Artifactory", timeout, sendCrumbs = true)
      val result = httpClient.get("/artifactory/api/security/apiKey")
      try {
        val apiKey = result.getString("apiKey")
        stratoWorkspace.log.info(s"User API key exists: ${apiKey.nonEmpty}")
        apiKey
      } catch {
        case NonFatal(apiAccessError) =>
          stratoWorkspace.log.error(s"Failed to obtain user API key", apiAccessError)
          throw apiAccessError
      }
    }

    def downloadFromJfrog(): String = {
      val jfrogCommand: Seq[String] = stratoWorkspace.internal.tools.jfrogCmd
      val extractParams = if (extract) Seq(ExtractionFlag) else Seq()
      val apiKeyParams = Seq("--apikey", fetchApiKey())
      val jfrogDownloadCommand = jfrogCommand ++ extractParams ++ apiKeyParams ++ Seq(artifactoryLocation, downloadTo)
      val output =
        new CommonProcess(stratoWorkspace).runAndWaitFor(
          jfrogDownloadCommand,
          label = Some("JFrog download command"),
          env = artifactoryCliEnv(stratoWorkspace))
      checkOutput(output)
      output
    }

    if (Try(downloadFromJfrog()).isFailure) {
      import stratoWorkspace.log

      log.info("Download failed, setting up artifactory configuration and re-trying...")
      try {
        log.info(setupUser(stratoWorkspace))
        log.info(downloadFromJfrog())
      } catch {
        case NonFatal(ex) =>
          log.error(s"Failed to download $artifactoryLocation from artifactory")
          if (artifactoryLocation.contains("-secure")) {
            log.error(stratoWorkspace.internal.messages.afSecure(EnvironmentUtils.userName))
          }
          throw ex
      }
    }
  }

  private def checkOutput(output: String): Unit = {
    import ArtifactoryResponseJsonProtocol._

    // jfrog cli does not fail if you try to download from a non-existing path, instead it returns a JSON response like
    // the one below, we need to check if 'success' is not 0 manually
    //
    // {
    //  "status": "success",
    //  "totals": {
    //    "success": 0,
    //    "failure": 0
    //  }
    // }

    val result = output.parseJson.convertTo[ArtifactoryResponse]
    if (result.totals.success == 0)
      throw new StratosphereException("Download failed, see log above for details.")
  }

  def noProgress7Zip(archive: Path, output: Path): Unit = {
    val target = ";!@InstallEnd@!\n".getBytes
    if (!Files.exists(archive))
      throw new FileNotFoundException(s"7z SFX file '${archive.toAbsolutePath.toString}' does not exist!")
    Using.Manager { use =>
      val is = use(new BufferedInputStream(Files.newInputStream(archive)))
      val out = use(Files.newOutputStream(output))

      val content = is.readAllBytes()

      val endOfPropertiesIndex = content.indexOfSlice(target)
      out.write(content.slice(0, endOfPropertiesIndex))
      // if doesn't already contain progress=no, add it
      if (content.slice(0, endOfPropertiesIndex).indexOfSlice(SfxNoProgressDialogAttribute.getBytes) == -1) {
        out.write(SfxNoProgressDialogAttribute.getBytes)
      }

      // write the remaining
      out.write(content.slice(endOfPropertiesIndex, content.length))
    }
  }

}

class ArtifactoryToolDownloader(
    stratoWorkspace: StratoWorkspaceCommon,
    name: String,
    requestedVersion: Option[String] = None) {

  import ArtifactoryToolDownloader._
  val version: String = requestedVersion.getOrElse(getProperty("version"))

  object install {
    val rootDir: Path = stratoWorkspace.directoryStructure.toolsDir.resolve(name)
    val toolDir: Path = rootDir.resolve(version)
    val installerPattern: Option[Regex] = stratoWorkspace.artifactoryTools.available.installerPattern(name)
  }

  object cache {
    val artifactoryLocation: String = getProperty("artifactory-path")
    val rootDir: Path = stratoWorkspace.directoryStructure.artifactoryCache.resolve(name)
    val toolDir: Path = rootDir.resolve(artifactoryLocation.split("/").to(Seq).drop(1).dropRight(1))
  }

  def install(forceReinstall: Boolean, verbose: Boolean = false): Boolean = {
    try {
      if (!install.toolDir.exists() || forceReinstall) {
        stratoWorkspace.log.info(s"Installing $name ver. $version")
        download()
        extract()
        stratoWorkspace.log.info(s"Installed $name at '${install.toolDir}'.")
      } else if (verbose) {
        stratoWorkspace.log.info(s"$name ver. $version already installed. Use '--force' to force re-install.")
      }
      true
    } catch {
      case NonFatal(e) =>
        stratoWorkspace.log.error(s"Failed to install $name to '${install.toolDir}'!", e)
        false
    }
  }

  def uninstall(): Unit = {
    stratoWorkspace.log.info(s"Removing $name installation from local drives")
    cache.toolDir.deleteIfExists()
    install.toolDir.deleteIfExists()
  }

  private def getProperty(prop: String): String =
    stratoWorkspace.artifactoryTools.available.forName(name).getString(prop)

  private def download(): Unit = {
    install.rootDir.dir.create()
    stratoWorkspace.log.info(s"Downloading $name from artifactory cache")
    downloadArtifact(stratoWorkspace, cache.artifactoryLocation, cache.rootDir)
  }

  def findInstallers(): Seq[Path] = {
    // filter files in rt cache dir by installerPattern if it exists
    cache.toolDir.dir.list().filter { potentialInstaller: Path =>
      install.installerPattern.forall(_.findFirstIn(potentialInstaller.name).isDefined)
    }
  }

  private def extract(): Unit = {
    findInstallers() match {
      case Seq() =>
        throw new FileNotFoundException(s"""No artifact files found in '${cache.toolDir}'!""")
      case Seq(installer) if installer.file.name.endsWith(".7z.exe") =>
        val noProgressInstaller =
          installer.resolveSibling(installer.getFileName.toString.replace(".7z.", ".7z.noprogress."))
        stratoWorkspace.log.info(s"Tweaking installer '$installer' as '$noProgressInstaller'")
        ArtifactoryToolDownloader.noProgress7Zip(installer, noProgressInstaller)
        stratoWorkspace.log.info(s"Unpacking $name from $noProgressInstaller to ${install.toolDir}")
        val command = Seq(s"${noProgressInstaller.toString}", "-y", s"""-o"${install.toolDir.toString}"""")
        val result = new CommonProcess(stratoWorkspace).runAndWaitFor(command, dir = Some(cache.rootDir))
        stratoWorkspace.log.info(result)
      case Seq(installer) if installer.file.name.endsWith(".zip") || installer.file.name.endsWith(".nupkg") =>
        stratoWorkspace.log.info(s"Unpacking $name from $installer to ${install.toolDir}")
        Unzip.extract(installer.toFile, install.toolDir.toFile)
      case other =>
        throw new FileNotFoundException(
          s"""Unsupported artifact file(s) found in '${cache.toolDir}': ${other.map(_.name).mkString(",")}""")
    }
  }
}
