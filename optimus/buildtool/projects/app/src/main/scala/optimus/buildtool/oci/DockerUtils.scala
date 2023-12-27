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
package optimus.buildtool.oci

import optimus.buildtool.app.OptimusBuildToolBootstrap
import optimus.buildtool.builders.BackgroundCmdId
import optimus.buildtool.builders.BackgroundProcessBuilder
import optimus.buildtool.config.ScopeId
import optimus.buildtool.files.Directory
import optimus.buildtool.files.FileAsset
import optimus.buildtool.format.docker.ExtraImageDefinition
import optimus.buildtool.trace.DockerCommand
import optimus.buildtool.utils.AssetUtils
import optimus.platform.util.Log
import optimus.platform._

import java.nio.file.Files
import java.nio.file.Path
import java.nio.file.Paths
import scala.collection.immutable.Seq
import scala.util.control.NonFatal

class DockerUtils(dockerImageCacheDir: Directory) extends Log {
  private val dockerProcessId = BackgroundCmdId("docker")
  private val logDir = dockerImageCacheDir.resolveDir("cmdLog")
  Files.createDirectories(logDir.path)

  private val dockerCmdLogFile: FileAsset = {
    val timestamp: String = OptimusBuildToolBootstrap.generateLogFilePrefix()
    logDir.resolveFile(s"$timestamp.log")
  }

  /**
   * save target cmd output to specified file, be used for read the output.
   * @param name
   *   customized log file suffix name, added after the timestamp, for example: 2023-03-03_19-17-30.name.log
   * @return
   *   FileAsset for customized log file
   */
  private def customLogFile(name: String): FileAsset = {
    val timestamp: String = OptimusBuildToolBootstrap.generateLogFilePrefix()
    logDir.resolveFile(s"$timestamp.$name.log")
  }

  @async protected def runDockerProcess(
      cmds: Seq[String],
      logFile: FileAsset = dockerCmdLogFile,
      retry: Int = 0
  ): Unit =
    BackgroundProcessBuilder(dockerProcessId, logFile, cmds)
      .buildWithRetry(ScopeId.RootScopeId, DockerCommand)(maxRetry = retry, msDelay = 0, lastLogLines = 100)

  private def getCmdOutput(logFile: FileAsset): Seq[String] = BackgroundProcessBuilder.lastLogLines(logFile, 100)

  @async def pullDockerImage(imageName: String): Unit = {
    runDockerProcess(Seq("docker", "pull", imageName), retry = 3)
    log.info(s"[$imageName] successfully pulled image!")
  }

  @async private def saveDockerImageAsTar(imageName: String, saveTo: FileAsset): FileAsset = {
    runDockerProcess(Seq("docker", "save", "-o", saveTo.pathString, imageName), retry = 3)
    log.info(s"[$imageName] successfully downloaded image into obt cache: ${saveTo.pathString}")
    saveTo
  }

  @async private def extractTargetsFromDockerTar(
      targets: Seq[Path],
      fromTar: FileAsset,
      extractTo: Directory): Set[FileAsset] = {
    Files.createDirectories(extractTo.path)

    val tarPathStr = fromTar.pathString

    // search all layer.tar files in tar
    val getLayersLogFile = customLogFile("layers")
    runDockerProcess(Seq("tar", "-tf", tarPathStr, "*.tar"), getLayersLogFile)
    val layerTars = getCmdOutput(getLayersLogFile)

    // try search & extract targets for each layer.tar file
    if (layerTars.isEmpty)
      throw new NoSuchElementException(
        s"[${fromTar.name}] no layer.tar found! please verify your docker image is build from dockerfile.")
    else {
      for {
        layerTar <- layerTars
        targetPath <- targets
      } runDockerProcess(
        Seq(
          "/bin/ksh",
          s"""|cd ${extractTo.pathString}
              |tar -xOf $tarPathStr $layerTar | tar -x ${targetPath.toString.replaceFirst("/+", "")}
              |echo $$?""".stripMargin // force tar cmd not throw exception when not found target path
        )
      )
    }

    val extractedFiles = Directory.findFiles(extractTo).toSet
    if (extractedFiles.isEmpty)
      throw new NoSuchElementException(
        s"[${fromTar.name}] No files are matching paths: '${targets.mkString(", ")}'. Please check your extraImages paths in docker.obt.")
    else log.info(s"[${fromTar.name}] successfully downloaded ${extractedFiles.size} files to: $extractTo")
    extractedFiles
  }

  @async private def removeImage(imageName: String): Unit = {
    runDockerProcess(Seq("docker", "rmi", imageName))
    log.info(s"[$imageName] successfully removed downloaded image in docker's cache.")
  }

  /**
   * this method be used for generate legal file name, the downloaded tars will be saved into same cache dir.
   * @param imageName
   *   docker image name, for example 'my/image:7.2.1'
   * @return
   *   legal file name like 'my-image-filename-7.2.1'
   */
  private def extraImageFileName(imageName: String) =
    s"${imageName.substring(imageName.indexOf("/") + 1).replaceAll("/", "-").replaceAll(":", "-")}"

  /**
   * get target files from predefined 'extraImages' paths in docker.obt
   * @param extraImage
   *   extra image definition, be predefined in docker.obt
   * @return
   *   Map(in output.tar path -> local disk file path)
   */
  @entersGraph def getExtraImageFilesMap(extraImage: ExtraImageDefinition): Map[Path, Path] = {
    if (extraImage.pathsToIncluded.isEmpty)
      throw new UnsupportedOperationException(
        s"[${extraImage.name}] is empty! please check docker.obt and add your paths into extraImages.")

    log.info(s"[${extraImage.name}] downloading docker image files now...")

    val fileName = extraImageFileName(extraImage.name)
    val downloadTarTo = dockerImageCacheDir.resolveFile(s"$fileName.tar")
    val extractTo = dockerImageCacheDir.resolveDir(fileName)

    if (extractTo.exists)
      try {
        log.debug(s"[${extraImage.name}] found extract dir from previous docker build, cleaning now...")
        AssetUtils.recursivelyDelete(extractTo)
        log.debug(s"[${extraImage.name}] successfully cleaned extract dir: $extractTo")
      } catch {
        case NonFatal(e) => log.error(s"Clean old extract dir failed: $extractTo", e)
      }

    val extraImageFiles =
      if (downloadTarTo.exists) { // try load cached tar file.
        log.info(s"[${extraImage.name}] found docker image from local disk: ${downloadTarTo.pathString}")
        extractTargetsFromDockerTar(extraImage.pathsToIncluded, downloadTarTo, extractTo)
      } else { // try download image
        pullDockerImage(extraImage.name)
        saveDockerImageAsTar(extraImage.name, downloadTarTo)
        removeImage(extraImage.name)
        extractTargetsFromDockerTar(extraImage.pathsToIncluded, downloadTarTo, extractTo)
      }

    extraImageFiles.map { file =>
      val localDiskFilePath = file.path.toAbsolutePath
      val inOutputTarPath = Paths.get(localDiskFilePath.toString.replace(extractTo.path.toAbsolutePath.toString, ""))
      inOutputTarPath -> localDiskFilePath
    }.toMap
  }

}
