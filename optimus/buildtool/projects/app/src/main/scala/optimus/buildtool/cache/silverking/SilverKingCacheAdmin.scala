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
package optimus.buildtool.cache.silverking

import java.io.ByteArrayInputStream
import java.nio.charset.StandardCharsets.UTF_8
import java.nio.file.Files
import java.nio.file.Paths
import java.util.zip.GZIPInputStream
import java.util.zip.ZipException
import optimus.buildtool.OptimusBuildTool
import optimus.buildtool.app.OptimusBuildToolCmdLineT.NoneArg
import optimus.buildtool.artifacts.ArtifactType
import optimus.buildtool.artifacts.CachedArtifactType
import optimus.buildtool.cache.silverking.SilverKingStore.ArtifactKey
import optimus.buildtool.cache.silverking.SilverKingStore.DirectoryContent
import optimus.buildtool.cache.silverking.SilverKingStore.FileContent
import optimus.buildtool.config.ScopeId
import optimus.buildtool.files.Directory
import optimus.buildtool.files.FileAsset
import optimus.buildtool.utils.AssetUtils
import optimus.buildtool.utils.FlexibleBooleanOptionHandler
import optimus.buildtool.utils.PathUtils
import optimus.platform._
import optimus.platform.OptimusApp.ExitHandler
import optimus.platform.util.ArgHandlers.StringOptionOptionHandler
import optimus.platform.util.Log
import org.apache.poi.util.IOUtils
import org.kohsuke.args4j.CmdLineParser

private[silverking] class SilverKingCacheAdminCmdLine extends OptimusAppCmdLine {

  import org.kohsuke.{args4j => args}

  @args.Option(name = "--silverKing", required = true, aliases = Array("--sk", "--silverking"))
  val silverKing: String = ""

  @args.Option(name = "--silverKingContent", required = false)
  val silverKingContent: String = ""

  @args.Option(name = "--artifactVersion", required = false, aliases = Array("--obtVersion"))
  val artifactVersion: String = OptimusBuildTool.DefaultArtifactVersionNumber

  @args.Option(name = "--get", required = false)
  val get: Boolean = false

  @args.Option(name = "--put", required = false)
  val put: Boolean = false

  @args.Option(name = "--invalidate", required = false)
  val invalidate: Boolean = false

  @args.Option(name = "--scope", required = true)
  val scope: String = ""

  @args.Option(name = "--hash", required = true)
  val hash: String = ""

  @args.Option(name = "--type", required = true, usage = "Artifact type (in lower case)")
  val artifactType: String = ""

  @args.Option(
    name = "--discriminator",
    required = false,
    handler = classOf[StringOptionOptionHandler],
    usage = "Artifact discriminator")
  val discriminator: Option[String] = None

  @args.Option(name = "--keyFile", required = false)
  val keyFile: String = ""

  @args.Option(name = "--input", required = false, aliases = Array("-i"))
  val input: String = ""

  @args.Option(name = "--output", required = false, aliases = Array("-o", "--out"))
  val output: String = ""

  @args.Option(name = "--forward", required = false)
  val forward: Boolean = true

  @args.Option(name = "--unzip", required = false)
  val unzip: Boolean = true
}

object SilverKingCacheAdmin extends OptimusApp[SilverKingCacheAdminCmdLine] with Log {

  override protected def parseCmdline(args: Array[String], exitHandler: ExitHandler): Unit = {
    CmdLineParser.registerHandler(classOf[Boolean], classOf[FlexibleBooleanOptionHandler])
    super.parseCmdline(args, exitHandler)
  }

  @entersGraph override def run(): Unit = {

    val config = SilverKingConfig(cmdLine.silverKing)
    log.info(s"SilverKing artifact version: ${cmdLine.artifactVersion}")

    val artifactType =
      if (cmdLine.artifactType == NoneArg) null // used for cached maven jars
      else ArtifactType.parse(cmdLine.artifactType).asInstanceOf[CachedArtifactType]
    val key = ArtifactKey(
      ScopeId.parse(cmdLine.scope),
      cmdLine.hash,
      artifactType,
      cmdLine.discriminator,
      cmdLine.artifactVersion
    )

    // Always use ClusterType.Custom here so we don't affect stats for builds
    val ops = createOps(cmdLine.silverKing)
    val contentOps = if (cmdLine.silverKingContent.nonEmpty) createOps(cmdLine.silverKingContent) else ops

    if (cmdLine.keyFile.nonEmpty) {
      val keyFile = FileAsset(Paths.get(cmdLine.keyFile).toAbsolutePath)
      log.info(s"Writing key to ${keyFile.pathString}")
      Files.createDirectories(keyFile.parent.path)
      AssetUtils.atomicallyWrite(keyFile, replaceIfExists = true) { p =>
        import ArtifactJsonProtocol._
        import spray.json._
        val str = key.toJson.prettyPrint
        Files.write(p, str.getBytes(UTF_8))
      }
    }

    (cmdLine.get, cmdLine.put, cmdLine.invalidate) match {
      case (true, false, false) => get(ops, contentOps, key)
      case (false, true, false) => put(ops, contentOps, key)
      case (false, false, true) => invalidate(ops, key)
      case _ =>
        throw new IllegalArgumentException("One (and only one) of '--get', '--put' or '--invalidate' must be specified")
    }
  }

  @async private def createOps(configStr: String): SilverKingOperations = {
    val config = SilverKingConfig(configStr)
    // Always use ClusterType.Custom here so we don't affect stats for builds
    SilverKingOperationsImpl(config, ClusterType.Custom, debug = true, forward = cmdLine.forward)
  }

  @async def get(ops: SilverKingOperations, contentOps: SilverKingOperations, key: ArtifactKey): Unit = {
    log.info(s"Getting cached artifact for key: $key")

    ops.getArtifact(key) match {
      case Some(content) =>
        log.info(s"Query result: $content")
        val outputPath = Paths.get(cmdLine.output).toAbsolutePath
        val asset = content match {
          case FileContent(_)      => FileAsset(outputPath)
          case DirectoryContent(_) => Directory(outputPath)
        }
        contentOps.getContentData(key, content, asset) match {
          case Some(files) =>
            val filesInfo = files.map {
              case (file, buffer) if file == asset => s"${buffer.limit()} bytes"
              case (file, buffer)                  => s"\n  ${file.pathString} (${buffer.limit()} bytes)"
            }
            log.info(s"Data: ${filesInfo.mkString}")
            if (cmdLine.output.nonEmpty) {
              val unzipped = files.forall { case (file, contentBuffer) =>
                Files.createDirectories(file.parent.path)
                AssetUtils.atomicallyWrite(file, replaceIfExists = true) { p =>
                  val contentBytes =
                    if (contentBuffer.hasArray) contentBuffer.array
                    else {
                      val arr = new Array[Byte](contentBuffer.limit())
                      contentBuffer.get(arr)
                      arr
                    }
                  val processedBytes = if (cmdLine.unzip) {
                    var zIn: GZIPInputStream = null
                    try {
                      zIn = new GZIPInputStream(new ByteArrayInputStream(contentBytes))
                      IOUtils.toByteArray(zIn)
                    } catch {
                      case e: ZipException =>
                        log.warn(s"Failed to unzip content, will write to ${file.pathString} without unzipping ($e)")
                        contentBytes
                    } finally if (zIn != null) zIn.close()
                  } else contentBytes
                  Files.write(p, processedBytes)
                  contentBytes ne processedBytes
                }
              }
              val unzippedStr = if (unzipped) "unzipped and " else ""
              log.info(
                s"${files.size} file(s) ${unzippedStr}written to ${PathUtils.platformIndependentString(outputPath)}")
            }
          case None => log.info("No data found")
        }
      case None =>
        log.info(s"No value found for key $key")
    }
  }

  @async def put(ops: SilverKingOperations, contentOps: SilverKingOperations, key: ArtifactKey): Unit = {
    if (cmdLine.input.isEmpty) throw new IllegalArgumentException("No --input specified for --put")
    log.info(s"Putting path content for key: $key")
    val inputPath = Paths.get(cmdLine.input).toAbsolutePath
    val (content, contentSize) = contentOps.putContentData(key, inputPath)
    log.info(s"$contentSize bytes written")
    log.info(s"Putting cached artifact for key: $key")
    ops.putArtifact(key, content)
    log.info("Put complete")
  }

  @async def invalidate(ops: SilverKingOperations, key: ArtifactKey): Unit = {
    log.info(s"Invalidating cached artifact for key: $key")
    ops.invalidateArtifact(key)
    log.info("Invalidate complete")
  }
}
