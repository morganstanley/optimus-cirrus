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
package optimus.buildtool.builders.postinstallers.uploaders

import optimus.breadcrumbs.crumbs.Properties
import optimus.buildtool.builders.postinstallers.uploaders.AssetUploader.UploadFormat
import optimus.buildtool.config.ScopeId.RootScopeId
import optimus.buildtool.files.Asset
import optimus.buildtool.files.Directory
import optimus.buildtool.trace.Upload
import optimus.buildtool.utils.OsUtils
import optimus.buildtool.utils.Utils
import optimus.buildtool.utils.process.ExternalProcessBuilder
import optimus.platform._
import optimus.platform.throttle.Throttle
import optimus.platform.util.Log

import java.io.File
import java.util.concurrent.ConcurrentHashMap

class LocationUploader(
    id: String,
    val location: UploadLocation,
    throttle: Throttle,
    maxRetries: Int,
    processBuilder: ExternalProcessBuilder,
    toolsDir: Option[Directory]
) extends Log {

  private val category = Upload(id, location)

  private val toolBinDirs = toolsDir.map(d => Seq(d.resolveDir("bin"), d.resolveDir("usr/bin")))

  // use `path.toString` here so we get the OS-specific path format
  private val env = toolBinDirs
    .map { ds =>
      val pathAdditions = ds.map(_.path.toString).mkString(File.pathSeparator)
      Map("PATH" -> s"$pathAdditions${File.pathSeparator}${sys.env.getOrElse("PATH", sys.env("Path"))}")
    }
    .getOrElse(Map.empty)
  protected val exe: String = if (OsUtils.isWindows) ".exe" else ""

  private val isInitialized = new ConcurrentHashMap[UploadFormat, Boolean]
  @async private def init(format: UploadFormat) = {
    if (!isInitialized.getOrDefault(format, false)) {
      // may run more than once, but that's ok. Init commands are idempotent and thread-safe.
      // throttle will ensure it won't run a large number of times
      location.initCmds(format).aseq.foreach(launchProcess)
      isInitialized.put(format, true)
    }
  }

  @async def uploadToLocation(source: Asset, format: UploadFormat): Long =
    throttle {
      log.debug(s"[$id] Starting upload for ${source.name}...")
      init(format)
      val (durationInNanos, _) = AdvancedUtils.timed {
        location.cmds(source, format).aseq.foreach(launchProcess)
      }
      durationInNanos
    }

  @async private def launchProcess(cmd: Seq[String]): Unit = {
    // waiting 1s the first time, 2s the second time, 3s the third time, etc..
    val (durationInNanos, _) = AdvancedUtils.timed {
      val rawExecutable = s"${cmd.head}$exe"
      val args = cmd.tail
      // look for the full path to the executable
      val executable = toolBinDirs
        .flatMap { ds =>
          ds.map(_.resolveFile(rawExecutable)).find(_.exists)
        }
        .map(_.path.toString)
        .getOrElse(rawExecutable)
      val fullCmd = executable +: args

      execute(fullCmd)
    }
    log.debug(s"[$id] Cmd ${cmd.mkString(" ")} executed in ${Utils.durationString(durationInNanos / 1000000L)}")
  }

  private val defaultProps: Seq[Properties.Elem[_]] = {
    val host = location match {
      case UploadLocation.Remote(h, _, _, _) => Some(h)
      case _: UploadLocation.Local           => None
    }

    Seq(Properties.obtUploadTargetDir -> location.target.pathString) ++ host.map(Properties.obtUploadHost -> _)
  }

  @async protected def execute(fullCmd: Seq[String]): Unit = {
    // reusing the same log file for each location uploader
    // strip out LD_PRELOAD since we don't want/need that for the upload command (and it can cause problems for
    // cygwin-based apps on windows)
    processBuilder
      .build(
        RootScopeId,
        id,
        cmdLine = fullCmd,
        Some(category),
        env,
        Seq("LD_PRELOAD"),
        lastLogLines = 20
      )
      .startWithRetry(
        maxRetries = maxRetries,
        msDelay = 1000,
        // this is intended to reduce noise in console log for our greedy approach: when the OBT batch uploader is
        // using the maximum NFS I/O bandwidth, we will occasionally get 1 retry
        showWarningsAfter = if (maxRetries > 2) 1 else 0
      )
  }

}
