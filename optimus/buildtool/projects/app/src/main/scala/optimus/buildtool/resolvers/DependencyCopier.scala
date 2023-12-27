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
package optimus.buildtool.resolvers

import java.nio.file.Files
import optimus.buildtool.artifacts.Artifact
import optimus.buildtool.artifacts.ClassFileArtifact
import optimus.buildtool.artifacts.ExternalClassFileArtifact
import optimus.buildtool.cache.NoOpRemoteAssetStore
import optimus.buildtool.cache.RemoteAssetStore
import optimus.buildtool.config.ScopeId.RootScopeId
import optimus.buildtool.files.BaseHttpAsset
import optimus.buildtool.files.Directory
import optimus.buildtool.files.FileAsset
import optimus.buildtool.files.JarAsset
import optimus.buildtool.files.ReactiveDirectory
import optimus.buildtool.resolvers.MavenUtils.wrongCredMsg
import optimus.buildtool.trace.CheckRemoteUrl
import optimus.buildtool.trace.DepCopy
import optimus.buildtool.trace.DepCopyFromRemote
import optimus.buildtool.trace.ObtStats
import optimus.buildtool.trace.ObtTrace
import optimus.buildtool.utils.Utils.atomicallyWriteIfMissing
import optimus.buildtool.utils.PathUtils.isDisted
import optimus.buildtool.utils.Utils.localize
import optimus.platform._
import optimus.stratosphere.artifactory.Credential

import java.io.FileNotFoundException
import java.io.InputStream
import java.security.InvalidParameterException

@entity class DependencyCopier(
    depCopyPath: Directory,
    credentials: Seq[Credential] = Nil,
    assetStore: RemoteAssetStore = NoOpRemoteAssetStore,
    depCopyFileSystemAsset: Boolean = false
) {

  @node def atomicallyDepCopyArtifactsIfMissing(artifact: Artifact): Artifact = artifact match {
    case c: ExternalClassFileArtifact => atomicallyDepCopyExternalClassFileArtifactsIfMissing(c)
    case a                            => a
  }

  @node def atomicallyDepCopyClassFileArtifactsIfMissing(artifact: ClassFileArtifact): ClassFileArtifact =
    artifact match {
      case c: ExternalClassFileArtifact => atomicallyDepCopyExternalClassFileArtifactsIfMissing(c)
      case a                            => a
    }

  @node def atomicallyDepCopyExternalClassFileArtifactsIfMissing(
      artifact: ExternalClassFileArtifact
  ): ExternalClassFileArtifact =
    // n.b. we never extract signature jars since they specifically want classes
    artifact.copyWithUpdatedAssets(
      file = atomicallyDepCopyJarIfMissing(artifact.file),
      source = artifact.source.map(atomicallyDepCopyJarIfMissing),
      javadoc = artifact.javadoc.map(atomicallyDepCopyJarIfMissing)
    )

  @node def atomicallyDepCopyJarIfMissing(depFile: FileAsset): JarAsset = atomicallyDepCopyFileIfMissing(depFile).asJar

  @node def atomicallyDepCopyFileIfMissing(depFile: FileAsset): FileAsset = depFile match {
    case http: BaseHttpAsset => atomicallyCopyOverHttp(http)
    case d                   => atomicallyCopyOverAfs(d)
  }

  def httpLocalAsset(httpAsset: BaseHttpAsset): httpAsset.LocalAsset =
    MavenUtils.httpLocalAsset(httpAsset, depCopyPath)

  def skipCopyMissingFileMsg(depFile: FileAsset, reason: String = ""): FileAsset = {
    val reasonMsg = if (reason.nonEmpty) s", $reason" else ""
    val msg = s"Not depcopying missing file: ${depFile.pathString}$reasonMsg"
    log.warn(msg)
    ObtTrace.warn(msg)
    depFile
  }

  @node def getHttpStream(httpAsset: BaseHttpAsset, retry: Int = MavenUtils.maxRetry): Either[Exception, InputStream] =
    ObtTrace.traceTask(RootScopeId, CheckRemoteUrl) {
      try {
        val host = httpAsset.url.getHost
        val cred = credentials.find(_.host == host)
        val conn = httpAsset.url.openConnection()
        cred.foreach(c => conn.setRequestProperty("Authorization", c.toHttpBasicAuth))
        Right(conn.getInputStream)
      } catch {
        case e: Exception =>
          if (e.getMessage.contains("401"))
            Left(new InvalidParameterException(s"$wrongCredMsg $e"))
          else if (e.getMessage.contains("404") || e.isInstanceOf[FileNotFoundException])
            Left(new NoSuchElementException(e.toString))
          else Left(new UnsupportedOperationException(e.toString))
      }
    }

  @node def atomicallyCopyOverHttp(httpAsset: BaseHttpAsset, isMarker: Boolean = false): httpAsset.LocalAsset = {
    // even if depCopy is disabled we still need to copy HTTP Jars locally, so use tempDir instead
    val localFile = httpLocalAsset(httpAsset)

    val fetchedFromMaven = atomicallyWriteIfMissing(localFile) { tmp =>
      ObtTrace.traceTask(RootScopeId, DepCopyFromRemote) {
        if (assetStore.get(httpAsset.url, FileAsset(tmp)).isDefined) false // false because we fetched from SK
        else if (isMarker) {
          try { // put empty marker url file in local disk
            Files.createDirectories(tmp.getParent)
            Files.createFile(tmp)
          } catch { case e: Exception => log.warn(s"Create marker url file failed! $e") }
          true
        } else
          getHttpStream(httpAsset) match {
            case Left(e) => skipCopyMissingFileMsg(localFile, e.toString)
            case Right(stream) =>
              try { // put url file in local disk
                Files.createDirectories(tmp.getParent)
                Files.copy(stream, tmp)
              } finally stream.close()
              log.debug(s"Downloaded maven file to: $localFile")
              ObtTrace.addToStat(ObtStats.MavenDownloads, 1)
              true
          }
      }
    }

    // if we fetched from maven then the artifact wasn't in SK (modulo a small race, but re-putting is safe)
    // so we should put it. Note that we do this using the final localFile name, not the tmp file, because the put
    // is async and the tmp file is often removed by the point where the put actually reads the file.
    if (fetchedFromMaven.contains(true)) assetStore.put(httpAsset.url, localFile)

    localFile
  }

  @node def atomicallyCopyOverAfs(depFile: FileAsset): FileAsset = if (depCopyFileSystemAsset && isDisted(depFile)) {
    val localFile = localize(depFile, depCopyPath)
    // existsUnsafe is ok to call here as long as users aren't deleting depCopy files while OBT is running
    if (!localFile.existsUnsafe) {
      if (depFile.existsUnsafe) {
        atomicallyWriteIfMissing(localFile) { tmp =>
          ObtTrace.traceTask(RootScopeId, DepCopy) {
            Files.createDirectories(tmp.getParent)
            Files.copy(depFile.path, tmp)
          }
        }
        localFile
      } else skipCopyMissingFileMsg(depFile)
    } else localFile
  } else depFile

  @node def depCopyDirectoryIfMissing(dir: ReactiveDirectory): Directory = if (
    depCopyFileSystemAsset && isDisted(dir)
  ) {
    dir.findFiles().foreach { f =>
      atomicallyDepCopyFileIfMissing(f)
    }
    localize(dir, depCopyPath)
  } else dir
}
