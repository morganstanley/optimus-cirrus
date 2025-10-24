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

import optimus.buildtool.artifacts.Artifact
import optimus.buildtool.artifacts.ClassFileArtifact
import optimus.buildtool.artifacts.ExternalBinaryArtifact
import optimus.buildtool.artifacts.ExternalClassFileArtifact
import optimus.buildtool.artifacts.ExternalHashedArtifact
import optimus.buildtool.cache.CacheMode
import optimus.buildtool.cache.NoOpRemoteAssetStore
import optimus.buildtool.cache.NodeCaching
import optimus.buildtool.cache.RemoteAssetStore
import optimus.buildtool.config.ScopeId.RootScopeId
import optimus.buildtool.files.BaseHttpAsset
import optimus.buildtool.files.Directory
import optimus.buildtool.files.FileAsset
import optimus.buildtool.files.JarAsset
import optimus.buildtool.files.ReactiveDirectory
import optimus.buildtool.resolvers.HttpResponseUtils._
import optimus.buildtool.resolvers.MavenUtils.getHttpAssetResponse
import optimus.buildtool.resolvers.MavenUtils.isNonUnzipLibJarFile
import optimus.buildtool.resolvers.MavenUtils.maxDownloadSeconds
import optimus.buildtool.resolvers.MavenUtils.maxRetry
import optimus.buildtool.resolvers.MavenUtils.retryIntervalSeconds
import optimus.buildtool.trace.DepCopy
import optimus.buildtool.trace.DepCopyFromRemote
import optimus.buildtool.trace.DownloadRemoteUrl
import optimus.buildtool.trace.ObtTrace
import optimus.buildtool.utils.PathUtils.isDisted
import optimus.buildtool.utils.Utils.atomicallyWriteIfMissing
import optimus.buildtool.utils.Utils.localize
import optimus.platform._
import optimus.stratosphere.artifactory.Credential

import java.io.InputStream
import java.net.URL
import java.nio.file.Files
import java.nio.file.Path
import java.util.concurrent.TimeUnit

@entity class DependencyCopier(
    depCopyPath: Directory,
    val credentials: Seq[Credential] = Nil,
    assetStore: RemoteAssetStore = NoOpRemoteAssetStore,
    depCopyFileSystemAsset: Boolean = false
) {

  @node def atomicallyDepCopyArtifactsIfMissing(artifact: Artifact): Artifact = artifact match {
    case c: ExternalClassFileArtifact => atomicallyDepCopyExternalClassFileArtifactsIfMissing(c)
    case b: ExternalBinaryArtifact    => atomicallyDepCopyExternalBinaryArtifactsIfMissing(b)
    case a                            => a
  }

  @node def atomicallyDepCopyExternalArtifactsIfMissing(artifact: ExternalHashedArtifact): ExternalHashedArtifact =
    artifact match {
      case c: ExternalClassFileArtifact => atomicallyDepCopyExternalClassFileArtifactsIfMissing(c)
      case b: ExternalBinaryArtifact    => atomicallyDepCopyExternalBinaryArtifactsIfMissing(b)
      case a                            => a
    }

  @node def atomicallyDepCopyClassFileArtifactsIfMissing(artifact: ClassFileArtifact): ClassFileArtifact =
    artifact match {
      case c: ExternalClassFileArtifact => atomicallyDepCopyExternalClassFileArtifactsIfMissing(c)
      case a                            => a
    }

  @node def atomicallyDepCopyExternalBinaryArtifactsIfMissing(
      artifact: ExternalBinaryArtifact
  ): ExternalBinaryArtifact = artifact.copy(atomicallyDepCopyFileIfMissing(artifact.file))

  @node def atomicallyDepCopyExternalClassFileArtifactsIfMissing(
      artifact: ExternalClassFileArtifact
  ): ExternalClassFileArtifact =
    // n.b. we never extract signature jars since they specifically want classes
    artifact.copyWithUpdatedAssets(
      file = atomicallyDepCopyJarIfMissing(artifact.file),
      source = artifact.source.flatMap(tryAtomicallyDepCopyJarIfMissing(_).toOption),
      javadoc = artifact.javadoc.flatMap(tryAtomicallyDepCopyJarIfMissing(_).toOption)
    )

  @node def atomicallyDepCopyJarIfMissing(depFile: FileAsset): JarAsset =
    tryAtomicallyDepCopyJarIfMissing(depFile).get

  @node private def tryAtomicallyDepCopyJarIfMissing(depFile: FileAsset): NodeTry[JarAsset] = NodeTry {
    atomicallyDepCopyFileIfMissing(depFile).asJar
  }

  @node def atomicallyDepCopyFileIfMissing(depFile: FileAsset): FileAsset = depFile match {
    case http: BaseHttpAsset => atomicallyCopyOverHttp(http, retryIntervalSeconds, assetStore.cacheMode)
    case d                   => atomicallyCopyOverAfs(d)
  }

  def httpLocalAsset(httpAsset: BaseHttpAsset): httpAsset.LocalAsset =
    MavenUtils.httpLocalAsset(httpAsset, depCopyPath)

  private def skipCopyMissingFileMsg(depFile: FileAsset, reason: String = ""): FileAsset = {
    val reasonMsg = if (reason.nonEmpty) s", $reason" else ""
    val msg = s"Not depcopying missing file: ${depFile.pathString}$reasonMsg"
    ObtTrace.warn(msg)
    depFile
  }

  private def downloadUrlFromStream(
      httpAsset: BaseHttpAsset,
      localPath: Path,
      stream: InputStream,
      headerInfo: HttpHeaderInfo,
      retry: Int,
      retryIntervalSeconds: Int): Unit =
    ObtTrace.traceTask(RootScopeId, DownloadRemoteUrl(httpAsset.url)) {
      try {
        Files.createDirectories(localPath.getParent)
        Files.copy(stream, localPath)
        val downloadedFileChecksum = getSha256(localPath)
        log.debug(
          s"Downloaded remote url ${httpAsset.uriString} to local disk, ${getLocalChecksumStr(localPath, downloadedFileChecksum)}")
        // check .jar file
        validateJarFileReadable(httpAsset, localPath, headerInfo, downloadedFileChecksum)
      } catch {
        case e: Exception =>
          if (retry > 0) {
            stream.close()
            Files.deleteIfExists(localPath)
            retryWarningMsg(retry, maxRetry, httpAsset.url, e, "download")
            Thread.sleep(TimeUnit.SECONDS.toMillis(retryIntervalSeconds))
            downloadHttpAsset(httpAsset, localPath, None, retry - 1, retryIntervalSeconds)
          } else throw getHttpException(httpAsset, maxRetry, e)
      } finally stream.close()
    }

  private def downloadHttpAsset(
      httpAsset: BaseHttpAsset,
      localPath: Path,
      openedHttp: Option[HttpProbingResponse],
      retry: Int,
      retryIntervalSeconds: Int): Unit = {
    val workingHttpResponse = openedHttp.getOrElse(getHttpAssetResponse(httpAsset, maxDownloadSeconds, credentials))
    workingHttpResponse.inputStream match {
      case Right(stream) =>
        downloadUrlFromStream(httpAsset, localPath, stream, workingHttpResponse.headerInfo, retry, retryIntervalSeconds)
      case Left(unableGetHttpStream) =>
        skipCopyMissingFileMsg(httpAsset, unableGetHttpStream.toString)
        throw unableGetHttpStream
    }
  }

  @node def atomicallyCopyOverHttp(
      httpAsset: BaseHttpAsset,
      retryIntervalSeconds: Int,
      cacheMode: CacheMode,
      isMarker: Boolean = false,
      openedHttp: Option[HttpProbingResponse] = None,
  ): httpAsset.LocalAsset = {
    // even if depCopy is disabled we still need to copy HTTP Jars locally, so use tempDir instead
    val localFile = httpLocalAsset(httpAsset)

    val fetchedFromMaven = atomicallyWriteIfMissing(localFile) { tmp =>
      ObtTrace.traceTask(RootScopeId, DepCopyFromRemote) {
        if (assetStore.get(httpAsset.url, FileAsset(tmp)).isDefined) {
          val downloadedFileChecksum = getSha256(tmp)
          log.debug(
            s"Downloaded http file from remote cache to: $tmp ${getLocalChecksumStr(tmp, downloadedFileChecksum)}")
          false // false because we fetched from remote cache
        } else if (isMarker) {
          try { // put empty marker url file in local disk
            Files.createDirectories(tmp.getParent)
            Files.createFile(tmp)
          } catch { case e: Exception => log.warn(s"Create marker url file failed! $e") }
          true
        } else {
          val (durationInNanos, _) = AdvancedUtils.timed {
            downloadHttpAsset(httpAsset, tmp, openedHttp, maxRetry, retryIntervalSeconds)
          }
          // add non-unzip repo lib .jar download trace
          val urlStr = httpAsset.url.toString
          if (isNonUnzipLibJarFile(urlStr)) DependencyDownloadTracker.addDownloadDuration(urlStr, durationInNanos)

          DependencyDownloadTracker.addHttpFile(urlStr)
          true
        }
      }
    }

    // if we fetched from maven then the artifact wasn't in remote cache (modulo a small race, but re-putting is safe)
    // so we should put it. Note that we do this using the final localFile name, not the tmp file, because the put
    // is async and the tmp file is often removed by the point where the put actually reads the file.
    val shouldWrite = fetchedFromMaven.contains(true) || cacheMode.forceWrite
    val canWrite = assetStore != NoOpRemoteAssetStore && cacheMode.canWrite
    if (shouldWrite && canWrite) DependencyCopier.putFileIntoRemoteCache(assetStore, httpAsset.url, localFile)

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

@entity object DependencyCopier {
  // extracted as a separate node so that we cache the side-effect of uploading and avoid re-uploading redundantly
  // from the same process even when in forceWrite mode
  @node def putFileIntoRemoteCache(assetStore: RemoteAssetStore, url: URL, localFile: FileAsset): Unit = {
    assetStore.put(url, localFile)
    log.debug(
      s"Uploaded http file to remote cache: $url, ${getLocalChecksumStr(localFile.path, getSha256(localFile.path))}")
  }

  putFileIntoRemoteCache_info.setCustomCache(NodeCaching.uploadCache)
}
