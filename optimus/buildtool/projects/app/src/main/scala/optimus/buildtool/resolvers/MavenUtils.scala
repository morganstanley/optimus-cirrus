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

import coursier.core._
import optimus.buildtool.artifacts.CompilationMessage
import optimus.buildtool.cache.RemoteAssetStore
import optimus.buildtool.config.DependencyDefinition
import optimus.buildtool.config.DependencyDefinition.DefaultConfiguration
import optimus.buildtool.config.NamingConventions._
import optimus.buildtool.files.BaseHttpAsset
import optimus.buildtool.files.Directory
import optimus.buildtool.files.FileAsset
import optimus.buildtool.files.JarHttpAsset
import optimus.buildtool.resolvers.HttpResponseUtils._
import optimus.buildtool.trace.ObtTrace
import optimus.buildtool.utils.AssetUtils.isJarReadable
import optimus.buildtool.utils.Hashing.hashFileContent
import optimus.exceptions.RTException
import optimus.platform._

import java.io.FileNotFoundException
import java.io.InputStream
import java.net.URI
import java.net.URL
import java.nio.file.Files
import java.nio.file.Path
import java.security.InvalidParameterException
import scala.util.control.NonFatal

@entity private[buildtool] object MavenUtils {
  private[buildtool] val WrongCredMsg = "Credential is invalid!"
  private[buildtool] val strictTransitiveVerification =
    Option(System.getProperty("optimus.buildtool.strictTransitiveVerification")).exists(_.toBoolean)

  // may make these be configurable in future
  private[buildtool] val maxRetry = 1
  private[buildtool] val maxDownloadSeconds = 300
  private[buildtool] val maxSrcDocDownloadSeconds = 10
  private[buildtool] val retryIntervalSeconds = 10

  def isNonUnzipLibJarFile(urlStr: String): Boolean =
    UnzipMavenRepoExts.forall(ext => !urlStr.contains(s".$ext!/")) && !isSrcOrDocFile(urlStr) && urlStr.endsWith(JarKey)

  def invalidUrlMsg(url: URL, e: Throwable): String = s"${e.toString.replaceAll(url.toString, "")} $url"

  def getUrlFileName(url: URL): String = {
    val urlStr = url.toString
    urlStr.substring(urlStr.lastIndexOf("/") + 1, urlStr.length)
  }

  // be used by Coursier maven repo .pom files fetch and our manually downloads: 1. unzipRepo 2. maven javadoc & sources
  @node def downloadUrl[T](
      url: URL,
      depCopier: DependencyCopier,
      remoteAssetStore: RemoteAssetStore,
      isMarker: Boolean = false,
      timeoutSec: Int = maxDownloadSeconds,
      retryIntervalSeconds: Int = retryIntervalSeconds)(f: NodeFunction1[FileAsset, T])(r: String => T): T = {
    val fileUrl = if (isMarker) new URI(url.toString + ".marker").toURL else url
    val urlAsset = BaseHttpAsset.httpAsset(fileUrl)

    if (isMarker || checkLocalDisk(url, depCopier))
      f(depCopier.atomicallyCopyOverHttp(urlAsset, retryIntervalSeconds, remoteAssetStore.cacheMode, isMarker))
    else if (checkRemoteStore(url, remoteAssetStore)) {
      val depCopiedLocation = depCopier.httpLocalAsset(urlAsset)
      remoteAssetStore.get(url, depCopiedLocation) match {
        case Some(file) =>
          log.debug(
            s"Downloaded http file from remote cache to: ${file.path} ${getLocalChecksumStr(file.path, getSha256(file.path))}")
          f(file)
        case None =>
          log.debug(s"Failed load url from remote cache, try download it now: $url")
          f(depCopier.atomicallyCopyOverHttp(urlAsset, retryIntervalSeconds, remoteAssetStore.cacheMode, isMarker))
      }
    } else { // check url, only download it when valid
      val (durationInNanos, (downloadResult, urlCode, isSuccess)) = AdvancedUtils.timed {
        val probingHttpResponse = depCopier.getHttpAssetResponse(urlAsset, timeoutSec)
        probingHttpResponse.inputStream match {
          case Right(stream) =>
            NodeTry {
              (
                f(
                  depCopier
                    .atomicallyCopyOverHttp(
                      urlAsset,
                      retryIntervalSeconds,
                      remoteAssetStore.cacheMode,
                      isMarker,
                      Some(probingHttpResponse))),
                probingHttpResponse.headerInfo.code,
                true)
            } getOrRecover { case e @ RTException =>
              (r(e.toString), probingHttpResponse.headerInfo.code, false)
            }
          case Left(e) => (r(e.toString), probingHttpResponse.headerInfo.code, false)
        }
      }
      if (isSuccess) // add .pom/-sources.jar/-javadoc.jar/unzip.jar downloads trace
        DependencyDownloadTracker.addDownloadDuration(url.toString, durationInNanos)
      else
        DependencyDownloadTracker.addFailedDownloadDuration(s"${url.toString}, reason: $urlCode", durationInNanos)

      downloadResult
    }
  }

  @node def checkRemoteStore(url: URL, remoteAssetStore: RemoteAssetStore): Boolean = remoteAssetStore.check(url)

  @node def checkLocalDisk(url: URL, depCopier: DependencyCopier): Boolean = {
    try { Files.exists(depCopier.httpLocalAsset(BaseHttpAsset.httpAsset(url)).path) }
    catch {
      case NonFatal(e) =>
        log.warn(s"Check depCopied url in local disk failed! $e, $url")
        false
    }
  }

  def checkLocalDisk(localAsset: FileAsset): Boolean = try { Files.exists(localAsset.path) }
  catch {
    case NonFatal(e) =>
      log.warn(s"Check asset in local disk failed! $e, ${localAsset.pathString}")
      false
  }

  def httpLocalAsset(httpAsset: BaseHttpAsset, depCopyPath: Directory): httpAsset.LocalAsset = {
    val localRoot = depCopyPath
    val urlFilePath =
      if (UnzipMavenRepoExts.exists(ext => httpAsset.url.toString.contains(s"$ext!")))
        httpAsset.url.getPath.stripPrefix("/").replaceAll("!", "/")
      else httpAsset.url.getPath.stripPrefix("/")
    httpAsset.asLocal(
      localRoot.path
        .resolve(httpAsset.url.getProtocol)
        .resolve(httpAsset.url.getHost)
        .resolve(urlFilePath))
  }

  def getLocal(httpAsset: JarHttpAsset, depCopyRoot: Directory): Option[FileAsset] = {
    val localAsset = httpLocalAsset(httpAsset, depCopyRoot)
    Some(localAsset).filter(checkLocalDisk)
  }

  def getMappedMavenDeps(
      org: String,
      name: String,
      configuration: String,
      afsToMavenMap: Map[MappingKey, Seq[DependencyDefinition]]): Option[Seq[DependencyDefinition]] = {
    // if ivy configuration-level mapping not exists and strictTransitiveVerification is disabled,
    // then automatically apply name-level mapping  example:
    //   for mapping rule "afs.bar -> maven.bar" ("afs.bar" .ivy defined configuration named "afs.bar.core")
    //   when "afs.foo" depends on "afs.bar.core", we will map "afs.bar.core" to "maven.bar" transitively
    //   and output "afs.foo -> maven.bar"
    val configLevelMapping = MappingKey(org, name, configuration)
    val nameLevelMapping = MappingKey(org, name, DefaultConfiguration)

    val configLevelResult = afsToMavenMap.get(configLevelMapping)
    if (strictTransitiveVerification && !Set("*", "runtime", "default").contains(configuration)) configLevelResult
    else configLevelResult.orElse(afsToMavenMap.get(nameLevelMapping))
  }

  @node def applyDirectMapping(
      dep: DependencyDefinition,
      afsToMavenMap: Map[MappingKey, Seq[DependencyDefinition]]
  ): Either[CompilationMessage, Seq[DependencyDefinition]] =
    getMappedMavenDeps(dep.group, dep.name, dep.configuration, afsToMavenMap).toRight {
      CompilationMessage.error(
        s"""Could not find ivy mapping for configuration `${dep.configuration}`
           |Consider adding it to `ivyConfigurations` key for AFS dependency `${dep.group}.${dep.name}` (line ${dep.line})""".stripMargin
      )
    }

  def applyTransitiveMapping(
      org: String,
      name: String,
      configuration: String,
      attr: Seq[(String, String)],
      afsToMavenMap: Map[MappingKey, Seq[DependencyDefinition]]
  ): Option[Seq[TransitiveMappedResult]] = {
    getMappedMavenDeps(org, name, configuration, afsToMavenMap).map { deps =>
      deps.map { af =>
        TransitiveMappedResult(
          Module(Organization(af.group), ModuleName(af.name), attr.toMap),
          af.version,
          // use default config to resolve transitive maven dep properly, for no artifact ivy.runtime extends cases
          if (af.configuration.isEmpty) DefaultConfiguration else af.configuration
        )
      }
    }
  }
}

final case class MappingKey(group: String, name: String, configuration: String)

final case class TransitiveMappedResult(module: Module, version: String, configuration: String)

final case class AutoMappedResult(autoAfsToMavenMap: Map[Dependency, Set[Dependency]]) {
  def afs: Set[Dependency] = autoAfsToMavenMap.keySet
}

final case class UnmappedResult(all: Set[Dependency], leafs: Set[Dependency])

final case class DependencyManagementResult(finalDeps: Seq[DependencyDefinition], msgs: Seq[CompilationMessage]) {
  def ++(to: DependencyManagementResult): DependencyManagementResult =
    DependencyManagementResult(this.finalDeps ++ to.finalDeps, this.msgs ++ to.msgs)
}

final case class HttpProbingResponse(inputStream: Either[Exception, InputStream], headerInfo: HttpHeaderInfo)

final case class HttpHeaderInfo(headerMap: Map[String, String]) {
  override def toString: String = headerMap.mkString(", ")

  def code: Option[String] = headerMap.get(ServerCodeKey)
  def keepAlive: Option[String] = headerMap.get("Keep-Alive")
  def connection: Option[String] = headerMap.get("Connection")
  def server: Option[String] = headerMap.get("Server")
  def sha1: Option[String] = headerMap.get("X-Checksum-Sha1")
  def sha256: Option[String] = headerMap.get("X-Checksum-Sha256")
  def md5: Option[String] = headerMap.get("X-Checksum-Md5")
  def eTag: Option[String] = headerMap.get("ETag")
  def lastModified: Option[String] = headerMap.get("Last-Modified")
  def date: Option[String] = headerMap.get("Date")
  def serverId: Option[String] = headerMap.get(MavenServerId)
  def serverNodeId: Option[String] = headerMap.get(MavenServerNodeId)
  def contentDisposition: Option[String] = headerMap.get("Content-Disposition")
  def contentLength: Option[String] = headerMap.get("Content-Length")
  def contentType: Option[String] = headerMap.get("Content-Type")
  def acceptRanges: Option[String] = headerMap.get("Accept-Ranges")
}

object HttpResponseUtils {
  import optimus.buildtool.resolvers.MavenUtils._

  val ServerCodeKey = "null"
  val emptyHttpHeaderInfo: HttpHeaderInfo = HttpHeaderInfo(Map.empty)
  private val LocalSha256 = "localSha256"
  private val RemoteSha256 = "remoteSha256"

  def loadHttpHeaderInfo(header: String, url: URL): HttpHeaderInfo = {
    val loadedMap: Map[String, String] =
      try {
        header
          .replaceAll("[{}\\n]", "")
          .split("],")
          .map { raw =>
            val strs = raw.split("=")
            val (key, rawValue) = (strs.headOption.getOrElse("").trim, strs.lastOption.getOrElse("").trim)
            key -> rawValue.replaceAll("[\\[\\]]", "")
          }
          .toMap
      } catch {
        case NonFatal(e) =>
          val msg = s"Failed to parse http header: $url, $e"
          ObtTrace.warn(msg)
          log.warn(msg)
          Map.empty
      }
    HttpHeaderInfo(loadedMap)
  }

  def getSha256(localPath: Path): String = hashFileContent(FileAsset.apply(localPath)).replaceFirst("HASH", "")

  def getLocalChecksumStr(localPath: Path, checksumStr: String): String = {
    s"Size: ${Files.size(localPath)} bytes, $LocalSha256:$checksumStr"
  }

  def retryWarningMsg(retry: Int, maxRetry: Int, url: URL, exception: Exception, actStr: String): Unit = {
    val msg = s"Retrying $actStr url $retry/$maxRetry: $url, $exception"
    ObtTrace.warn(msg)
    log.warn(msg)
    DependencyDownloadTracker.addRetry(msg)
  }

  def getHttpException(httpAsset: BaseHttpAsset, maxRetry: Int, e: Exception): RuntimeException = {
    if (e.getMessage.contains(" 401")) new InvalidParameterException(s"$WrongCredMsg $e")
    else if (e.getMessage.contains(" 404") || e.isInstanceOf[FileNotFoundException])
      new NoSuchElementException(e.toString)
    else new UnsupportedOperationException(s"${invalidUrlMsg(httpAsset.url, e)}, tried ${maxRetry + 1} times")
  }

  // should exclude javadoc & sources files
  def pathShouldValidate(pathStr: String): Boolean =
    !pathStr.endsWith(s"${Classifier.javadoc.value}.jar") && !pathStr.endsWith(
      s"${Classifier.sources.value}.jar") && pathStr.endsWith(".jar")

  // not all url contain SHA-256 checksum, we should verify the downloaded jar is readable or not
  def validateJarFileReadable(
      httpAsset: BaseHttpAsset,
      localPath: Path,
      headerInfo: HttpHeaderInfo,
      localFileSha256: String): Unit = if (pathShouldValidate(localPath.getFileName.toString)) try {
    val msgPrefix = "Downloaded remote file is"
    val validated = headerInfo.sha256 match {
      case Some(remoteChecksum) => remoteChecksum == localFileSha256
      case None                 => true // skip when remote server not provide
    }
    val msgSuffix =
      s"$localPath, $RemoteSha256: ${headerInfo.sha256.getOrElse("")}, $LocalSha256: $localFileSha256"
    val readable = isJarReadable(localPath)
    if (readable && validated)
      log.debug(s"$msgPrefix valid: $msgSuffix")
    else throw new UnsupportedOperationException(s"$msgPrefix invalid! $msgSuffix")
  } catch {
    case e: Exception =>
      val msg = s"Downloaded remote file broken! from ${httpAsset.url}, $RemoteSha256: ${headerInfo.sha256
          .getOrElse("")}, ${getLocalChecksumStr(localPath, localFileSha256)}, header: $headerInfo, $e"
      ObtTrace.warn(msg)
      log.warn(msg)
      DependencyDownloadTracker.addBrokenFile(httpAsset.url.toString)
      throw e
  }

}
