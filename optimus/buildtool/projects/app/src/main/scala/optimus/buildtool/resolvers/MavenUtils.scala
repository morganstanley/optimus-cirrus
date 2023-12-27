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
import optimus.buildtool.config.MappedDependencyDefinitions
import optimus.buildtool.files.BaseHttpAsset
import optimus.buildtool.files.Directory
import optimus.buildtool.files.FileAsset
import optimus.buildtool.files.JarHttpAsset
import optimus.buildtool.trace.ObtTrace
import optimus.platform._

import java.net.URL
import java.nio.file.Files
import java.security.InvalidParameterException
import scala.util.control.NonFatal

@entity private[buildtool] object MavenUtils {
  private[buildtool] val maxRetry = 1
  private[buildtool] val wrongCredMsg = "Credential is invalid!"

  def invalidUrlMsg(url: URL, e: Throwable): String = s"${e.toString.replaceAll(url.toString, "")} $url"

  @node def downloadUrl[T](url: URL, depCopier: DependencyCopier, isMarker: Boolean = false, retry: Int = maxRetry)(
      f: NodeFunction1[FileAsset, T])(r: String => T): T = {
    val fileUrl = if (isMarker) new URL(url.toString + ".marker") else url
    val urlAsset = BaseHttpAsset.httpAsset(fileUrl)
    depCopier.getHttpStream(urlAsset, retry) match {
      case Left(e) if !isMarker =>
        e match {
          case invalidCredential: InvalidParameterException => r(invalidUrlMsg(url, invalidCredential))
          case noElement: NoSuchElementException            => r(invalidUrlMsg(url, noElement))
          case e: Exception =>
            if (retry > 0) {
              ObtTrace.warn(s"Retrying download url $retry/$maxRetry: $url")
              downloadUrl(url, depCopier, isMarker, retry - 1)(f)(r)
            } else r(s"${invalidUrlMsg(url, e)}, tried ${maxRetry + 1} times")
        }
      case _ =>
        // this atomicallyCopyOverHttp invoke at resolver stage would be cached as @node, which no side-effect and could be
        // reused in compilation stage latter.
        f(depCopier.atomicallyCopyOverHttp(urlAsset, isMarker))
    }
  }

  @node def checkSK(url: URL, remoteAssetStore: RemoteAssetStore): Boolean = remoteAssetStore.check(url)

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
      if (httpAsset.url.toString.contains("tar.gz!")) httpAsset.url.getPath.stripPrefix("/").replaceAll("!", "/")
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

  @node def applyDirectMapping(
      afsToMavenMap: Map[DependencyDefinition, Seq[DependencyDefinition]],
      deps: Seq[DependencyDefinition]
  ): MappedDependencyDefinitions = {
    import scala.collection.immutable.Seq
    import scala.collection.compat._

    if (deps.nonEmpty && afsToMavenMap.nonEmpty) {
      val appliedMap: Map[DependencyDefinition, Seq[DependencyDefinition]] = deps.collect {
        case d if afsToMavenMap.contains(d) =>
          val MavenEquivalents = afsToMavenMap.getOrElse(d, Nil).to(Seq)
          d -> MavenEquivalents
      }.toMap
      val mappedAfsDeps = appliedMap.keys.to(Seq)
      val unMappedAfsDeps = deps.diff(mappedAfsDeps).to(Seq)

      MappedDependencyDefinitions(appliedMap, unMappedAfsDeps)
    } else MappedDependencyDefinitions(Map.empty, deps.to(Seq))
  }

  def applyTransitiveMapping(
      org: String,
      name: String,
      attr: Seq[(String, String)],
      afsToMavenMap: Map[(String, String), Seq[DependencyDefinition]]
  ): Seq[(Module, String)] =
    afsToMavenMap.getOrElse((org, name), Nil).map { af =>
      (Module(Organization(af.group), ModuleName(af.name), attr.toMap), af.version)
    }

}

final case class AutoMappedResult(autoAfsToMavenMap: Map[Dependency, Set[Dependency]]) {
  def afs: Set[Dependency] = autoAfsToMavenMap.keySet
}
final case class UnmappedResult(all: Set[Dependency], leafs: Set[Dependency])
final case class DependencyManagementResult(finalDeps: Seq[DependencyDefinition], msgs: Seq[CompilationMessage]) {
  def ++(to: DependencyManagementResult): DependencyManagementResult =
    DependencyManagementResult(this.finalDeps ++ to.finalDeps, this.msgs ++ to.msgs)
}
