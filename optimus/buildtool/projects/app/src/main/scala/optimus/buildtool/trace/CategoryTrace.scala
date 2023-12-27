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
package optimus.buildtool.trace

import java.net.URL
import optimus.buildtool.artifacts.CachedArtifactType
import optimus.buildtool.builders.postinstallers.uploaders.UploadLocation
import optimus.buildtool.cache.silverking.ClusterType
import optimus.buildtool.config.MetaBundle
import optimus.buildtool.config.ScopeId

import scala.collection.mutable

private[buildtool] sealed trait CategoryTrace {

  /** A CategoryTrace for which we want to warn if more than one is recorded for a given scope. */
  def isSingleton = false

  /** A CategoryTrace for which it's a high priority to warn if more than one is recorded for a given scope. */
  def isStrictSingleton = false

  /** A CategoryTrace for which the task executes asynchronously. */
  def isAsync = false

  /** A CategoryTrace for which the task executes exclusively on a single thread */
  def isSingleThreaded = false

  protected def lower(clazz: Class[_]): String = CategoryTrace.lower(clazz)

  lazy val categoryName: String = lower(getClass)

  lazy val name: String = categoryName

  // If defined, it is used for finer granularity reporting in breadcrumbs
  // (e.g.: which post-install-app ran the longest, the upload to which host took the most)
  def scenario(scopeId: ScopeId): Option[String] = None
}

object CategoryTrace {
  // convert "CamelCase" to "camel-case" because it looks nicer in the build diagnostics
  def lower(clazz: Class[_]): String =
    clazz.getSimpleName.replaceAll("([a-z])([A-Z])", "$1-$2").toLowerCase.stripSuffix("$")
}

private[buildtool] sealed trait AsyncCategoryTrace extends CategoryTrace { override def isAsync = true }

private[buildtool] sealed trait SingleThreadedCategoryTrace extends CategoryTrace {
  override def isSingleThreaded = true
}

private[buildtool] sealed trait SingletonCategoryTrace extends CategoryTrace { override def isSingleton = true }

private[buildtool] sealed trait StrictSingletonCategoryTrace extends SingletonCategoryTrace {
  override def isStrictSingleton = true
}

/** A CategoryTrace that can produce CompilerMessages. */
private[buildtool] sealed trait MessageTrace extends CategoryTrace

object MessageTrace {
  private val parseMap = new mutable.HashMap[String, MessageTrace]
  private def add(c: MessageTrace): Unit = {
    parseMap += (c.name -> c)
  }
  def parse(name: String): MessageTrace = parseMap(name)

  add(LoadConfig)
  add(GenerateSource)
  add(Signatures)
  add(Outline)
  add(Scala)
  add(Java)
  add(Jmh)
  add(Runconf)
  add(ProcessScope)
  add(RegexCodeFlagging)
}

private[buildtool] sealed trait ResolveTrace extends SingletonCategoryTrace

object ResolveTrace {
  private val parseMap = new mutable.HashMap[String, ResolveTrace]
  private def add(c: ResolveTrace): Unit = {
    parseMap += (c.name -> c)
  }
  def parse(name: String): ResolveTrace = parseMap(name)

  add(CompileResolve)
  add(CompileOnlyResolve)
  add(RuntimeResolve)
}

trait ClusterTrace extends CategoryTrace {
  def clusterType: ClusterType
  override lazy val categoryName: String =
    if (clusterType == ClusterType.QA) lower(getClass) else s"${lower(getClass)}-${clusterType.toString.toLowerCase}"
}

private[buildtool] sealed trait CacheTrace extends ClusterTrace with AsyncCategoryTrace {
  def tpe: CacheTraceType

  override lazy val name: String = s"$categoryName($tpe)"
}

trait MetaBundleTrace extends CategoryTrace {
  val metaBundle: MetaBundle
  final def meta: String = metaBundle.meta
  final def bundle: String = metaBundle.bundle
  override lazy val name: String = s"$categoryName($meta.$bundle)"
}

// OBT configuration
private[buildtool] case object LoadStratoConfig extends MessageTrace
private[buildtool] case object LoadConfig extends MessageTrace
private[buildtool] case object ReadObtFiles extends MessageTrace
private[buildtool] case object FindObtFiles extends MessageTrace

// Workspace structure builder
private[buildtool] case object BuildWorkspaceStructure extends MessageTrace
private[buildtool] case object BuildDependenciesStructure extends MessageTrace
private[buildtool] case object ResolveScopeStructure extends MessageTrace
private[buildtool] case object HashWorkspaceStructure extends MessageTrace

private[buildtool] sealed trait SyncTrace extends CategoryTrace
private[buildtool] case object BuildTargets extends SyncTrace
private[buildtool] case object BuildTargetSources extends SyncTrace
private[buildtool] case object BuildTargetScalacOptions extends SyncTrace
private[buildtool] case object BuildTargetPythonOptions extends SyncTrace

private[buildtool] case object Build extends SingletonCategoryTrace

private[buildtool] case object Queue extends SingletonCategoryTrace with AsyncCategoryTrace
private[buildtool] case object MemQueue extends SingletonCategoryTrace with AsyncCategoryTrace

private[buildtool] case object GenerateSource extends MessageTrace

private[buildtool] case object Signatures
    extends StrictSingletonCategoryTrace
    with MessageTrace
    with SingleThreadedCategoryTrace
private[buildtool] case object Outline
    extends StrictSingletonCategoryTrace
    with MessageTrace
    with SingleThreadedCategoryTrace
private[buildtool] case object Scala
    extends StrictSingletonCategoryTrace
    with MessageTrace
    with SingleThreadedCategoryTrace
private[buildtool] case object Java
    extends StrictSingletonCategoryTrace
    with MessageTrace
    with SingleThreadedCategoryTrace
private[buildtool] case object Cpp extends StrictSingletonCategoryTrace with MessageTrace
private[buildtool] case object Web extends StrictSingletonCategoryTrace with MessageTrace
private[buildtool] case object Electron extends StrictSingletonCategoryTrace with MessageTrace
private[buildtool] case object Python extends StrictSingletonCategoryTrace with MessageTrace
private[buildtool] case object RegexCodeFlagging extends SingletonCategoryTrace with MessageTrace
private[buildtool] case object Jmh
    extends StrictSingletonCategoryTrace
    with MessageTrace
    with SingleThreadedCategoryTrace
private[buildtool] case object Runconf extends SingletonCategoryTrace with MessageTrace
private[buildtool] case object Pathing extends SingletonCategoryTrace with MessageTrace
private[buildtool] case object Validation extends SingletonCategoryTrace with MessageTrace

private[buildtool] case object ProcessScope extends MessageTrace

private[buildtool] sealed trait CacheTraceType
private[buildtool] final case class ArtifactCacheTraceType(tpe: CachedArtifactType) extends CacheTraceType {
  override def toString: String = CategoryTrace.lower(tpe.getClass)
}
private[buildtool] final case class RemoteAssetCacheTraceType(url: URL) extends CacheTraceType {
  override def toString: String = url.getPath
}

private[buildtool] final case class ConnectCache(clusterType: ClusterType) extends ClusterTrace with MessageTrace
private[buildtool] final case class KeyQuery(clusterType: ClusterType, tpe: CacheTraceType) extends CacheTrace
private[buildtool] final case class Query(clusterType: ClusterType, tpe: CacheTraceType) extends CacheTrace
private[buildtool] final case class Fetch(clusterType: ClusterType, tpe: CacheTraceType) extends CacheTrace
private[buildtool] final case class Put(clusterType: ClusterType, tpe: CacheTraceType) extends CacheTrace

// Deliberately lowercase 'k' here so that it's pretty-printed as "silverking-operation"
private[buildtool] final case class SilverkingOperation(clusterType: ClusterType, requestId: String)
    extends ClusterTrace
    with AsyncCategoryTrace {
  override lazy val name: String = s"$categoryName($requestId)"
}

private[buildtool] case object ScanFilesystem extends CategoryTrace
private[buildtool] case object TidyRubbish extends CategoryTrace
private[buildtool] case object MarkRecentArtifacts extends CategoryTrace
private[buildtool] case object BackgroundCommand extends SingletonCategoryTrace with AsyncCategoryTrace
private[buildtool] case object HashSources extends SingletonCategoryTrace
private[buildtool] case object HashResources extends SingletonCategoryTrace
private[buildtool] case object HashArchiveContents extends SingletonCategoryTrace
private[buildtool] case object Sources extends SingletonCategoryTrace
private[buildtool] case object Resources extends SingletonCategoryTrace
private[buildtool] case object WarSources extends SingletonCategoryTrace
private[buildtool] case object GenericFiles extends SingletonCategoryTrace
private[buildtool] case object InitializeScope extends CategoryTrace
private[buildtool] case object CompileResolve extends ResolveTrace
private[buildtool] case object CompileOnlyResolve extends ResolveTrace
private[buildtool] case object RuntimeResolve extends ResolveTrace
private[buildtool] case object DepCopy extends CategoryTrace
private[buildtool] case object DepCopyFromRemote extends CategoryTrace
private[buildtool] case object CheckRemoteUrl extends CategoryTrace
private[buildtool] case object InstallJar extends CategoryTrace
private[buildtool] case object InstallSourceJar extends CategoryTrace
private[buildtool] case object InstallPathingJar extends CategoryTrace
private[buildtool] case object InstallArchive extends CategoryTrace
private[buildtool] case object InstallCpp extends CategoryTrace
private[buildtool] case object InstallIvyFile extends CategoryTrace
private[buildtool] case object InstallPomFile extends CategoryTrace
private[buildtool] case object InstallGenericFiles extends CategoryTrace
private[buildtool] case object InstallProcessedFiles extends CategoryTrace
private[buildtool] case object InstallElectronFiles extends CategoryTrace
private[buildtool] final case class InstallBuildPropertiesFiles(metaBundle: MetaBundle) extends MetaBundleTrace
private[buildtool] final case class InstallBundleJar(metaBundle: MetaBundle) extends MetaBundleTrace
private[buildtool] final case class InstallRunconfs(metaBundle: MetaBundle) extends MetaBundleTrace
private[buildtool] final case class InstallVersionJar(metaBundle: MetaBundle) extends MetaBundleTrace
private[buildtool] final case class InstallWorkspace(metaBundle: MetaBundle) extends MetaBundleTrace
private[buildtool] final case class InstallLoadState(metaBundle: MetaBundle) extends MetaBundleTrace
private[buildtool] final case class InstallSaveState(metaBundle: MetaBundle) extends MetaBundleTrace
private[buildtool] case object InstallApplicationScripts extends SingletonCategoryTrace
private[buildtool] case object CopyFiles extends SingletonCategoryTrace
private[buildtool] case object SourceSync extends CategoryTrace
private[buildtool] case object StoreMappings extends CategoryTrace
private[buildtool] final case class Upload(id: String, location: UploadLocation) extends AsyncCategoryTrace {
  override lazy val name: String = s"$categoryName($id, $location)"
  // no need to track scopeId here: all uploads are associated to RootScopeId
  override def scenario(scopeId /* ignored */: ScopeId): Option[String] = location match {
    case UploadLocation.Local(_, _)        => Some(id)
    case UploadLocation.Remote(host, _, _) => Some(s"${id}_$host")
  }
}
private[buildtool] final case class PostInstallApp(appName: String) extends AsyncCategoryTrace {
  override lazy val name: String = s"$categoryName($appName)"
  override def scenario(scopeId: ScopeId): Option[String] = Some(s"${categoryName}_$scopeId-$appName")
}
private[buildtool] final case class OpenApiApp(appName: String) extends AsyncCategoryTrace {
  override lazy val name: String = s"$categoryName($appName)"
  override def scenario(scopeId: ScopeId): Option[String] = Some(s"${categoryName}_$scopeId-$appName")
}
private[buildtool] final case class MavenApp(appName: String) extends AsyncCategoryTrace {
  override lazy val name: String = s"$categoryName($appName)"
  override def scenario(scopeId: ScopeId): Option[String] = Some(s"${categoryName}_$scopeId-$appName")
}
private[buildtool] object DockerCommand extends AsyncCategoryTrace {
  override def scenario(scopeId: ScopeId): Option[String] = Some(s"${categoryName}_$scopeId")
}
private[buildtool] object NpmCommand extends AsyncCategoryTrace {
  override def scenario(scopeId: ScopeId): Option[String] = Some(s"${categoryName}_$scopeId")
}

private[buildtool] case object GitDiff extends SingletonCategoryTrace

trait TraceFilter {
  import TraceFilter._
  def include(category: CategoryTrace): Boolean = category != Build && !category.isInstanceOf[SilverkingOperation]
  def publish(category: CategoryTrace): FilterResult
}

object TraceFilter {
  sealed trait FilterResult
  case object Include extends FilterResult
  case object Exclude extends FilterResult
  case object WhenFileModified extends FilterResult
  case object WhenLineModified extends FilterResult
  case object InTargetModule extends FilterResult

  // Kinds of trace filters
  final case class InclusionFilter(categories: CategoryTrace*) extends TraceFilter {
    override def include(category: CategoryTrace): Boolean = categories.contains(category)
    override def publish(category: CategoryTrace): TraceFilter.FilterResult = TraceFilter.Include
  }

  case object ModifiedFilesFilter extends TraceFilter {
    override def publish(category: CategoryTrace): FilterResult = WhenLineModified
  }

  case object TargetScopeFilter extends TraceFilter {
    override def publish(category: CategoryTrace): FilterResult =
      if (category == RegexCodeFlagging) WhenFileModified else InTargetModule
  }

  case object AllWarningsFilter extends TraceFilter {
    override def publish(category: CategoryTrace): FilterResult =
      if (category == RegexCodeFlagging) WhenFileModified else TraceFilter.Include
  }

  def parse(opt: Option[String]): Option[TraceFilter] = {
    opt match {
      case Some("all")      => Some(AllWarningsFilter)
      case Some("modified") => Some(ModifiedFilesFilter)
      case Some("target")   => Some(TargetScopeFilter)
      case _                => None
    }
  }

}
