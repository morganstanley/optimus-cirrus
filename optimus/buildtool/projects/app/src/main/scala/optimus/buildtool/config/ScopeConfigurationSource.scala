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
package optimus.buildtool.config

import optimus.buildtool.files.Directory
import optimus.buildtool.format.PostInstallApp
import optimus.buildtool.utils.TypeClasses._
import optimus.platform._

import scala.util.Failure
import scala.util.Success
import scala.util.Try
import scala.collection.immutable.Seq
import scala.collection.immutable.Set

/**
 * A source of dependency configuration information, i.e. which scopes depend on which other scopes or libraries
 *
 * Doesn't tell us anything about how to actually do the build
 */
@entity private[buildtool] trait ScopeConfigurationSource {
  @node def compilationScopeIds: Set[ScopeId]
  @node def compilationBundles: Set[MetaBundle] = compilationScopeIds.map(_.metaBundle)
  @node def scope(id: String): ScopeId

  @node def metaBundle(id: String): MetaBundle
  @node def resolveScopes(partialId: String): Set[ScopeId]
  @node def tryResolveScopes(partialId: String): Option[Set[ScopeId]]
  @node def local(id: ScopeId): Boolean
  @node def scopeConfiguration(id: ScopeId): ScopeConfiguration
  @node def jarConfiguration(id: ScopeId, versionConfig: VersionConfiguration): JarConfiguration
  @node def copyFilesConfiguration(id: ScopeId): Option[CopyFilesConfiguration]
  def archiveConfiguration(id: ScopeId): Option[ArchiveConfiguration]
  @node def extensionConfiguration(id: ScopeId): Option[ExtensionConfiguration]
  @node def postInstallApps(id: ScopeId): Seq[Set[PostInstallApp]]
  @node def root(id: ParentId): Directory
  @node def pathingBundles(ids: Set[ScopeId]): Set[ScopeId]
  @node def includeInClassBundle(id: ScopeId): Boolean
}

object ScopeConfigurationSourceBase {
  private def str(field: Option[String]): String = field match {
    case Some(s) => s"'$s'"
    case None    => "<any>"
  }
}

@entity private[buildtool] trait ScopeConfigurationSourceBase extends ScopeConfigurationSource {
  import ScopeConfigurationSourceBase._

  @node override def scope(id: String): ScopeId = id match {
    case ScopeIdString(scopeId) if compilationScopeIds.contains(scopeId) =>
      scopeId
    case ScopeIdString(scopeId) =>
      throw new IllegalArgumentException(s"Unknown scope: '$scopeId'")
    case _ =>
      throw new IllegalArgumentException(s"Invalid scope: '$id'")
  }

  @node override def tryResolveScopes(partialId: String): Option[Set[ScopeId]] = {
    val Seq(meta, bundle, module, tpe) = RelaxedIdString.parts(partialId)

    val scopes = compilationScopeIds
      .fieldFilter(_.meta, meta)
      .fieldFilter(_.bundle, bundle)
      .fieldFilter(_.module, module)
      .fieldFilter(_.tpe, tpe)

    if (scopes.isEmpty) None else Some(scopes)
  }

  @node override def resolveScopes(partialId: String): Set[ScopeId] = {
    val Seq(meta, bundle, module, tpe) = RelaxedIdString.parts(partialId)
    val scopes = tryResolveScopes(partialId)

    scopes.getOrElse {
      throw new IllegalArgumentException(
        s"No scope(s) found matching partial ID '$partialId'" +
          s" (meta: ${str(meta)}, bundle: ${str(bundle)}, module: ${str(module)}, type: ${str(tpe)})")
    }
  }

  @node override def metaBundle(id: String): MetaBundle =
    Try(MetaBundle.parse(id)) match {
      case Success(bundle) if compilationBundles.contains(bundle) => bundle
      case Success(bundle) => throw new IllegalArgumentException(s"Unknown bundle: '$bundle'")
      case Failure(ex)     => throw new IllegalArgumentException(s"Invalid bundle: '$id'", ex)
    }

  @node private def compilationBundleScopeIds: Set[ScopeId] =
    compilationScopeIds.apar.filter(id => scopeConfiguration(id).pathingBundle)

  @node override def pathingBundles(compiledIds: Set[ScopeId]): Set[ScopeId] = {
    val compiledMetaBundles = compiledIds.map(_.metaBundle)
    compilationBundleScopeIds.filter(bundleScope => compiledMetaBundles.contains(bundleScope.metaBundle))
  }
}
