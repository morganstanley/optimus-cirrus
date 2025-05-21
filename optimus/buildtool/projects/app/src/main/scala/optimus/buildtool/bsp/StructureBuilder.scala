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
package optimus.buildtool
package bsp

import optimus.buildtool.artifacts.Artifact
import optimus.buildtool.artifacts.ArtifactType
import optimus.buildtool.artifacts.ExternalClassFileArtifact
import optimus.buildtool.artifacts.FingerprintArtifact
import optimus.buildtool.artifacts.MessagesArtifact
import optimus.buildtool.builders.StandardBuilder
import optimus.buildtool.config._
import optimus.buildtool.files.LocalDirectoryFactory
import optimus.buildtool.resolvers.DependencyCopier
import optimus.buildtool.resolvers.DependencyMetadataResolver
import optimus.buildtool.rubbish.ArtifactRecency
import optimus.buildtool.scope.FingerprintHasher
import optimus.buildtool.trace.BuildWorkspaceStructure
import optimus.buildtool.trace.HashWorkspaceStructure
import optimus.buildtool.trace.ObtTrace
import optimus.buildtool.trace.ResolveScopeStructure
import optimus.buildtool.utils.Hashing
import optimus.platform._
import optimus.scalacompat.collection._
import com.github.plokhotnyuk.jsoniter_scala.core._

import scala.collection.compat._
import scala.collection.immutable.Seq

@entity class StructureHasher(
    hasher: FingerprintHasher,
    underlyingBuilder: NodeFunction0[StandardBuilder],
    scalaVersionConfig: NodeFunction0[ScalaVersionConfig],
    pythonEnabled: NodeFunction0[Boolean],
    depMetadataResolvers: NodeFunction0[Seq[DependencyMetadataResolver]]) {
  import optimus.buildtool.artifacts.JsonImplicits.{scalaVersionJsonValueCodec, scopeConfigurationJsonValueCodec}

  @node private def scopeConfigSource = underlyingBuilder().factory.scopeConfigSource

  @node private def scalaFp: String = {
    val scalaConfig = scalaVersionConfig()
    "scala fingerprint:" + Hashing.hashBytes(
      writeToArray(scalaConfig)
    ) // TODO (OPTIMUS-47169): to be really efficient we could write to a custom output stream which hashes the bytes as they arrive and then throws them away
  }

  @node private def pyEnabledFp: String = {
    val pythonEnabledAsBoolean = pythonEnabled()
    "python enabled: " + Hashing.hashString(pythonEnabledAsBoolean.toString)
  }

  @node private def singleScopeFp(id: ScopeId): String = {
    val config = scopeConfigSource.scopeConfiguration(id)
    id.properPath + ":" + Hashing.hashBytes(writeToArray(config))
  }

  @node private def scopesFp: Seq[String] =
    scopeConfigSource.compilationScopeIds.toIndexedSeq
      .sortBy(_.properPath)
      .apar
      .map(singleScopeFp)

  @node private def depMetadataResolversFp: String =
    "resolvers fingerprint:" + Hashing.hashStrings(depMetadataResolvers().flatMap(_.fingerprint).sorted)

  /**
   * Compute a hash for the workspace from the scope configurations.
   */
  @node def hash: FingerprintArtifact = ObtTrace.traceTask(ScopeId.RootScopeId, HashWorkspaceStructure) {
    val (scala, py, resolvers, scopes) = apar(scalaFp, pyEnabledFp, depMetadataResolversFp, scopesFp)
    val configFingerprint =
      hasher.hashFingerprint(Seq(scala, py, resolvers) ++ scopes, ArtifactType.StructureFingerprint)
    log.debug(s"Hashed config fingerprint: ${configFingerprint.hash}")
    configFingerprint
  }
}

@entity class StructureBuilder(
    underlyingBuilder: NodeFunction0[StandardBuilder],
    scalaVersionConfig: NodeFunction0[ScalaVersionConfig],
    rawPythonEnabled: NodeFunction0[Boolean],
    rawExtractVenvs: NodeFunction0[Boolean],
    directoryFactory: LocalDirectoryFactory,
    dependencyCopier: DependencyCopier,
    structureHasher: StructureHasher,
    recency: Option[ArtifactRecency]
) {

  @node def structure: WorkspaceStructure = ObtTrace.traceTask(ScopeId.RootScopeId, BuildWorkspaceStructure) {
    val builder = underlyingBuilder()
    val rawScalaConfig = scalaVersionConfig()

    checkForErrors(builder.factory.globalMessages)

    // Depcopy all scala lib jars (class, source and javadoc) so they're available for use in the bundle structure.
    // This will generally be a superset of `rawScalaConfig.scalaJars`, which we also depcopy below.
    val scalaLibPath = dependencyCopier.depCopyDirectoryIfMissing(rawScalaConfig.scalaLibPath)
    val scalaJars = rawScalaConfig.scalaJars.apar.map(dependencyCopier.atomicallyDepCopyJarIfMissing)
    val scalaConfig = rawScalaConfig.copy(scalaLibPath = directoryFactory.reactive(scalaLibPath), scalaJars = scalaJars)
    val pythonEnabled = rawPythonEnabled()
    val extractVenvs = rawExtractVenvs()
    val scopeConfigSource = builder.factory.scopeConfigSource

    val scopes: Map[ScopeId, (Seq[Artifact], ResolvedScopeInformation)] =
      ObtTrace.traceTask(ScopeId.RootScopeId, ResolveScopeStructure) {
        scopeConfigSource.compilationScopeIds.apar.flatMap { id =>
          val config = scopeConfigSource.scopeConfiguration(id)
          val local = scopeConfigSource.local(id)
          if (local) {
            val localScopeDeps =
              config.internalCompileDependencies.apar.flatMap(localScopeDependencies(_, scopeConfigSource)).distinct
            val sparseScopeDeps =
              config.internalCompileDependencies.apar.flatMap(sparseScopeDependencies(_, scopeConfigSource)).distinct

            val resolutions = builder.factory
              .lookupScope(id)
              .to(Seq)
              .apar
              .flatMap(_.allCompileDependencies)
              .apar
              .flatMap(_.resolution)

            checkForErrors(resolutions)

            val externalDeps = resolutions.apar.flatMap { resolution =>
              val deps = resolution.result.resolvedArtifacts
              deps.apar.map(dependencyCopier.atomicallyDepCopyExternalClassFileArtifactsIfMissing)
            }
            val scopeInfo = ResolvedScopeInformation(config, local, localScopeDeps, sparseScopeDeps, externalDeps)
            Some(id -> (resolutions, scopeInfo))
          } else None
        }(Map.breakOut)
      }

    val artifacts = scopes.flatMap { case (_, (as, _)) => as }.to(Seq)

    val scopeInfos = scopes.map { case (id, (_, info)) => id -> info }
    val hashedStructureFingerprint = structureHasher.hash

    // mark the artifacts so that we don't garbage collect them on the next bsp run
    recency.foreach(_.markRecent(artifacts :+ hashedStructureFingerprint))

    WorkspaceStructure(hashedStructureFingerprint.hash, scalaConfig, pythonEnabled, extractVenvs, scopeInfos)
  }

  @node private def localScopeDependencies(id: ScopeId, scopeConfigSource: ScopeConfigurationSource): Seq[ScopeId] = {
    if (scopeConfigSource.compilationScopeIds(id)) {
      val config = scopeConfigSource.scopeConfiguration(id)
      if (scopeConfigSource.local(id)) Seq(id)
      else
        config.internalCompileDependencies.apar.flatMap(localScopeDependencies(_, scopeConfigSource)).distinct
    } else Nil
  }

  @node private def sparseScopeDependencies(
      id: ScopeId,
      scopeConfigSource: ScopeConfigurationSource
  ): Seq[ScopeId] = {
    if (scopeConfigSource.compilationScopeIds(id)) {
      val config = scopeConfigSource.scopeConfiguration(id)
      val scopeJar =
        if (scopeConfigSource.local(id)) None
        else Some(id)

      scopeJar.toIndexedSeq ++ config.internalCompileDependencies.apar.flatMap { d =>
        sparseScopeDependencies(d, scopeConfigSource)
      }.distinct
    } else Nil
  }

  private def checkForErrors(artifacts: Seq[Artifact]): Unit = {
    val errorArtifacts = artifacts.collectInstancesOf[MessagesArtifact].filter(_.hasErrors)
    if (errorArtifacts.nonEmpty) {
      val messages = errorArtifacts.flatMap(_.messages.filter(_.isError))
      val messageStrings = messages.map { m =>
        m.pos match {
          case Some(p) => s"${m.msg} [${p.filepath}:${p.startLine}]"
          case None    => m.msg
        }
      }
      throw new IllegalArgumentException(s"Configuration error(s):\n  ${messageStrings.mkString("\n  ")}")
    }
  }
}

final case class WorkspaceStructure(
    structureHash: String,
    scalaConfig: ScalaVersionConfig,
    pythonEnabled: Boolean,
    extractVenvs: Boolean,
    scopes: Map[ScopeId, ResolvedScopeInformation]
) {
  override def toString: String = s"WorkspaceStructure($structureHash)"
  // again assuming that sha256 is a decent hash
  override def hashCode: Int = structureHash.##
  override def equals(that: Any): Boolean = that match {
    case other: WorkspaceStructure => this.structureHash == other.structureHash
    case _                         => false
  }
}

final case class ResolvedScopeInformation(
    config: ScopeConfiguration,
    local: Boolean,
    localInternalCompileDependencies: Seq[ScopeId],
    sparseInternalCompileDependencies: Seq[ScopeId],
    externalCompileDependencies: Seq[ExternalClassFileArtifact]
)
