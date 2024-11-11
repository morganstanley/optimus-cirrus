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
package optimus.buildtool.compilers

import optimus.buildtool.artifacts.CompilationMessage
import optimus.buildtool.artifacts.CompilationMessage.Severity
import optimus.buildtool.artifacts.ResolutionArtifactType
import optimus.buildtool.config.ForbiddenDependencyConfiguration
import optimus.buildtool.config.PartialScopeId
import optimus.buildtool.config.ScopeId
import optimus.buildtool.config.ScopeResolver
import optimus.buildtool.resolvers.DependencyInfo
import optimus.platform._

import scala.util.matching.Regex
import scala.collection.immutable.Seq

@entity class ConfigurationValidator(scopeId: ScopeId, forbiddenDependencies: Seq[ForbiddenDependencyConfiguration]) {

  @node def duplicateDefinitionsCheck: Option[Seq[CompilationMessage]] = {
    val dependencyDefinitions =
      forbiddenDependencies.flatMap(_.dependencyId) ++ forbiddenDependencies.flatMap(_._dependencyRegex)
    val duplicateDefinitions = (dependencyDefinitions diff dependencyDefinitions.distinct).distinct
    if (duplicateDefinitions.nonEmpty)
      Some(
        duplicateDefinitions.map(d =>
          CompilationMessage(
            None,
            msg = s"Forbidden dependencies for '$d' defined in more than one place. " +
              "Forbidden dependencies should be defined in at most one location for the same dependency",
            Severity.Error
          )))
    else None
  }

  @node def validate(
      internalDirectDeps: Seq[ScopeId],
      externalDirectDeps: Seq[DependencyInfo],
      internalTransitiveDeps: Seq[ScopeId],
      externalTransitiveDeps: Seq[DependencyInfo],
      tpe: ResolutionArtifactType): Seq[CompilationMessage] =
    validateInternal(internalDirectDeps, internalTransitiveDeps, tpe) ++
      validateExternal(externalDirectDeps, externalTransitiveDeps, tpe)

  @node private def validateInternal(
      internalDirectDeps: Seq[ScopeId],
      internalTransitiveDeps: Seq[ScopeId],
      tpe: ResolutionArtifactType): Seq[CompilationMessage] =
    forbiddenDependencies.apar.flatMap { fd =>
      val allowedIn =
        isAllowedInId(fd.allowedInIds, scopeId) || isAllowedInPattern(fd.allowedInPatterns, scopeId.toString)
      if (fd.isExternal || allowedIn) Seq.empty
      else {
        val internalDependencies =
          if (fd.transitive) internalTransitiveDeps else internalDirectDeps
        internalDependencies.apar.flatMap { internalDep =>
          idDependencyCheck(fd, internalDep, tpe, fd.transitive) ++
            regexDependencyCheck(fd, internalDep.toString, tpe, fd.transitive)
        }
      }
    }

  @node private def validateExternal(
      externalDirectDeps: Seq[DependencyInfo],
      externalTransitiveDeps: Seq[DependencyInfo],
      tpe: ResolutionArtifactType): Seq[CompilationMessage] =
    forbiddenDependencies.apar.flatMap { fd =>
      val allowedIn =
        isAllowedInId(fd.allowedInIds, scopeId) || isAllowedInPattern(fd.allowedInPatterns, scopeId.toString)
      if (!fd.isExternal || allowedIn) Seq.empty
      else {
        val externalDependencies = if (fd.transitive) externalTransitiveDeps else externalDirectDeps
        externalDependencies.apar.flatMap { externalDep =>
          if (fd.configurations.isEmpty || fd.configurations.contains(externalDep.config)) {
            stringDependencyCheck(fd, externalDep.dotModule, tpe, fd.transitive) ++
              regexDependencyCheck(fd, externalDep.dotModule, tpe, fd.transitive)
          } else Seq.empty
        }
      }
    }

  @node private def idDependencyCheck(
      fd: ForbiddenDependencyConfiguration,
      dependency: ScopeId,
      tpe: ResolutionArtifactType,
      transitiveCheck: Boolean): Seq[CompilationMessage] =
    fd.dependencyId
      .map(d => {
        val resolvedDependencyId = ScopeResolver.tryResolveScopes(Set(dependency), d, requireExactMatch = true)
        resolvedDependencyId match {
          case Some(_) => Seq(generateForbiddenDependencyErrorMsg(dependency.toString, scopeId, tpe, transitiveCheck))
          case None    => Seq.empty
        }
      })
      .getOrElse(Seq.empty)

  @node private def stringDependencyCheck(
      fd: ForbiddenDependencyConfiguration,
      dependency: String,
      tpe: ResolutionArtifactType,
      transitiveCheck: Boolean
  ): Seq[CompilationMessage] =
    fd.dependencyId
      .map { id =>
        if (id == dependency)
          Seq(generateForbiddenDependencyErrorMsg(dependency, scopeId, tpe, transitiveCheck))
        else Seq.empty
      }
      .getOrElse(Seq.empty)

  @node private def regexDependencyCheck(
      fd: ForbiddenDependencyConfiguration,
      dependency: String,
      tpe: ResolutionArtifactType,
      transitiveCheck: Boolean
  ): Seq[CompilationMessage] =
    fd.dependencyRegex
      .map { r =>
        if (r.pattern.matcher(dependency).matches())
          Seq(generateForbiddenDependencyErrorMsg(dependency, scopeId, tpe, transitiveCheck))
        else Seq.empty
      }
      .getOrElse(Seq.empty)

  @node private def isAllowedInId(allowedIn: Seq[PartialScopeId], dependencyScopeId: ScopeId): Boolean =
    allowedIn.exists(partialScopeId => partialScopeId.contains(dependencyScopeId))

  @node private def isAllowedInPattern(allowedIn: Seq[Regex], dependency: String): Boolean =
    allowedIn.exists(regex => regex.pattern.matcher(dependency).matches)

  @node private def generateForbiddenDependencyErrorMsg(
      fd: String,
      scope: ScopeId,
      tpe: ResolutionArtifactType,
      transitiveCheck: Boolean): CompilationMessage =
    CompilationMessage(
      None,
      msg =
        if (transitiveCheck)
          s"Forbidden direct or transitive dependency: '$fd' in scope '$scope' (${tpe.name}). Please run OBTVisualizer on your branch to check the dependency tree."
        else s"Forbidden direct dependency: '$fd' in scope '$scope' (${tpe.name}).",
      Severity.Error
    )
}
