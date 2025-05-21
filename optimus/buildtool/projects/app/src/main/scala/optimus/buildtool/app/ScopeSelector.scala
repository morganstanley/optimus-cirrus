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
package optimus.buildtool.app

import optimus.buildtool.app.OptimusBuildToolCmdLineT._
import optimus.buildtool.config.DockerConfigurationSupport
import optimus.buildtool.config.DockerImage
import optimus.buildtool.config.ScopeConfigurationSource
import optimus.buildtool.config.ScopeId
import optimus.buildtool.files.Directory
import optimus.buildtool.utils.FileDiff
import optimus.buildtool.utils.OsUtils
import optimus.platform._

final case class ScopeArgs(
    scopesToBuild: Set[String],
    imagesToBuild: Set[String],
    warScopes: Set[String],
    scopesToExclude: Set[String],
    buildModifiedScopes: Boolean,
    buildDownstreamScopes: Boolean,
    minimalInstall: Boolean,
    imageTag: String
)

object ScopeArgs {
  def apply(cmdLine: OptimusBuildToolCmdLineT): ScopeArgs = ScopeArgs(
    cmdLine.scopesToBuild,
    cmdLine.imagesToBuild,
    cmdLine.warScopes,
    cmdLine.scopesToExclude,
    cmdLine.buildModifiedScopes,
    cmdLine.buildDownstreamScopes,
    cmdLine.minimalInstall,
    cmdLine.imageTag
  )
}

@entity class ScopeSelector(
    args: ScopeArgs,
    scopeConfigSource: ScopeConfigurationSource with DockerConfigurationSupport,
    workspaceSourceRoot: Directory,
    dockerDir: Directory
) {

  @node private[buildtool] def scopesToInclude: Set[ScopeId] = {
    if (args.scopesToBuild == Set(NoneArg))
      Set.empty[ScopeId]
    else if (args.scopesToBuild == Set(AllArg))
      scopeConfigSource.compilationScopeIds
    else if (
      args.scopesToBuild.isEmpty && args.imagesToBuild.isEmpty && args.warScopes.isEmpty && !args.buildModifiedScopes
    )
      scopeConfigSource.compilationScopeIds.apar.filter(scopeConfigSource.local)
    else
      args.scopesToBuild.apar.flatMap(scopeConfigSource.resolveScopes) ++ scopesFromImages ++ warScopes
  }

  @node private def scopesToExclude: Set[ScopeId] = args.scopesToExclude.apar.flatMap(scopeConfigSource.resolveScopes)

  @node private def modifiedScopes(fileDiff: Option[FileDiff]): Set[ScopeId] = fileDiff match {
    case Some(fd) if args.buildModifiedScopes =>
      val additionalScopes = scopeConfigSource.changesAsScopes(fd.modifiedFiles.map(_.path))
      log.info(s"""Running with '--buildModifiedScopes', adding ${additionalScopes.size} scope(s) to build:
                  |  ${additionalScopes.mkString(", ")}""".stripMargin)
      additionalScopes
    case _ => Set.empty
  }

  @node private[buildtool] def minimalInstallScopes: Option[Set[ScopeId]] =
    if (args.minimalInstall)
      Some(scopesToInclude -- scopesFromImages ++ imagesToBuild.flatten(_.directScopeIds))
    else None

  @node private def requestedScopes(fileDiff: Option[FileDiff]): Set[ScopeId] =
    scopesToInclude ++ modifiedScopes(fileDiff) -- scopesToExclude

  @node private def downstreamScopes(requestedScopes: Set[ScopeId]): Set[ScopeId] =
    if (args.buildDownstreamScopes) {
      val additionalScopes = scopeConfigSource.compilationScopeIds.apar.filter { id =>
        transitiveDeps(id).exists(requestedScopes.contains)
      }
      log.info(s"""Running with '--buildDownstreamScopes', adding ${additionalScopes.size} scope(s) to build:
                  |  ${additionalScopes.mkString(", ")}""".stripMargin)
      additionalScopes
    } else Set.empty

  // Note: We remove scopesToExclude both here and in `requestedScopes`. That ensures we don't get any of their
  // downstreams, and we also don't include them even if they're downstream of another scope.
  @node def scopes(fileDiff: Option[FileDiff] = None): Set[ScopeId] = {
    val requested = requestedScopes(fileDiff)
    requested ++ downstreamScopes(requested) -- scopesToExclude
  }

  @node private def transitiveDeps(id: ScopeId): Set[ScopeId] =
    scopeConfigSource.scopeConfiguration(id).dependencies.allInternal.toSet.apar.flatMap(transitiveDeps) + id

  @node private[buildtool] def imagesToBuild: Set[DockerImage] =
    if (args.imagesToBuild.nonEmpty) {
      if (OsUtils.isWindows) {
        throw new IllegalStateException("Building a docker image from Windows not supported. Please use a unix machine")
      }
      scopeConfigSource.parseImages(dockerDir, args.imagesToBuild, args.imageTag)
    } else Set.empty

  @node private def scopesFromImages: Set[ScopeId] = imagesToBuild.flatten(_.relevantScopeIds)

  @node private[buildtool] def warScopes = args.warScopes.apar.flatMap(scopeConfigSource.resolveScopes)

  def withIncludedScopes(includedScopes: Set[String]): ScopeSelector =
    ScopeSelector(args.copy(scopesToBuild = includedScopes), scopeConfigSource, workspaceSourceRoot, dockerDir)

}
