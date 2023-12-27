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
package optimus.buildtool.format

import com.typesafe.config.Config
import optimus.buildtool.config.ScopeId
import optimus.buildtool.dependencies
import optimus.buildtool.dependencies.CentralDependencies
import optimus.buildtool.dependencies.JdkDependenciesLoader
import optimus.buildtool.dependencies.JvmDependenciesLoader
import optimus.buildtool.dependencies.PythonDependenciesLoader
import optimus.buildtool.files.Directory
import optimus.buildtool.format.ConfigUtils._
import optimus.buildtool.format.docker.DockerStructure

import java.util.{Map => JMap}
import scala.collection.immutable.Seq

final case class WorkspaceDefinition(
    structure: WorkspaceStructure,
    config: ProjectProperties,
    dependencies: CentralDependencies,
    resolvers: Seq[ResolverDefinition],
    scopes: Map[ScopeId, ScopeDefinition],
    appValidator: AppValidator,
    runConfSubstitutionsValidator: RunConfSubstitutionsValidator,
    dockerStructure: DockerStructure,
    globalRules: RulesStructure
)

object WorkspaceDefinition {
  def load(
      workspaceName: String,
      workspaceSrcRoot: Directory,
      externalConfig: Config,
      externalProperties: JMap[String, String],
      loadFile: ObtFile.Loader,
      cppOsVersions: Seq[String],
      useMavenLibs: Boolean = false
  ): Result[WorkspaceDefinition] = for {
    projectProperties <- ProjectProperties.load(loadFile, externalConfig, externalProperties)

    loadFileWithProperties = ObtFile.Loader(loadFile) { res =>
      res.map(_.resolveWithReferences(projectProperties.config))
    }

    workspace <- WorkspaceStructure.loadModuleStructure(workspaceName, loadFileWithProperties)
    resolvers <- ResolverDefinition.load(loadFileWithProperties)
    pythonDependencies <- PythonDependenciesLoader.load(PythonConfig, loadFile)
    jvmDependencies <- JvmDependenciesLoader.load(projectProperties, loadFile, useMavenLibs)
    jdkDependencies <- JdkDependenciesLoader.load(loadFile)
    updatedProperties = projectProperties.includeConfig(jvmDependencies.versionsConfig)
    rules <- RulesStructure.load(loadFile)
    toolchainStructure <- CppToolchainStructure.load(loadFileWithProperties)
    cppToolchains = toolchainStructure.toolchains.map(c => c.name -> c).toMap
    centralDependencies = dependencies.CentralDependencies(jvmDependencies, jdkDependencies, pythonDependencies)

    modules <- new ScopeDefinitionCompiler(
      loadFileWithProperties,
      centralDependencies,
      workspace,
      rules,
      cppToolchains,
      cppOsVersions,
      useMavenLibs
    ).compile(workspaceSrcRoot)

    appValidator <- AppValidator.load(loadFileWithProperties)
    runConfSubstitutions <- RunConfSubstitutionsValidator.load(loadFileWithProperties)
    dockerStructure <- DockerStructure.load(loadFileWithProperties)
  } yield {
    WorkspaceDefinition(
      workspace,
      updatedProperties,
      centralDependencies,
      resolvers.toList,
      modules,
      appValidator,
      runConfSubstitutions,
      dockerStructure,
      rules
    )
  }
}
