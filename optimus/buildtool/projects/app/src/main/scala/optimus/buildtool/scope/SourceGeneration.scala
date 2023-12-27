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
package optimus.buildtool.scope

import optimus.buildtool.artifacts.Artifact
import optimus.buildtool.artifacts.CompilationMessage
import optimus.buildtool.artifacts.InMemoryMessagesArtifact
import optimus.buildtool.artifacts.InternalArtifactId
import optimus.buildtool.generators.GeneratorType
import optimus.buildtool.generators.SourceGenerator
import optimus.buildtool.trace.GenerateSource
import optimus.buildtool.utils.Utils
import optimus.platform._

import scala.collection.immutable.Seq

@entity class SourceGeneration(
    scope: CompilationScope,
    sourceGenerators: Map[GeneratorType, SourceGenerator]
) {

  @node def generatedSources: Seq[Artifact] = {
    scope.config.generatorConfig.apar.flatMap { case (generatorType, cfg) =>
      val gen = sourceGenerators.getOrElse(generatorType, noGenerator(generatorType.name))
      val dependencyErrors = Artifact.onlyErrors(gen.dependencies(scope))
      dependencyErrors.getOrElse {
        val inputs =
          gen.inputs(
            cfg.name,
            cfg.internalTemplateFolders.apar.map { p =>
              val d = scope.config.paths.absScopeRoot.resolveDir(p)
              scope.directoryFactory.lookupSourceFolder(scope.config.paths.workspaceSourceRoot, d)
            },
            cfg.externalTemplateFolders.apar.map(scope.directoryFactory.reactive),
            cfg.sourceFilter,
            cfg.configuration,
            scope
          )

        val tpe = gen.artifactType
        val tpeStr = if (cfg.name == tpe.name) tpe.name else s"${tpe.name} (${cfg.name})"

        if (gen.containsRelevantSources(inputs)) {
          val fingerprintHash = inputs().fingerprintHash
          scope.cached(tpe, Some(cfg.name), fingerprintHash) {
            val outputJar =
              scope.pathBuilder.outputPathFor(scope.id, fingerprintHash, tpe, Some(cfg.name), incremental = false).asJar
            log.info(s"[${scope.id}] Starting $tpeStr source generation")
            val artifacts = gen.generateSource(scope.id, inputs, outputJar)
            if (!artifacts.exists(_.hasErrors)) log.info(s"[${scope.id}] Completing $tpeStr source generation")
            else Utils.FailureLog.error(s"[${scope.id}] Completing $tpeStr source generation with errors")
            artifacts
          }
        } else {
          val messages =
            Seq(CompilationMessage.error(s"[${scope.id}] No relevant sources found for $tpeStr source generation"))
          Seq(
            InMemoryMessagesArtifact(
              InternalArtifactId(scope.id, gen.artifactType, Some(cfg.name)),
              messages,
              GenerateSource))
        }
      }
    }
  }

  private def noGenerator(key: String): Nothing = throw new IllegalArgumentException(
    s"No generator for key '$key' (known generators: ${sourceGenerators.keys.map(_.name)})"
  )
}
