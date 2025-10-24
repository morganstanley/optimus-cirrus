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
import optimus.buildtool.generators.ArtifactWriter
import optimus.buildtool.generators.GeneratorType
import optimus.buildtool.generators.GeneratorUtils
import optimus.buildtool.generators.SourceGenerator
import optimus.buildtool.trace.GenerateSource
import optimus.buildtool.trace.ObtTrace
import optimus.buildtool.utils.Utils
import optimus.platform._

@entity class SourceGeneration(
    scope: CompilationScope,
    sourceGenerators: Map[GeneratorType, SourceGenerator]
) {

  @node def generatedSources: Seq[Artifact] = {
    scope.config.generatorConfig.apar.flatMap { case (generatorType, cfg) =>
      val gen = sourceGenerators.getOrElse(generatorType, noGenerator(generatorType.name))
      val artifactType = gen.artifactType
      if (ScopedCompilation.generate(artifactType)) {
        val dependencyErrors = Artifact.onlyErrors(gen.dependencies(scope))
        dependencyErrors.getOrElse {
          val generatorId = GeneratorUtils.generatorId(gen.tpe, cfg.name)

          val internalFolders = cfg.internalTemplateFolders.apar.map { p =>
            val d = scope.config.paths.absScopeRoot.resolveDir(p)
            scope.directoryFactory.lookupSourceFolder(scope.config.paths.workspaceSourceRoot, d)
          }

          val externalFolders = cfg.externalTemplateFolders.apar.map(scope.directoryFactory.reactive)

          val inputs = gen.inputs(
            asNode { () =>
              GeneratorUtils.sandboxTemplates(
                scope.sandboxFactory,
                scope.config.paths.workspaceSourceRoot,
                generatorId,
                internalFolders,
                externalFolders,
                gen.templateType(cfg.configuration) && cfg.sourceFilter,
                scope
              )
            },
            cfg.configuration,
            scope
          )

          val discriminator = GeneratorUtils.discriminator(generatorType, cfg.name)
          val category = GenerateSource(generatorType, cfg.name)

          val fingerprint = inputs().fingerprint
          val genSources = if (gen.containsRelevantSources(inputs)) {
            val fingerprintHash = fingerprint.hash
            scope.cached(artifactType, discriminator, fingerprint.hash) {
              ObtTrace.traceTask(scope.id, category) {
                val outputJar =
                  scope.pathBuilder
                    .outputPathFor(scope.id, fingerprintHash, gen.artifactType, discriminator)
                    .asJar
                log.info(s"[${scope.id}] Starting $generatorId source generation")
                val artifactWriter = ArtifactWriter(scope.id, generatorType, cfg.name, outputJar)
                val artifacts = gen.generateSource(scope.id, inputs, artifactWriter)
                if (!artifacts.exists(_.hasErrors)) log.info(s"[${scope.id}] Completing $generatorId source generation")
                else Utils.FailureLog.error(s"[${scope.id}] Completing $generatorId source generation with errors")
                artifacts
              }
            }
          } else {
            val messages =
              Seq(
                CompilationMessage.error(s"[${scope.id}] No relevant sources found for $generatorId source generation"))
            Seq(
              InMemoryMessagesArtifact(
                InternalArtifactId(scope.id, gen.artifactType, discriminator),
                messages,
                category))
          }
          genSources :+ fingerprint
        }
      } else Nil
    }
  }

  private def noGenerator(key: String): Nothing = throw new IllegalArgumentException(
    s"No generator for key '$key' (known generators: ${sourceGenerators.keys.map(_.name)})"
  )
}
