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
import optimus.buildtool.artifacts.PathingArtifact
import optimus.buildtool.files.RelativePath
import optimus.buildtool.processors.ProcessorType
import optimus.buildtool.processors.ScopeProcessor
import optimus.buildtool.scope.sources.JavaAndScalaCompilationSources
import optimus.buildtool.utils.Utils
import optimus.platform._

import scala.collection.immutable.Seq

@entity class ScopeProcessing(
    scope: CompilationScope,
    javaAndScalaSources: JavaAndScalaCompilationSources,
    processors: Map[ProcessorType, ScopeProcessor]
) {

  private case class ProcessedResource(location: RelativePath, content: String)

  @node def process(pathingArtifact: PathingArtifact): Seq[Artifact] = {
    scope.config.processorConfig.apar.flatMap { case (processorType, cfg) =>
      val processor = processors.getOrElse(processorType, noProcessor(processorType.name))
      val dependencyArtifacts = processor.dependencies(scope)
      val dependencyErrors = Artifact.onlyErrors(dependencyArtifacts)
      val templateFile = scope.config.paths.absScopeRoot.resolveFile(cfg.templateFile)
      val templateHeaderFile =
        cfg.templateHeaderFile.map(relativePath => scope.config.paths.absScopeRoot.resolveFile(relativePath))
      val templateFooterFile =
        cfg.templateFooterFile.map(relativePath => scope.config.paths.absScopeRoot.resolveFile(relativePath))
      val objectsFile = cfg.objectsFile.map(relativePath => scope.config.paths.absScopeRoot.resolveFile(relativePath))
      dependencyErrors.getOrElse {
        val inputs = processor.inputs(
          cfg.name,
          templateFile,
          templateHeaderFile,
          templateFooterFile,
          objectsFile,
          cfg.installLocation,
          cfg.configuration,
          scope,
          javaAndScalaSources
        )
        val fingerprintHash = inputs().fingerprintHash
        val tpe = processor.artifactType
        scope.cached(tpe, Some(cfg.name), fingerprintHash) {
          val outputJar =
            scope.pathBuilder.outputPathFor(scope.id, fingerprintHash, tpe, Some(cfg.name), incremental = false).asJar
          val tpeStr = if (cfg.name == tpe.name) tpe.name else s"${tpe.name} (${cfg.name})"
          log.info(s"[${scope.id}:$tpeStr] Starting processing task ${cfg.name}")
          val artifact = processor.processInputs(scope.id, inputs, pathingArtifact, dependencyArtifacts, outputJar)
          if (!artifact.exists(_.hasErrors)) log.info(s"[${scope.id}:$tpeStr] Completing processing task ${cfg.name}")
          else Utils.FailureLog.error(s"[${scope.id}:$tpeStr] Completing processing task ${cfg.name} with errors")
          artifact
        }
      }
    }
  }

  private def noProcessor(key: String): Nothing = throw new IllegalArgumentException(
    s"No processor for key '$key' (known processors: ${processors.keys.map(_.name)})"
  )
}
