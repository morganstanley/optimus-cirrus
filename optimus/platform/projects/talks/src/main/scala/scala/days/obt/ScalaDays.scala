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
package scala.days.obt

import optimus.platform.OptimusApp
import optimus.platform._
import optimus.platform.entersGraph
import optimus.platform.reactive._
import optimus.platform.reactive.dsl._
import optimus.platform.util.Log

// Scopes represent modules in IntelliJ: they are groupings of code compiled at the same time with the same dependencies
final case class ScopeId(org: String, name: String) {
  override def toString: String = s"$org.$name"
}

// Defines a scope, its dependencies and its versions
@entity class ScopeConfiguration(val id: ScopeId, val scopeDependencies: Seq[ScopeId]) extends Timings {
  // this represents the current state of the source files for this scope
  // (in real OBT this is a hash of the content of all this scope's .scala/.java source files)
  @node(tweak = true) val sourceVersion: Int = 1
}

// Represents any output from the compiler (e.g. a jar file)
sealed trait Artifact {
  def scopeId: ScopeId
  def fingerprint: Map[ScopeId, Int] // scopeId -> sourceVersion (a hash of the inputs) for this and all dependencies
}

final case class SignatureArtifact(scopeId: ScopeId, fingerprint: Map[ScopeId, Int]) extends Artifact
final case class ClassFileArtifact(scopeId: ScopeId, fingerprint: Map[ScopeId, Int]) extends Artifact

final case class Dependencies(signatures: Seq[SignatureArtifact], fingerprint: Map[ScopeId, Int])

final case class BuildResult(artifacts: Set[ClassFileArtifact]) extends DisplayString

@entity class BuildTool(scopes: Set[ScopeId], configSource: Map[ScopeId, ScopeConfiguration]) {
  @node def build: BuildResult = buildScopes(scopes)

  @node private def buildScopes(scopes: Set[ScopeId]): BuildResult = {
    log.info(s"Building scopes: $scopes")
    val configs = scopes.map(configSource)
    val artifacts = configs.apar.map(compile)
    BuildResult(artifacts)
  }

  @node private def compile(config: ScopeConfiguration): ClassFileArtifact = {
    log.info(s"[${config.id}] Started compiling ${config.id.name} scope")
    val signatureArtifact = signatures(config)
    val classesArtifact = classes(config, signatureArtifact)
    log.info(s"[${config.id}] Done compiling ${config.id.name} scope")
    classesArtifact
  }

  @node private def signatures(config: ScopeConfiguration): SignatureArtifact = {
    val deps = dependencies(config)
    doCompile(config.id, "signatures", config.signatureTime, config.sourceVersion, deps.signatures)
    SignatureArtifact(config.id, deps.fingerprint + (config.id -> config.sourceVersion))
  }

  @node private def classes(config: ScopeConfiguration, signatureArtifact: SignatureArtifact): ClassFileArtifact = {
    val deps = dependencies(config)
    doCompile(config.id, "classes", config.classesTime, config.sourceVersion, signatureArtifact +: deps.signatures)
    ClassFileArtifact(config.id, deps.fingerprint + (config.id -> config.sourceVersion))
  }

  @node private def dependencies(config: ScopeConfiguration): Dependencies = {
    val dependencyConfigs = config.scopeDependencies.map(configSource)
    val dependencySignatures = dependencyConfigs.apar.map(signatures)
    val dependencyFingerprint = dependencySignatures.flatMap(_.fingerprint).toMap
    Dependencies(dependencySignatures, dependencyFingerprint)
  }

  private def doCompile(
      scope: ScopeId,
      phase: String,
      delay: Int,
      version: Int,
      dependencySignatures: Seq[SignatureArtifact]): Unit = {
    log.info(s"[$scope] Started compile phase $phase for ${scope.name} scope (source version: $version)")
    log.debug(s"[$scope] Dependencies: $dependencySignatures")
    Thread.sleep(delay * 1000)
    log.info(s"[$scope] Done compile phase $phase for ${scope.name} scope (source version: $version)")
  }
}

object ScalaDays extends OptimusApp with Log {
  // 1. define the scopes
  private val (utils, data, model, app) = createScopes

  // 2. get their dependencies (usually by reading .obt files, but we'll hardcode them for the demo)
  private val configSource = dependenciesForScopes

  // 3. create a build tool for scopes 'model' and 'app' given the dependency information
  private val builder = BuildTool(Set(model, app), configSource)

  // 4. simulate invalidating 'data' sources
  private val sourceToInvalidate = configSource(data)
  private val updater = new ExternalVersionUpdater(data, firstVersionToPublish = 2)
  private val target = new BuildOutputTarget

  private val reactiveApplication: ReactiveApplication = react() {
    updater --> sourceToInvalidate.sourceVersion
    target <-- builder.build
  }

  @entersGraph override def run(): Unit = {
    reactiveApplication.createEvaluator
    Thread.sleep(Integer.MAX_VALUE) // keep publishing until stopped
  }

  private def createScopes: (ScopeId, ScopeId, ScopeId, ScopeId) = {
    def createScope(name: String): ScopeId = ScopeId("optimus", name)

    val utils = createScope("utils")
    val data = createScope("data")
    val model = createScope("model")
    val app = createScope("app")
    (utils, data, model, app)
  }

  private def dependenciesForScopes: Map[ScopeId, ScopeConfiguration] = {
    val utilsConfig = ScopeConfiguration(id = utils, scopeDependencies = Nil)
    val dataConfig = ScopeConfiguration(id = data, scopeDependencies = Seq(utils))
    val modelConfig = ScopeConfiguration(id = model, scopeDependencies = Seq(utils))
    val appConfig = ScopeConfiguration(id = app, scopeDependencies = Seq(data, model))
    Map(
      utils -> utilsConfig,
      data -> dataConfig,
      model -> modelConfig,
      app -> appConfig
    )
  }
}
