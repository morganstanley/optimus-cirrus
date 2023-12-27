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
package optimus.buildtool.compilers.zinc.setup

import java.util.Optional

import optimus.buildtool.artifacts.Artifact.InternalArtifact
import optimus.buildtool.compilers.SyncCompiler
import optimus.buildtool.compilers.zinc.ZincCompilerFactory
import optimus.buildtool.compilers.zinc.mappers.MappingTrace
import optimus.buildtool.compilers.zinc.mappers.ZincReadMapper
import optimus.buildtool.compilers.zinc.mappers.ZincWriteMapper
import optimus.buildtool.config.ScopeId
import optimus.buildtool.trace.MessageTrace
import sbt.internal.inc.FileAnalysisStore
import xsbti.compile.AnalysisContents
import xsbti.compile.AnalysisStore
import xsbti.compile.analysis.ReadWriteMappers

object ClassAnalysisStore {
  private val log = msjava.slf4jutils.scalalog.getLogger(this)
}

class ClassAnalysisStore(
    scopeId: ScopeId,
    traceType: MessageTrace,
    settings: ZincCompilerFactory,
    inputs: SyncCompiler.Inputs,
    jars: Jars,
    incremental: Boolean,
    analysisMappingTrace: MappingTrace
) extends AnalysisStore {
  import ClassAnalysisStore._

  private val tpeAndNameToLatestIncrAndHash = (inputs.inputArtifacts ++ inputs.pluginArtifacts.flatten).collect {
    case InternalArtifact(id, a) =>
      (id.tpe, id.scopeId.properPath) -> (MappingTrace.incr(a.path), MappingTrace.hash(a.path))
  }.toMap // we can't use toSingleMap here, because it's possible we have duplicates

  private val analysisJar = jars.analysisJar.tempPath

  private val mappers = new ReadWriteMappers(
    new ZincReadMapper(
      scopeId = scopeId,
      fingerprintHash = inputs.fingerprintHash,
      incremental = incremental,
      traceType = traceType,
      outputJar = jars.outputJar,
      mappingTrace = analysisMappingTrace,
      tpeAndNameToLatestIncrAndHash = tpeAndNameToLatestIncrAndHash,
      workspaceRoot = settings.workspaceRoot,
      buildDir = settings.buildDir,
      depCopyRoot = settings.depCopyRoot,
      updatePluginHash = settings.zincIgnorePluginHash,
      strictErrorTolerance = settings.strictErrorTolerance,
      depCopyFileSystemAsset = settings.depCopyFileSystemAsset
    ),
    // On output, all we need to do is strip out the UUID-hyphen.
    new ZincWriteMapper(
      scopeId = scopeId,
      fingerprintHash = inputs.fingerprintHash,
      incremental = incremental,
      traceType = traceType,
      outputJar = jars.outputJar,
      workspaceRoot = settings.workspaceRoot,
      buildDir = settings.buildDir,
      depCopyRoot = settings.depCopyRoot,
      strictErrorTolerance = settings.strictErrorTolerance,
      settings.coreClasspath,
      depCopyFileSystemAsset = settings.depCopyFileSystemAsset
    )
  )
  private val store =
    FileAnalysisStore.binary(analysisJar.path.toFile, mappers, tmpDir = jars.analysisJar.tempPath.parent.path.toFile)

  override def get: Optional[AnalysisContents] = store.get
  override def unsafeGet: AnalysisContents = store.unsafeGet

  override def set(analysisContents: AnalysisContents): Unit = {
    if (!inputs.saveAnalysis) log.warn(s"Not storing ${analysisJar.pathString}") else store.set(analysisContents)
  }
}
