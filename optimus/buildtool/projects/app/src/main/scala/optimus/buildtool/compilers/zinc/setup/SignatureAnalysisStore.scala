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

import optimus.buildtool.compilers.zinc.ZincCompilerFactory
import optimus.buildtool.compilers.zinc.mappers.ZincSignatureReadMapper
import optimus.buildtool.compilers.zinc.mappers.ZincSignatureWriteMapper
import optimus.buildtool.config.ScopeId
import optimus.buildtool.files.JarAsset
import xsbti.compile.AnalysisStore
import sbt.internal.inc.FileAnalysisStore
import xsbti.compile.AnalysisContents
import xsbti.compile.analysis.ReadWriteMappers

object SignatureAnalysisStore {
  private val log = msjava.slf4jutils.scalalog.getLogger(this)
}

class SignatureAnalysisStore(
    scopeId: ScopeId,
    settings: ZincCompilerFactory,
    saveAnalysis: Boolean,
    signatureAnalysisJar: JarAsset
) extends AnalysisStore {
  import SignatureAnalysisStore._

  private val signatureMappers = new ReadWriteMappers(ZincSignatureReadMapper, ZincSignatureWriteMapper)
  private val store = FileAnalysisStore.binary(
    signatureAnalysisJar.path.toFile,
    signatureMappers,
    tmpDir = signatureAnalysisJar.parent.path.toFile)

  // this store is only used for writing - signature analysis for upstreams is loaded via ZincEntryLookup
  override def get(): Optional[AnalysisContents] =
    throw new UnsupportedOperationException(
      s"Unexpected call to SignatureAnalysisStore.get() for ${signatureAnalysisJar.pathString}"
    )
  override def unsafeGet(): AnalysisContents =
    throw new UnsupportedOperationException(
      s"Unexpected call to SignatureAnalysisStore.unsafeGet() for ${signatureAnalysisJar.pathString}"
    )
  override def set(analysisContents: AnalysisContents): Unit = {
    if (!saveAnalysis) log.warn(s"Not storing ${signatureAnalysisJar.pathString}")
    else store.set(analysisContents)
  }
}
