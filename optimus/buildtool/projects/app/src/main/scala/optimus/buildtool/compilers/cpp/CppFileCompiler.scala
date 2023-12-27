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
package optimus.buildtool.compilers.cpp

import optimus.buildtool.artifacts.CompilationMessage
import optimus.buildtool.config.CppBuildConfiguration
import optimus.buildtool.files.Directory
import optimus.buildtool.files.FileAsset
import optimus.buildtool.files.RelativePath
import optimus.buildtool.files.SourceUnitId

import scala.collection.immutable.Seq

trait CppFileCompiler {
  import CppFileCompiler._

  def setup(cppConfiguration: CppBuildConfiguration, sandbox: Directory): Unit
  def compile(inputs: FileInputs): Output
}

object CppFileCompiler {
  final case class FileInputs(
      sourceFile: FileAsset,
      sourceId: SourceUnitId,
      cppConfiguration: CppBuildConfiguration,
      sandbox: Directory,
      internalIncludes: Seq[Directory],
      objectDir: Directory,
      precompiledHeaders: Seq[PrecompiledHeader]
  )

  final case class PrecompiledHeader(header: RelativePath, headerDir: Directory, create: Boolean)

  final case class Output(
      objectFile: Option[FileAsset],
      messages: Seq[CompilationMessage]
  )
}
