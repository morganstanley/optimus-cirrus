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
import optimus.buildtool.compilers.AsyncCppCompiler.BuildType
import optimus.buildtool.config.CppBuildConfiguration
import optimus.buildtool.files.Directory
import optimus.buildtool.files.FileAsset

import scala.collection.immutable.Seq

trait CppLinker {
  import CppLinker._

  def link(inputs: LinkInputs): Output
}

object CppLinker {
  final case class LinkInputs(
      objectFiles: Seq[FileAsset],
      cppConfiguration: CppBuildConfiguration,
      sandbox: Directory,
      outputDir: Directory,
      buildType: BuildType
  )

  final case class Output(
      outputFile: Option[FileAsset],
      messages: Seq[CompilationMessage]
  )
  object Output {
    val empty = Output(None, Nil)
  }
}
