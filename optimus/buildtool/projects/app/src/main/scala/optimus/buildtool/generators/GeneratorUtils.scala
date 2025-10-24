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
package optimus.buildtool.generators

import optimus.buildtool.files.Directory
import optimus.buildtool.files.Directory.PathFilter
import optimus.buildtool.files.JarAsset
import optimus.buildtool.files.ReactiveDirectory
import optimus.buildtool.files.SourceFolder
import optimus.buildtool.generators.sandboxed.SandboxedInputs
import optimus.buildtool.generators.sandboxed.SandboxedInputsImpl
import optimus.buildtool.scope.CompilationScope
import optimus.buildtool.utils.PathUtils
import optimus.buildtool.utils.SandboxFactory
import optimus.platform._

import java.nio.file.FileSystems
import scala.reflect.ClassTag
import scala.reflect.classTag

@entity object GeneratorUtils {

  def generatorId(generatorType: GeneratorType, generatorName: String): String =
    if (generatorType.name == generatorName) generatorName else s"${generatorType.name}-$generatorName"

  def discriminator(generatorType: GeneratorType, generatorName: String): Option[String] =
    Some(generatorId(generatorType, generatorName))

  @node def sandboxTemplates(
      sandboxFactory: SandboxFactory,
      workspaceSourceRoot: Directory,
      generatorId: String,
      internalFolders: Seq[SourceFolder],
      externalFolders: Seq[ReactiveDirectory],
      sourcePredicate: PathFilter,
      scope: CompilationScope
  ): SandboxedInputs =
    SandboxedInputsImpl(
      sandboxFactory,
      workspaceSourceRoot,
      generatorId,
      internalFolders,
      externalFolders,
      sourcePredicate,
      scope
    )

  def location[A: ClassTag]: JarAsset =
    JarAsset(
      PathUtils.uriToPath(
        classTag[A].runtimeClass.getProtectionDomain.getCodeSource.getLocation.toURI.toString,
        FileSystems.getDefault
      )
    )
}
