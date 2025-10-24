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
package optimus.buildtool.utils

import java.nio.file.Files
import optimus.buildtool.files.Directory
import optimus.buildtool.files.FileAsset
import optimus.buildtool.files.RelativePath
import optimus.buildtool.files.SourceUnitId
import optimus.platform._

final case class Sandbox private (
    root: Directory,
    sources: Seq[(SourceUnitId, SandboxedFile)],
    rootSourcesPaths: Seq[(SourceUnitId, SandboxedFile)]) {
  val buildDir: Directory = root.resolveDir(Sandbox.BuildDir)
  val sourceDir: Directory = root.resolveDir(Sandbox.SrcDir)
  val rootSourceDir: Directory = root.resolveDir(Sandbox.RootSrcDir)

  private val sourcesById = sources.toMap

  def sourceDir(contentPath: RelativePath): Directory = sourceDir.resolveDir(contentPath)

  def outputDir(dir: String): Directory = {
    val d = buildDir.resolveDir(dir)
    Files.createDirectories(d.path)
    d
  }
  def outputFile(file: String): FileAsset = buildDir.resolveFile(file)

  def source(id: SourceUnitId): SandboxedFile = sourcesById(id)

  def close(): Unit = AssetUtils.recursivelyDelete(root)
}

// Note: When reading files, prefer `content` over `file` when possible to avoid unnecessary I/O
final case class SandboxedFile(file: FileAsset, content: HashedContent)

object Sandbox {
  val BuildDir = "build"
  val SrcDir = "src"
  private val RootSrcDir = "root-src"
}

final case class SandboxFactory(tempRoot: Directory) {

  def empty(name: String): Sandbox = {
    val root = Directory.temporary(name, Some(tempRoot))
    Sandbox(root, Nil, Nil)
  }

  @async def apply[A <: SourceUnitId](name: String, content: Map[A, HashedContent]): Sandbox = {
    val sandbox = empty(name)
    val sources = prepareSources(content, sandbox.sourceDir, (s: A) => s.sourceFolderToFilePath)
    sandbox.copy(sources = sources)
  }

  @async def apply[A <: SourceUnitId](name: String, content: Seq[(RelativePath, Map[A, HashedContent])]): Sandbox = {
    val sandbox = empty(name)
    val sources = content.apar.flatMap { case (contentRoot, c) =>
      prepareSources(c, sandbox.sourceDir.resolveDir(contentRoot), (s: A) => s.sourceFolderToFilePath)
    }
    sandbox.copy(sources = sources)
  }

  @async def withRootPaths[A <: SourceUnitId](name: String, content: Map[A, HashedContent]): Sandbox = {
    val sandbox = empty(name)
    val sources = prepareSources(content, sandbox.sourceDir, (s: A) => s.sourceFolderToFilePath)
    val rootSources = prepareSources(content, sandbox.rootSourceDir, (s: A) => s.localRootToFilePath)
    sandbox.copy(sources = sources, rootSourcesPaths = rootSources)
  }

  @async private def prepareSources[A <: SourceUnitId](
      content: Map[A, HashedContent],
      targetDir: Directory,
      targetStrategy: A => RelativePath
  ): Seq[(A, SandboxedFile)] =
    content.toIndexedSeq.apar.map { case (s, hc) =>
      val file = targetDir.resolveFile(targetStrategy(s))
      Utils.createDirectories(file.parent)
      Files.copy(hc.contentAsInputStream, file.path)
      s -> SandboxedFile(file, hc)
    }
}
