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
package optimus.buildtool.compilers.zinc.mappers

import java.nio.file.Path
import java.nio.file.Paths
import java.util.concurrent.ConcurrentHashMap

import optimus.buildtool.artifacts.InternalClassFileArtifactType
import optimus.buildtool.compilers.SyncCompiler.PathPair
import optimus.buildtool.compilers.zinc.ZincUtils
import optimus.buildtool.compilers.zinc.ZincVirtualFiles
import optimus.buildtool.files.Directory
import xsbti.VirtualFileRef
import xsbti.compile.MiniSetup
import xsbti.compile.analysis.GenericMapper
import xsbti.compile.analysis.Stamp
import optimus.buildtool.config.NamingConventions._
import optimus.buildtool.config.ScopeId
import optimus.buildtool.trace.MessageTrace
import optimus.buildtool.utils.PathUtils
import optimus.buildtool.utils.Utils

private[zinc] abstract class ZincPathTranslatingMapper extends PathTranslatingMapper {

  protected def scopeId: ScopeId
  protected def traceType: MessageTrace
  protected def outputJar: PathPair

  protected def workspaceRoot: Directory
  protected def buildDir: Directory
  protected def depCopyRoot: Directory
  protected def depCopyFileSystemAsset: Boolean
  protected def strictErrorTolerance: Boolean

  protected val scopeName: String = scopeId.properPath
  protected val uuid: String = outputJar.uuid.toString
  protected val classType: InternalClassFileArtifactType = ZincUtils.classType(traceType)

  protected val workspaceRootStr: String = workspaceRoot.pathString
  private val buildDirStr = buildDir.pathString
  protected val depCopyDistStr: String = // for test/linux, should not translate AFS root paths to DEPCOPY
    if (depCopyFileSystemAsset) depCopyRoot.resolveDir("dist").pathString.stripSuffix("/")
    else AfsDist.pathString.stripSuffix("/")
  protected val depCopyHttpStr: String = depCopyRoot.resolveDir("http").pathString.stripSuffix("/")
  protected val depCopyHttpsStr: String = depCopyRoot.resolveDir("https").pathString.stripSuffix("/")
  // for workspace external libs, for example `<workspace>/a/b/install/lib`
  protected val workspaceSubstitution: Substitution = Substitution(workspaceRootStr, WORKSPACE)
  protected val externalDepsSubstitutions: IndexedSeq[Substitution] = // need check https first
    workspaceSubstitution +: IndexedSeq(
      Substitution(depCopyHttpsStr, HTTPSCOPY),
      Substitution(depCopyHttpStr, HTTPCOPY),
      Substitution(depCopyDistStr, DEPCOPY))

  protected def validatePath(path: String): Unit = {
    if (looksDissectable(path))
      throw new RuntimeException(
        s"Something went terribly wrong here - please contact the OBT team. Untranslatable path: $path"
      )
    else if (strictErrorTolerance && !workspaceIndependent(path))
      throw new RuntimeException(
        s"Translated zinc path is not workspace independent. Invalid path: $path"
      )
  }

  private def looksDissectable(path: String, strict: Boolean = strictErrorTolerance): Boolean =
    Seq(BUILD_DIR_STR, buildDirStr).exists(path.contains)

  // anything making it into zinc analysis files should be workspace independent
  private def workspaceIndependent(path: String): Boolean = {
    // zinc itself sets output path to //tmp/dummy in the analysis, and that is fine
    if (path == "//tmp/dummy")
      true
    else if (
      path.startsWith(buildDirStr)
    ) // this should have been checked in looksDissectable already, but just in case...
      false
    else if (Utils.isWindows)
      !WindowsDrive.pattern.matcher(path).matches()
    else // linux
      !path.startsWith("//tmp") && !path.startsWith("//d/d1")

  }
}

final case class Substitution(realDirectory: String, key: String) { def isSame: Boolean = realDirectory == key }

private object PathTranslatingMapper {
  private val log = msjava.slf4jutils.scalalog.getLogger(this)
}

private[zinc] abstract class PathTranslatingMapper extends GenericMapper {
  private def logExceptions[T](f: => T): T =
    try f
    catch {
      case ex: Throwable =>
        PathTranslatingMapper.log.error("Error in mapper (may cause over-compilation)", ex)
        throw ex
    }

  protected def translateOptions(text: String): String
  protected def translateFile(file: String): String
  private def translateFile(file: VirtualFileRef): VirtualFileRef = {
    val translated = logExceptions(translateFile(file.id()))
    ZincVirtualFiles.toVirtualFileRef(translated)
  }
  private def translateFile(file: Path): Path =
    logExceptions(Paths.get(translateFile(PathUtils.platformIndependentString(file))))

  final override def mapSourceFile(sourceFile: VirtualFileRef): VirtualFileRef = translateFile(sourceFile)
  private val mapBinaryFileCache = new ConcurrentHashMap[VirtualFileRef, VirtualFileRef]()
  final override def mapBinaryFile(binaryFile: VirtualFileRef): VirtualFileRef = {
    mapBinaryFileCache.computeIfAbsent(binaryFile, (binaryFile: VirtualFileRef) => translateFile(binaryFile))
  }
  final override def mapScalacOption(scalacOption: String): String = scalacOption
  final override def mapBinaryStamp(file: VirtualFileRef, binaryStamp: Stamp): Stamp = binaryStamp
  final override def mapProductFile(productFile: VirtualFileRef): VirtualFileRef = translateFile(productFile)
  final override def mapJavacOption(javacOption: String): String = javacOption
  final override def mapSourceStamp(file: VirtualFileRef, sourceStamp: Stamp): Stamp = sourceStamp
  final override def mapClasspathEntry(classpathEntry: Path): Path = translateFile(classpathEntry)
  final override def mapProductStamp(file: VirtualFileRef, productStamp: Stamp): Stamp = productStamp

  final override def mapMiniSetup(miniSetup: MiniSetup): MiniSetup = logExceptions {
    // At this point, miniOptions has already been through mappers, so all file paths have been translated. However,
    // options may contain explicit output paths.
    val options = miniSetup.options()
    val javacOptions = options.javacOptions.map(translateOptions)
    val scalacOptions = options.scalacOptions.map(translateOptions)
    miniSetup.withOptions(options.withJavacOptions(javacOptions).withScalacOptions(scalacOptions))
  }

  override def mapSourceDir(sourceDir: Path): Path = translateFile(sourceDir)
  override def mapOutputDir(outputDir: Path): Path = translateFile(outputDir)
}
