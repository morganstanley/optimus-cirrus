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
package optimus.buildtool.compilers.zinc

import java.io.File
import java.io.InputStream
import java.nio.file.Files
import java.nio.file.Path
import java.nio.file.Paths
import java.util

import net.openhft.hashing.LongHashFunction
import optimus.buildtool.artifacts.ExternalClassFileArtifact
import optimus.buildtool.files.Asset
import optimus.buildtool.files.JarAsset
import optimus.buildtool.files.Pathed
import optimus.buildtool.utils.HashedContent
import optimus.buildtool.utils.Hashing
import optimus.buildtool.utils.PathUtils
import optimus.platform._
import sbt.internal.inc
import sbt.internal.inc.FarmHash
import xsbti.BasicVirtualFileRef
import xsbti.FileConverter
import xsbti.PathBasedFile
import xsbti.VirtualFile
import xsbti.VirtualFileRef
import xsbti.compile.analysis.ReadStamps
import xsbti.compile.analysis.Stamp

object ZincVirtualFiles {

  def toVirtualFileRef(p: Path): VirtualFileRef = toVirtualFileRef(PathUtils.platformIndependentString(p))

  def toVirtualFileRef(pathStr: String): VirtualFileRef = {
    toVirtualFileImpl(pathStr).getOrElse {
      // anything else is probably a source file, and we need to need to use the BasicVirtualFileRef to avoid
      // problems in change detection (since BasicVirtualFileRef and VirtualSourceFile cooperate on equality)
      VirtualFileRef.of(pathStr)
    }
  }

  def toVirtualFile(p: Path): VirtualFile = toVirtualFile(PathUtils.platformIndependentString(p))

  def toVirtualFile(pathStr: String): VirtualFile = {
    toVirtualFileImpl(pathStr).getOrElse {
      // Could be a java module (eg. //modules/java.base/java/lang/Object.class)
      SimpleVirtualFile(pathStr.intern())
    }
  }

  private def toVirtualFileImpl(pathStr: String): Option[VirtualFile] = {
    val idx = pathStr.indexOf('!')
    // intern the jar part of foo.jar!bar.class, and store it efficiently
    if (idx > 0) Some(ClassInJarVirtualFile(pathStr.substring(0, idx).intern(), pathStr.substring(idx + 1)))
    // intern foo.jar, and use our more efficient representation (since BasicVirtualFileRef weirdly holds three Strings)
    else if (pathStr.endsWith(".jar")) Some(SimpleVirtualFile(pathStr.intern()))
    else None
  }
}

/**
 * In-memory source file. Not a case class because we don't want to use the raw Path for equality (BasicVirtualFileRef
 * implements id-based equality for us).
 */
private[zinc] class VirtualSourceFile private (p: Path, c: HashedContent)
    extends BasicVirtualFileRef(PathUtils.platformIndependentString(p))
    with VirtualFile {
  override def contentHash(): Long = LongHashFunction.farmNa().hashChars(c.hash)
  override def input(): InputStream = c.contentAsInputStream

  override def toString: String = s"VirtualSourceFile($id@${c.hash})"
}
object VirtualSourceFile {
  def apply(p: Path, c: HashedContent): VirtualSourceFile = new VirtualSourceFile(p, c)
}

/** Classes in jars - foo/bar/baz.jar!my/great/Class.class, etc. */
private[zinc] final case class ClassInJarVirtualFile private (jar: String, clazz: String)
    extends VirtualFileRef
    with PathBasedFile {
  // it seems empirically that this is only called at the moment of writing protobufs, so it's a useful memory
  // optimization to avoid creating it until then
  override def id: String = s"$jar!$clazz"
  override def name: String = ZincUtils.substringAfterLast(clazz, '/')
  override def names: Array[String] = jar.split("/") ++ clazz.split("/")

  override def toPath: Path = Paths.get(id)
  override def contentHash: Long = ???
  override def input: InputStream = ???
}

/**
 * Typically these will be external dependency jars. We don't use PlainVirtualFile because it passes non-platform
 * independent Path#toString to the BasicVirtualFileRef.
 */
private[zinc] final case class SimpleVirtualFile private (override val id: String)
    extends BasicVirtualFileRef(id)
    with PathBasedFile {
  override def contentHash(): Long = ???
  override lazy val toPath: Path = Paths.get(id)
  override def input(): InputStream = Files.newInputStream(toPath)
}
object SimpleVirtualFile {
  def apply(p: Path): SimpleVirtualFile = SimpleVirtualFile(Pathed.pathString(p).intern())
}

private[zinc] class ObtZincFileConverter(
    virtualSources: Map[VirtualFileRef, VirtualFile], // in practice it's Map[VirtualSourceFile, VirtualSourceFile]
    virtualJars: Map[Path, VirtualFile] // in practice it's Map[Path, SimpleVirtualFile]
) extends FileConverter {
  override def toPath(ref: VirtualFileRef): Path = ref match {
    case pbf: PathBasedFile => pbf.toPath
    case _                  => Paths.get(ref.id)
  }
  override def toVirtualFile(ref: VirtualFileRef): VirtualFile = ref match {
    case v: VirtualFile => v
    case r              => virtualSources.getOrElse(r, ZincVirtualFiles.toVirtualFile(ref.id))
  }
  override def toVirtualFile(path: Path): VirtualFile =
    virtualJars.getOrElse(path, ZincVirtualFiles.toVirtualFile(path))
}

@entity private[zinc] object ObtReadStamps extends ReadStamps {
  // this is the simplest way to get hold of an EmptyStamp...
  private val emptyStamp = inc.Stamp.getStamp(Map.empty, new File(""))
  private val zeroStamp = FarmHash.fromLong(0)

  override def library(libraryFile: VirtualFileRef): Stamp = libraryFile match {
    case s: SimpleVirtualFile =>
      // anything from the java platform modules is effectively immutable - they crop up in various ways
      if (Asset.isJdkPlatformPath(s.id)) zeroStamp
      else if (s.toPath.getFileSystem.toString == "jrt:/") zeroStamp
      else hashLibrary(s.toPath)
    // this is here so you can put a breakpoint on it if needed
    case x => throw new MatchError(x)
  }

  @entersGraph def hashLibrary(library: Path): Stamp = hashLibraryNode(library)
  @node def hashLibraryNode(library: Path): Stamp = {
    // TODO (OPTIMUS-38945): We should use OBT's artifact hashes here.
    // They should be added to the SimpleVirtualFile, but we need a lot of plumbing to get there
    if (Hashing.isAssumedImmutable(JarAsset(library))) zeroStamp
    else {
      ExternalClassFileArtifact.witnessMutableExternalArtifactState()
      if (Files.exists(library)) FarmHash.ofPath(library)
      else zeroStamp
    }
  }

  // the data retrieved from this seems ultimately unused, but it's safer to return empty stamp than constant zero stamp
  override def product(compilationProduct: VirtualFileRef): Stamp = emptyStamp

  override def source(internalSource: VirtualFile): Stamp =
    internalSource match {
      case o: VirtualSourceFile => FarmHash.fromLong(o.contentHash())
      case f: VirtualFile       => FarmHash.ofFile(f)
    }

  override def getAllLibraryStamps: util.Map[VirtualFileRef, Stamp] = util.Collections.emptyMap()
  override def getAllSourceStamps: util.Map[VirtualFileRef, Stamp] = util.Collections.emptyMap()
  override def getAllProductStamps: util.Map[VirtualFileRef, Stamp] = util.Collections.emptyMap()
}
