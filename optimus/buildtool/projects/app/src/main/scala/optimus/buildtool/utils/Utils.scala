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

import java.io._
import java.lang.management.ManagementFactory
import java.net.URI
import java.nio.charset.StandardCharsets
import java.nio.charset.StandardCharsets.UTF_8
import java.nio.file._
import java.nio.file.attribute.BasicFileAttributes
import java.nio.file.attribute.PosixFilePermission
import java.nio.file.attribute.PosixFilePermissions
import java.{util => ju}
import java.util.{Collections, Set => JSet}
import optimus.stratosphere.bootstrap.WorkspaceRoot
import msjava.slf4jutils.scalalog.Logger
import msjava.slf4jutils.scalalog.getLogger
import optimus.buildtool.artifacts.Artifact
import optimus.buildtool.artifacts.ArtifactType
import optimus.buildtool.artifacts.Artifacts
import optimus.buildtool.artifacts.CompilerMessagesArtifact
import optimus.buildtool.artifacts.MessagePosition
import optimus.buildtool.config.NamingConventions
import optimus.buildtool.config.NamingConventions.incrString
import optimus.buildtool.config.ScopeId.RootScopeId
import optimus.buildtool.config.ScopeId
import optimus.buildtool.files.Directory.NoFilter
import optimus.buildtool.files.Directory.PathFilter
import optimus.buildtool.files._
import optimus.buildtool.trace.MessageTrace
import optimus.buildtool.utils.AssetUtils.atomicallyMove
import optimus.buildtool.utils.AsyncUtils.asyncTry
import optimus.exceptions.RTExceptionTrait
import optimus.graph.FlowControlException
import optimus.platform._
import optimus.utils.ErrorIgnoringFileVisitor
import xsbti.VirtualFile

import scala.collection.immutable.Seq
import scala.collection.mutable
import scala.util.control.NonFatal
import optimus.scalacompat.collection._

import scala.annotation.tailrec
import scala.collection.compat._

/**
 * Utility methods for dealing with files
 */
@entity private[optimus] object Utils {

  val LogSeparator = "====="

  val SuccessLog: Logger = getLogger(s"${getClass.getName}.success")
  val WarningLog: Logger = getLogger(s"${getClass.getName}.warning")
  val FailureLog: Logger = getLogger(s"${getClass.getName}.failure")

  @scenarioIndependent @node def outputPathForName(outDir: Directory, tpe: String, name: String): JarAsset = {
    val tpeDir = outDir.resolveDir(tpe)
    createDirectories(tpeDir)
    tpeDir.resolveJar(s"$name.jar")
  }

  def logPrefix(scopeId: ScopeId, traceType: MessageTrace, extra: Option[String] = None): String = {
    val extraStr = extra.map(e => s"-$e").getOrElse("")
    s"[$scopeId:${traceType.name}$extraStr] "
  }

  def recursivelyCopyAndCount(fromPath: Directory, toPath: Directory): Int =
    recursively(fromPath, toPath, new CountAccumulator) {
      case (_, to, Dir) =>
        Files.createDirectories(to)
        0
      case (from, to, File) =>
        Files.copy(from, to, StandardCopyOption.REPLACE_EXISTING)
        1
    }

  def recursivelyCopy(
      fromPath: Directory,
      toPath: Directory,
      posixFilePermissions: Option[JSet[PosixFilePermission]] = None,
      filter: PathFilter = NoFilter
  ): Seq[FileAsset] =
    recursively(fromPath, toPath, new SeqAccumulator[FileAsset], fileFilter = filter) {
      case (_, to, Dir) =>
        Files.createDirectories(to)
        Seq.empty
      case (from, to, File) =>
        Files.copy(from, to, StandardCopyOption.REPLACE_EXISTING)
        if (!Utils.isWindows) {
          posixFilePermissions.foreach { permissions =>
            Files.setPosixFilePermissions(to, permissions)
          }
        }
        Seq(FileAsset(to))
    }

  def recursivelyMove(fromPath: Directory, toPath: Directory): Int =
    recursively(fromPath, toPath, new CountAccumulator) {
      case (_, to, Dir) =>
        Files.createDirectories(to)
        0
      case (from, to, File) =>
        AssetUtils.atomicallyMove(FileAsset(from), FileAsset(to), replaceIfExists = true)
        1
      case (from, _, PostDir) =>
        Files.delete(from)
        0
    }

  @async def recursivelySync(fromPath: Directory, toPath: Directory): Int = {
    val previousTargetPaths: mutable.Map[Path, Boolean] = Directory
      .findAssetsUnsafe(toPath)
      .iterator // Non-RT Utils methods aren't annotated @impure for now
      .map {
        case f: FileAsset      => f.path -> true
        case d: DirectoryAsset => d.path -> false
      }
      .convertTo(mutable.Map)
    val numSynced = recursively(fromPath, toPath, new CountAccumulator) {
      case (_, to, Dir) if previousTargetPaths.remove(to).isEmpty =>
        Files.createDirectories(to)
        0
      case (from, to, File) =>
        Files.copy(from, to, StandardCopyOption.REPLACE_EXISTING)
        previousTargetPaths.remove(to)
        1
    }

    // delete any paths in the target that are no longer present in the source
    val (files, dirs) = previousTargetPaths.partition { case (_, isFile) => isFile }
    files.foreach { case (f, _) => Files.delete(f) }
    // reverse sort to delete leaf dirs first
    dirs.toSeq.sorted.reverse.foreach { case (d, _) => Files.delete(d) }

    numSynced
  }

  sealed trait VisitType
  case object Dir extends VisitType
  case object File extends VisitType
  case object PostDir extends VisitType

  sealed trait Accumulator[T] {
    def lifted(maybeValue: Option[T]): Unit
    def accumulatedValues: T
  }
  class CountAccumulator extends Accumulator[Int] {
    private var count: Int = 0
    override def lifted(maybeValue: Option[Int]): Unit = {
      count += maybeValue.getOrElse(0)
    }
    def accumulatedValues: Int = count
  }
  class SeqAccumulator[T] extends Accumulator[Seq[T]] {
    private val paths: mutable.Builder[T, List[T]] = List.newBuilder[T]
    override def lifted(values: Option[Seq[T]]): Unit = {
      values.foreach(paths ++= _)
    }
    def accumulatedValues: Seq[T] = paths.result()
  }

  def recursively[T](
      fromPath: Directory,
      toPath: Directory,
      accumulator: Accumulator[T],
      maxDepth: Int = Int.MaxValue,
      fileFilter: PathFilter = NoFilter)(
      f: PartialFunction[(Path, Path, VisitType), T]
  ): T = {
    val visitor: SimpleFileVisitor[Path] = new SimpleFileVisitor[Path] {
      private def mapPath(path: Path): Path = mapPath(relativeToFromPath(path))
      private def mapPath(relativePath: RelativePath): Path = toPath.path.resolve(relativePath.pathString)
      private def relativeToFromPath(path: Path): RelativePath = RelativePath(fromPath.path.relativize(path))

      override def preVisitDirectory(dir: Path, attrs: BasicFileAttributes): FileVisitResult = {
        val newDir = mapPath(dir)
        accumulator.lifted(f.lift((dir, newDir, Dir)))
        FileVisitResult.CONTINUE
      }

      override def visitFile(file: Path, attrs: BasicFileAttributes): FileVisitResult = {
        val relativePath = relativeToFromPath(file)
        if (fileFilter(relativePath.path, attrs)) {
          val newFile = mapPath(relativePath)
          accumulator.lifted(f.lift((file, newFile, File)))
        }
        FileVisitResult.CONTINUE
      }

      override def postVisitDirectory(dir: Path, exc: IOException): FileVisitResult = {
        val newDir = mapPath(dir)
        accumulator.lifted(f.lift((dir, newDir, PostDir)))
        FileVisitResult.CONTINUE
      }
    }
    Files.walkFileTree(fromPath.path, Collections.emptySet(), maxDepth, visitor)
    accumulator.accumulatedValues
  }

  def localize(file: FileAsset, depCopyPath: Directory): FileAsset =
    depCopyPath.resolveFile(depCopySuffix(file.pathString))
  def localize(dir: Directory, depCopyPath: Directory): Directory =
    depCopyPath.resolveDir(depCopySuffix(dir.pathString))

  // 5 == "//xx/".length
  private def depCopySuffix(pathString: String): String = pathString.substring(5)

  /**
   * Facilitates an atomic write of targetFile by generating a temporary file name, passing this to the specified fn
   * (which is expected to create that temporary file with the required content), and then atomically moving the
   * temporary file to the targetFile (clobbering it if it already exists).
   *
   * Note that this method does not create the temporary file - that is the responsibility of the supplied fn. If the fn
   * fails we do attempt to delete the temporary file (if it exists).
   */
  def atomicallyWrite[T](targetFile: FileAsset, replaceIfExists: Boolean = false, localTemp: Boolean = false)(
      fn: Path => T
  ): T = AssetUtils.atomicallyWrite(targetFile, replaceIfExists, localTemp)(fn)

  @async def atomicallyWrite$NF[T](targetFile: FileAsset, replaceIfExists: Boolean = false, localTemp: Boolean = false)(
      fn: AsyncFunction1[Path, T]
  ): T = {
    val tempFile = NamingConventions.tempFor(targetFile, localTemp)
    asyncTry {
      val t = fn(tempFile.path)
      if (!tempFile.existsUnsafe)
        throw new RuntimeException(s"Temp file $tempFile does not exist for $targetFile")
      if (!Files.isDirectory(targetFile.parent.path))
        throw new RuntimeException(s"Parent directory does not exist for $targetFile")
      atomicallyMove(tempFile, targetFile, replaceIfExists)
      t
    } asyncFinally Files.deleteIfExists(tempFile.path)
  }

  /**
   * Calls atomicallyWrite if the file doesn't exist, else does nothing.
   *
   * This is a very commonly used pattern in OBT: Since we include a hash of the inputs needed to compute the file in
   * the file's name name, and since we ensure that writes are atomic, we can safely assume that if the file exists with
   * the expected name, then it has the expected content so we don't need to compute and write it again.
   */
  def atomicallyWriteIfMissing[T](targetFile: FileAsset)(fn: Path => T): Option[T] =
    AssetUtils.atomicallyWriteIfMissing(targetFile)(fn)

  @async def atomicallyWriteIfMissing$NF[T](targetFile: FileAsset)(fn: AsyncFunction1[Path, T]): Option[T] =
    if (!targetFile.existsUnsafe) Some(atomicallyWrite$NF(targetFile)(fn)) else None

  def isWindows: Boolean = OsUtils.isWindows

  final case class ProcessDetails(pid: String, host: String)
  private val processDetails = ManagementFactory.getRuntimeMXBean.getName.split("@") match {
    case Array(pid, host) => Some(ProcessDetails(pid, host))
    case _                => None
  }

  def processId: Option[String] = processDetails.map(_.pid)
  def hostName: Option[String] = processDetails.map(_.host)

  // Only safe to use for directories which won't be deleted while OBT is running
  // @node-cached because Files.createDirectories is slow
  @scenarioIndependent @node def createDirectories(dir: Directory): Unit =
    Files.createDirectories(dir.path)

  def outputDirFor(bundleOutDir: Directory, tpe: ArtifactType, discriminator: Option[String]): Directory =
    discriminator match {
      case Some(d) => bundleOutDir.resolveDir(tpe.name).resolveDir(d)
      case None    => bundleOutDir.resolveDir(tpe.name)
    }

  @scenarioIndependent @node def outputPathFor(
      bundleOutDir: Directory,
      tpe: ArtifactType,
      discriminator: Option[String],
      fingerprintHash: String,
      id: ScopeId,
      incremental: Boolean
  ): FileAsset = {
    val s = tpe.suffix
    val tpeDir = outputDirFor(bundleOutDir, tpe, discriminator)
    createDirectories(tpeDir)
    val incr = incrString(incremental)
    val prefix = if (id == RootScopeId) "" else s"${id.properPath}."
    val fname = s"$prefix$incr$fingerprintHash.$s"
    tpeDir.resolveFile(fname)
  }

  def outputPathForType(file: FileAsset, newType: ArtifactType): FileAsset = {
    val workDir = file.parent.parent
    val fname = file.name
    val dot = fname.lastIndexOf(".")
    val suffixless = if (dot > 0) fname.substring(0, dot) else fname
    val newName = s"$suffixless.${newType.suffix}"
    val newDir = workDir.resolveDir(newType.name)
    Files.createDirectories(newDir.path)
    newDir.resolveFile(newName)
  }

  def sparseJarDir(buildDir: Directory): Directory = buildDir.resolveDir(NamingConventions.Sparse)
  val SparseJarInstallVersion = "local"

  private val SRC = "/src/"
  private def pathPrefix(filePath: String): Option[String] = {
    val i = filePath.indexOf(SRC)
    if (i > 0)
      Some(filePath.substring(0, i))
    else None
  }

  object LocatedCompilerMessages {
    def unapply(a: Artifact): Option[(String, CompilerMessagesArtifact)] = a match {
      case ma: CompilerMessagesArtifact =>
        for {
          m <- ma.messages.find(m => m.pos.exists(_.filepath.contains(SRC)))
          p <- m.pos
          prefix <- pathPrefix(p.filepath)
        } yield (prefix, ma)
      case _ => None
    }
  }

  object LocatedVirtualFile {
    def unapply(vf: VirtualFile): Option[MessagePosition] =
      if (!vf.id.contains(SRC)) None
      else {
        val prefix = vf.id.substring(0, vf.id.indexOf(SRC))
        val fictiveFile = prefix + SRC
        Some(MessagePosition(fictiveFile))
      }
  }

  def workspaceLocalString(str: String, oldDir: Directory, newDir: Directory): String =
    replaceDirectory(str, oldDir, newDir.pathString)

  def replaceDirectory(str: String, dir: Directory, replacement: String): String = {
    val oldDirFormats = dir.allPathFormats
    oldDirFormats.foldLeft(PathUtils.platformIndependentString(str)) { (accum, format) =>
      accum.replace(format, replacement)
    }
  }

  def writeStringsToFile(p: Path, strs: Iterable[String]): Unit = {
    // truncate and overwrite an existing file, or create the file if it doesn't initially exist
    val out = new PrintStream(new BufferedOutputStream(Files.newOutputStream(p)), false, StandardCharsets.UTF_8.name())
    try strs.foreach(out.println)
    finally out.close()
  }

  /**
   * Like Seq#distinct except that it preserves only the last occurrence of each entry rather than only the first.
   *
   * This is useful for classpath ordering, because if for example:
   *
   *   - A depends on C and B
   *   - B depends on C, and monkey patches it
   *
   * Then a depth first traversal yields A C B C, which distincts to A C B, thus losing the monkey patch in B. If we
   * instead distinctLast, we get A B C. In general, every entry will appear in its lowest precedence, allowing anyone
   * who depends on it (and may therefore patch it) to go first.
   */
  def distinctLast[A](s: Seq[A]): Seq[A] = s.reverse.distinct.reverse

  def distinctLast[A](as: Artifacts): Artifacts = Artifacts(distinctLast(as.scope), distinctLast(as.upstream))

  /**
   * Find the jar where a class is defined.
   */
  def findJar(clazz: Class[_]): Path = {
    val resource = clazz.getCanonicalName.replace('.', '/') + ".class"
    val url = this.getClass.getClassLoader.getResource(resource)
    assert(url ne null, s"Can't find resource $resource")
    val jarUri = url.toURI.toString
    // Expect something like:  jar:file://foo/baz/piffle.jar!blort/confustication/MyClass
    assert(jarUri.startsWith("jar:file:"), s"Unexpected URI $jarUri")
    val parts = jarUri.substring(4).split("!")
    assert(parts.size == 2, s"Unexpected URI parts ${parts.mkString("Array(", ", ", ")")}")
    val jarPath = Paths.get(URI.create(parts.head))
    assert(Files.exists(jarPath), s"$jarPath doesn't exist")
    jarPath
  }

  def recursiveSize(root: Directory): Long = {
    object visitor extends ErrorIgnoringFileVisitor {
      var result: Long = _
      override def visitFile(file: Path, attrs: BasicFileAttributes): FileVisitResult = {
        result += attrs.size()
        FileVisitResult.CONTINUE
      }
    }
    Files.walkFileTree(root.path, visitor)
    visitor.result
  }

  def notNull[T <: AnyRef](t: T, msg: => String): T = {
    assert(t ne null, msg)
    t
  }

  def resolveWorkspaceAndSrcRoots(
      wsRootOpt: Option[Directory],
      srcRootOpt: Option[Directory],
      pwd: Path = Paths.get("").toAbsolutePath): (Directory, Directory) = {
    def fail(err: String): Nothing =
      throw new IllegalArgumentException(
        s"$err; please run from <workspace>/src or specify path(s) using --workspaceDir and/or " +
          "--sourceDir and verify that you are using correct path(s)")

    val (wsRoot, srcRoot) = (wsRootOpt, srcRootOpt) match {
      case (Some(ws), Some(src)) => (ws, src)
      case (Some(ws), None)      => (ws, ws.resolveDir("src"))
      case (None, Some(src))     => (src.parent, src)
      case (None, None) =>
        val ws = Option(WorkspaceRoot.findIn(pwd))
          .map(Directory(_))
          .getOrElse(fail(s"Cannot infer the src directory from the current working directory ($pwd)"))
        (ws, ws.resolveDir("src"))
    }

    if (!Files.isDirectory(wsRoot.path)) fail(s"workspaceDir ${wsRoot.path} doesn't exist")
    if (!Files.isDirectory(srcRoot.path)) fail(s"sourceDir ${srcRoot.path} doesn't exist")

    (wsRoot, srcRoot)
  }

  def freeSpaceBytes(p: Pathed): Option[Long] =
    try {
      @tailrec def existingPath(p: Path): Path =
        if (Files.exists(p)) p
        else {
          val parent = p.getParent
          if (parent != null) existingPath(parent)
          else p
        }
      Some(Files.getFileStore(existingPath(p.path)).getUsableSpace)
    } catch {
      case NonFatal(t) =>
        log.warn(s"Failed to determine free space for $p", t)
        None
    }

  /**
   * Returns the String form of `i`, prefixed with sufficient zeros to ensure correct lexical sorting up to and
   * including `max`.
   */
  def sortableInt(i: Int, max: Int): String = {
    val sigFigs = if (max > 0) math.log10(max).toInt + 1 else 1
    s"%0${sigFigs}d".format(i)
  }

  object StatisticsKey {
    val Total = "total"
    val Mean = "mean"
    val Count = "count"
    val Min = "min"
    val Tenth = "10th"
    val Twentieth = "20th"
    val Thirtieth = "30th"
    val Fortieth = "40th"
    val Fiftieth = "50th"
    val Sixtieth = "60th"
    val Seventieth = "70th"
    val Eightieth = "80th"
    val Ninetieth = "90th"
    val NinetyFifth = "95th"
    val NinetyNinth = "99th"
    val Max = "max"
    val Deciles: Seq[String] =
      Seq(Min, Tenth, Twentieth, Thirtieth, Fortieth, Fiftieth, Sixtieth, Seventieth, Eightieth, Ninetieth, Max)
    val Centiles: Seq[String] =
      Seq(
        Min,
        Tenth,
        Twentieth,
        Thirtieth,
        Fortieth,
        Fiftieth,
        Sixtieth,
        Seventieth,
        Eightieth,
        Ninetieth,
        NinetyFifth,
        NinetyNinth,
        Max
      )
  }
  import StatisticsKey._

  def stats(unsorted: Seq[Long]): Map[String, Long] = {
    val sorted = unsorted.sorted
    val total = sorted.sum
    val size = sorted.size
    val count: Map[String, Long] = Map(
      Count -> size
    )
    val core: Map[String, Long] =
      if (size > 0)
        Map(
          Total -> total,
          Mean -> total / size
        )
      else Map.empty

    count ++ core
  }

  def centiles[A: Ordering](unsorted: Seq[A]): Seq[(String, A)] = {
    val sorted = unsorted.sorted
    val count = sorted.size
    def percentile(pos: Int): A = {
      if (sorted.isEmpty) throw new UnsupportedOperationException("empty.percentile")
      require(pos >= 0 && pos <= 100)
      sorted((count - 1) * pos / 100)
    }
    if (count > 1)
      Seq(
        Min -> sorted.min,
        Tenth -> percentile(10),
        Twentieth -> percentile(20),
        Thirtieth -> percentile(30),
        Fortieth -> percentile(40),
        Fiftieth -> percentile(50),
        Sixtieth -> percentile(60),
        Seventieth -> percentile(70),
        Eightieth -> percentile(80),
        Ninetieth -> percentile(90),
        NinetyFifth -> percentile(95),
        NinetyNinth -> percentile(99),
        Max -> sorted.max
      )
    else Nil
  }
  val javaHome: Directory = Directory(Paths get System.getProperty("java.home"))
  val javaClassVersion: String = System.getProperty("java.class.version")
  val javaClassVersionTag = s"java.class.version=$javaClassVersion"
  val javaSpecVersion: String = System.getProperty("java.specification.version")
  val javaSpecVersionTag = s"java.specification.version=$javaSpecVersion"

  def generatePropertiesFileContent(props: Map[String, String]): String = {
    import scala.jdk.CollectionConverters._
    val tmpProp = new ju.Properties()
    tmpProp.addAll(props.asJava)
    val result = new StringWriter()
    tmpProp.store(result, /* no comments */ null)
    result.toString
      .replace("\r\n", "\n") // Normalize line termination for cross-platform consistency, favoring unix
      .split("\n")
      .drop(1)
      .mkString("\n") // Strip first line comment, as it has a timestamp (impure)
  }

  def loadProperties(in: InputStream): ju.Properties =
    try {
      val result = new ju.Properties()
      result.load(in)
      result
    } finally in.close()

  def durationString(millis: Long): String = {
    val secs = millis / 1000.0 % 60
    val mins = millis / 60000
    if (mins > 0) f"$mins%,dm${secs.toInt}%ds" else if (secs >= 1.0) f"$secs%.3gs" else s"${millis}ms"
  }

  @tailrec def rootCause(t: Throwable): Throwable = t.getCause match {
    case null  => t
    case cause => rootCause(cause)
  }

  val ExecuteBits: JSet[PosixFilePermission] = PosixFilePermissions.fromString("rwxr-xr-x")

  class OsVersionMismatchException(osVersion: String)
      extends RuntimeException(
        s"Mismatched OS version - using $osVersion but true version is ${OsUtils.osVersion}"
      )
      with FlowControlException
      with RTExceptionTrait

  object OsVersionMismatch {
    def unapply(t: Throwable): Option[(Throwable, OsVersionMismatchException)] = rootCause(t) match {
      case e: OsVersionMismatchException => Some((t, e))
      case _                             => None
    }
  }

  @impure def writePropertiesToFile(file: FileAsset, properties: Map[String, String]): Unit = {
    val content = properties.map { case (k, v) => s"$k=$v" }.toList.sorted
    Files.write(file.path, content.mkString("\n").getBytes(UTF_8))
  }
}

// Wrap values that are irrelevant for cache keys.
class Hide[T](val get: T) {
  override def hashCode(): Int = 0
  override def equals(o: Any): Boolean = o.isInstanceOf[Hide[_]]
  override def toString: String = s"Hide($get)"
}
object Hide {
  def apply[T](hidden: T) = new Hide(hidden)
}
