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

import msjava.slf4jutils.scalalog

import java.io.ByteArrayOutputStream
import java.io.FilterOutputStream
import java.io.OutputStream
import java.nio.file.Files
import java.nio.file.Path
import java.nio.file.Paths
import java.nio.file.StandardCopyOption
import java.nio.file.StandardOpenOption
import java.nio.file.attribute.FileTime
import java.time.Instant
import java.util.jar
import java.util.jar.Attributes.Name
import java.util.jar.JarFile
import java.util.jar.JarInputStream
import java.util.jar.JarOutputStream
import java.util.zip.ZipEntry
import optimus.buildtool.artifacts.CachedMetadata
import optimus.buildtool.artifacts.CompilationMessage
import optimus.buildtool.artifacts.MessagesMetadata
import optimus.buildtool.config.JarConfiguration
import optimus.buildtool.files.Directory
import optimus.buildtool.files.JarAsset
import optimus.buildtool.files.RelativePath
import optimus.buildtool.utils.JarUtils.jarFileSystem
import optimus.buildtool.utils.JarUtils.nme
import optimus.platform._
import optimus.platform.metadatas.internal.MetaDataFiles
import spray.json.JsonFormat

import scala.annotation.tailrec
import scala.jdk.CollectionConverters._
import scala.collection.immutable.Seq
import scala.collection.mutable

class MismatchedManifestException(val key: AnyRef, val manifests: Seq[jar.Manifest], val values: Seq[AnyRef])
    extends IllegalArgumentException(
      s"Mismatched values for attribute '$key' when merging manifests: ${values.mkString(", ")}"
    )

/**
 * Utility methods for dealing with .jar files
 */
object Jars {

  private val log = scalalog.getLogger(this)

  // Note that we don't use JarAssets here, since the classpath may be relative
  def createPathingManifest(classpath: Seq[Path], premainClass: Option[String]): jar.Manifest = {
    val manifest = createManifest()
    if (classpath.nonEmpty)
      updateManifest(manifest, jar.Attributes.Name.CLASS_PATH -> formatClasspathForManifest(classpath))
    premainClass.foreach { cls =>
      updateManifest(
        manifest,
        nme.PremainClass -> cls,
        nme.CanRetransformClasses -> "true",
        nme.CanSetNativeMethodPrefix -> "true")
    }
    manifest
  }

  def createManifest(jarConfig: JarConfiguration): jar.Manifest = {
    val manifestEntries = jarConfig.manifest.map { case (k, v) => new Name(k) -> v }.toSeq
    createManifest(manifestEntries: _*)
  }

  def createManifest(entries: (Name, String)*): jar.Manifest =
    updateManifest(new jar.Manifest(), entries: _*)

  def updateManifest(manifest: jar.Manifest, entries: (Name, String)*): jar.Manifest = {
    val attrs = manifest.getMainAttributes
    // n.b. we always set the MANIFEST_VERSION even when updating existing manifests, because if it's missing (and
    // sometimes it is even in loaded manifests), many things don't work - it's less hassle to just always (re)set it
    attrs.put(jar.Attributes.Name.MANIFEST_VERSION, "1.0")
    entries.foreach { case (n, v) => attrs.put(n, v) }
    manifest
  }

  /**
   * Applies the de facto formatting rules to turn a sequence of Paths in to a classpath string suitable for a manifest.
   * Note that we don't use JarAssets here, since the classpath may be relative.
   */
  def formatClasspathForManifest(classpath: Seq[Path]): String = {
    val formattedClasspath = classpath.map(PathUtils.uriString)
    manifestPath(formattedClasspath)
  }

  def manifestPath(formattedPaths: Seq[String]): String = {
    val sb = new StringBuilder(formattedPaths.map(_.length + 1).sum)
    formattedPaths.addString(sb, " ").toString
  }

  /**
   * parses a classpath from the de facto format in manifest Class-Path strings (which doesn't seem to be documented by
   * Oracle, or at least their documentation doesn't match reality)
   */
  def parseManifestClasspath(jarAsset: JarAsset, formattedClasspath: String): Seq[JarAsset] = {
    formattedClasspath.split(' ').toIndexedSeq.map { p =>
      // we're not using java.net.URL to parse these because it gets confused and thinks the first path element is a host
      val path = if (p.startsWith(PathUtils.FileProtocol)) {
        val subPath = p.substring(PathUtils.FileProtocol.length)
        utils.ClassPathUtils.demanglePathFromUrlPath(subPath)
      } else Paths.get(p)
      if (path.isAbsolute) JarAsset(path) else jarAsset.parent.resolveFile(RelativePath(path)).asJar
    }
  }

  def extractManifestClasspath(jarAsset: JarAsset, manifest: jar.Manifest): Seq[JarAsset] =
    JarUtils
      .load(manifest, jar.Attributes.Name.CLASS_PATH)
      .map(parseManifestClasspath(jarAsset, _))
      .getOrElse(Nil)

  def extractClassJar(jarAsset: JarAsset, manifest: jar.Manifest): Option[JarAsset] =
    JarUtils.load(manifest, nme.ClassJar).map(f => jarAsset.parent.resolveJar(f))

  private def extractSemicolonSeparatedValue(manifest: jar.Manifest, name: Name): Seq[String] =
    JarUtils.load(manifest, name, ";")

  def extractManifestExtraFiles(manifest: jar.Manifest): Seq[Path] =
    extractSemicolonSeparatedValue(manifest, nme.ExtraFiles).map(Paths.get(_))

  def extractManifestJniPaths(manifest: jar.Manifest): Seq[String] =
    extractSemicolonSeparatedValue(manifest, nme.ExternalJniPath)

  def extractManifestModuleLoads(manifest: jar.Manifest): Seq[String] =
    extractSemicolonSeparatedValue(manifest, nme.ModuleLoads)

  /** creates a jar at outputPath and writes the specified manifest in to META-INF/MANIFEST.MF */
  def writeManifestJar(outputJar: JarAsset, manifest: jar.Manifest, extraEntries: List[(String, Path)] = Nil): Unit = {
    val manifestJar = new JarOutputStream(Files.newOutputStream(outputJar.path))
    try {
      manifestJar.putNextEntry(zipEntry(JarFile.MANIFEST_NAME))
      writeManifestToStreamWithLf(manifest, manifestJar)
      extraEntries.foreach { case (pathInJar, pathToCopy) =>
        manifestJar.putNextEntry(zipEntry(pathInJar))
        Files.copy(pathToCopy, manifestJar)
      }
    } finally manifestJar.close()
  }

  def fingerprint(manifest: jar.Manifest): Seq[String] = {
    val mainAttrs = fingerprint("Main", manifest.getMainAttributes)
    val entries = manifest.getEntries.asScala.toSeq.sortBy(_._1).flatMap { case (name, attrs) =>
      fingerprint(name, attrs)
    }
    mainAttrs ++ entries
  }

  private def fingerprint(name: String, attrs: jar.Attributes): Seq[String] =
    attrs.asScala.map { case (k, v) => s"[$name]$k=$v" }.toIndexedSeq.sorted

  /**
   * Writes a manifest to a stream with LF line endings. Does not close the stream.
   *
   * According to the Jar spec LF is valid, but Manifest#write always uses CRLF, and that's not good for OBT hashing
   * because we always convert CRLF to LF in text files.
   */
  def writeManifestToStreamWithLf(manifest: jar.Manifest, out: OutputStream): Unit = {
    val byteStream = new ByteArrayOutputStream
    manifest.write(byteStream)
    val bytes = byteStream.toByteArray
    val newLength = Hashing.convertCrLfToLfInPlace(bytes, bytes.length)
    out.write(bytes, 0, newLength)
  }

  /** reads the manifest from the META-INF/MANIFEST.MF in the specified jar */
  def readManifestJar(inputJar: JarAsset): Option[jar.Manifest] = {
    // because we are using this methods to update jars, we make sure not to lock the file.
    val manifestJar = new JarInputStream(Files.newInputStream(inputJar.path, StandardOpenOption.READ))
    try Option(manifestJar.getManifest)
    finally manifestJar.close()
  }

  /** Creates a new targetJar and copies the files from sourceJars into it. Optimizes the case of only one sourceJar */
  def mergeJarContentAndManifests(
      sourceJars: Seq[JarAsset],
      targetJar: JarAsset,
      manifest: Option[jar.Manifest] = None,
      compress: Boolean): Long = {
    if (sourceJars.size == 1 && manifest.isEmpty && !compress) {
      // optimize case of exactly one input
      val one = sourceJars.head.path
      Files.copy(one, targetJar.path, StandardCopyOption.REPLACE_EXISTING)
      Files.size(one)
    } else {
      // for zero or more than one input, first merge the manifests (if any), then write the jar
      val manifests = manifest ++ sourceJars.flatMap(readManifestJar)
      val mergedManifest = mergeManifests(manifests.toList)
      mergeAndHashJarContentGivenManifest(sourceJars, targetJar, mergedManifest, compress)
    }
  }

  def mergeAndHashJarContentGivenManifest(
      sourceJars: Seq[JarAsset],
      targetJar: JarAsset,
      manifest: Option[jar.Manifest] = None,
      compress: Boolean
  ): Long = {
    val countingStream = new CountingOutputStream(Files.newOutputStream(targetJar.path))
    try {
      val outputJar = () => new ConsistentlyHashedJarOutputStream(countingStream, manifest, compress)
      mergeContent(sourceJars, outputJar, Map.empty)(_ => ())
    } finally countingStream.close()
    countingStream.bytesWritten
  }

  def mergeContent(
      sourceJars: Seq[JarAsset],
      targetJar: JarAsset,
      tokens: Map[String, String],
      compress: Boolean
  )(extraJarContent: EnhancedJarOutputStream => Unit): Unit = {
    val outputJar = () => new UnhashedJarOutputStream(Files.newOutputStream(targetJar.path), None, compress)
    mergeContent(sourceJars, outputJar, tokens)(extraJarContent)
  }

  private def mergeContent(
      sourceJars: Seq[JarAsset],
      outputJar: () => EnhancedJarOutputStream,
      tokens: Map[String, String]
  )(extraJarContent: EnhancedJarOutputStream => Unit): Unit = {
    val allFiles = mutable.Buffer[(String, JarAsset)]()
    val metadataContents = mutable.Map[String, Seq[String]]()
    val servicesContents = mutable.Map[String, Seq[String]]()
    val output = outputJar()
    try {
      sourceJars.foreach { j =>
        val inputJar = JarUtils.jarFileSystem(j)
        try {
          val stream = Files.walk(inputJar.getPath("/"))
          try {
            stream.forEach { p =>
              val name = p.toString.stripPrefix("/")
              if (Files.isRegularFile(p)) {
                if (MetaDataFiles.metadataFileNames.contains(name)) {
                  val existingContent = metadataContents.getOrElse(name, Nil)
                  metadataContents += name -> (existingContent :+ Files.readString(p))
                } else if (p.startsWith("/META-INF/services")) {
                  val existingContent = servicesContents.getOrElse(name, Nil)
                  servicesContents += name -> (existingContent :+ Files.readString(p))
                } else if (
                  // manifest and hash are (re)generated automatically by the CHJOS so we exclude them here
                  name != JarFile.MANIFEST_NAME && name != Hashing.optimusHashPath && name != Hashing.optimusPerEntryHashPath
                ) {
                  allFiles.append((name, j))
                  val target = RelativePath(name)
                  if (tokens.isEmpty) output.copyInFile(p, target)
                  else output.writeFile(Files.readString(p), target, tokens)
                }
              }
            }
          } finally stream.close()
        } finally inputJar.close()
      }
      metadataContents.foreach { case (name, contents) =>
        val content = mergeMetadata(contents)
        val target = RelativePath(name)
        output.writeFile(content, target)
      }
      servicesContents.foreach { case (name, contents) =>
        val target = RelativePath(name)
        output.writeFile(contents.mkString("\n"), target)
      }
      extraJarContent(output)
    } finally {
      try output.close()
      catch {
        case e: DuplicateEntriesException =>
          val groupedFiles = allFiles.toGroupedMap
          val duplicates = e.files.map { file =>
            val jars = groupedFiles(file).map(_.pathString)
            s"$file (${jars.mkString(", ")})"
          }
          throw new IllegalArgumentException(s"Duplicate file entries: ${duplicates.mkString(", ")}")
      }
    }
  }

  @tailrec def mergeManifests(manifests: Seq[jar.Manifest]): Option[jar.Manifest] = manifests match {
    case Nil        => None
    case one +: Nil => Some(one)
    case one +: two +: others =>
      val combined = mergeManifests(one, two)
      mergeManifests(combined +: others)
  }

  def mergeManifests(one: jar.Manifest, two: jar.Manifest): jar.Manifest = {
    val combined = createManifest()
    val keys = one.getMainAttributes.keySet().asScala ++ two.getMainAttributes.keySet().asScala
    keys.foreach { k =>
      (one.getMainAttributes.get(k), two.getMainAttributes.get(k)) match {
        case (v, null) => combined.getMainAttributes.put(k, v)
        case (null, v) => combined.getMainAttributes.put(k, v)
        case (v1, v2) =>
          if (v1 == v2) combined.getMainAttributes.put(k, v1)
          else
            throw new MismatchedManifestException(k, Seq(one, two), Seq(v1, v2))
      }
    }
    combined
  }

  def mergeMetadata(contents: Seq[String]): String = {
    s"[${contents.map(c => c.trim().stripPrefix("[").stripSuffix("]")).mkString(",\n")}]"
  }

  def stampJarWithConsistentHash(jarAsset: JarAsset, compress: Boolean): Unit = {
    val manifest = readManifestJar(jarAsset).filterNot(_.getMainAttributes.isEmpty)
    AssetUtils.atomicallyWrite(jarAsset, replaceIfExists = true) { temp =>
      mergeAndHashJarContentGivenManifest(Seq(jarAsset), JarAsset(temp), manifest, compress)
    }
  }

  /** Creates a new [[ZipEntry]] with the given name and a modified date of 0, for RTness. */
  def zipEntry(name: String): ZipEntry = {
    val entry = new ZipEntry(name)
    entry.setLastModifiedTime(EpochFileTime)
    entry
  }
  private[this] val EpochFileTime = FileTime.from(Instant.EPOCH)

  def withJar[T](jarFile: JarAsset, create: Boolean = false)(res: Directory => T): T = {
    val fs = jarFileSystem(jarFile, create)
    try res(Directory.root(fs))
    finally fs.close()
  }
  @async def withJar$NF[T](jarFile: JarAsset, create: Boolean = false)(res: AsyncFunction1[Directory, T]): T = {
    val fs = jarFileSystem(jarFile, create)
    AsyncUtils.asyncTry(res(Directory.root(fs))).asyncFinally(fs.close())
  }

  def createJar(
      jar: JarAsset,
      messages: Seq[CompilationMessage],
      hasErrors: Boolean,
      contentRoot: Directory
  ): Seq[CompilationMessage] = createJar(jar, messages, hasErrors, Some(contentRoot))

  def createJar(
      jar: JarAsset,
      messages: Seq[CompilationMessage],
      hasErrors: Boolean,
      contentRoot: Option[Directory]
  ): Seq[CompilationMessage] = {
    import optimus.buildtool.artifacts.JsonImplicits._
    createJar(jar, MessagesMetadata(messages, hasErrors), contentRoot)()
    messages
  }

  private[buildtool] def createJar[A <: CachedMetadata: JsonFormat](
      jar: JarAsset,
      metadata: A,
      contentRoot: Option[Directory] = None
  )(f: ConsistentlyHashedJarOutputStream => Unit = _ => ()): Unit = {
    // we don't incrementally rewrite these jars, so might as well compress them and save the disk space
    val tempJarStream = new ConsistentlyHashedJarOutputStream(Files.newOutputStream(jar.path), None, compressed = true)
    try {
      AssetUtils.withTempJson(metadata) {
        tempJarStream.writeFile(_, CachedMetadata.MetadataFile)
      }
      // It is required that any @nodes calling this are RT in terms of the files that they write to tempDir, so it's
      // safe to call findFilesUnsafe here
      contentRoot.foreach(c =>
        Directory.findFilesUnsafe(c).foreach { f =>
          tempJarStream.copyInFile(f.path, c.relativize(f))
        })
      f(tempJarStream)
    } finally {
      tempJarStream.close()
      contentRoot.foreach(AssetUtils.recursivelyDelete(_))
    }
  }
}

private class CountingOutputStream(out: OutputStream) extends FilterOutputStream(out) {
  private[this] var count: Long = _
  def bytesWritten: Long = count
  override def write(b: Int): Unit = {
    out.write(b)
    count += 1
  }
  // (note that write(b: Array[Byte]) is redirected here too by FilterOutputStream)
  override def write(b: Array[Byte], off: Int, len: Int): Unit = {
    out.write(b, off, len)
    count += len
  }
}
