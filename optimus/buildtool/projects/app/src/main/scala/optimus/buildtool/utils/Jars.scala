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

import java.io.ByteArrayOutputStream
import java.io.OutputStream
import java.nio.file.Files
import java.nio.file.Path
import java.nio.file.Paths
import java.nio.file.StandardOpenOption
import java.nio.file.attribute.FileTime
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
import optimus.buildtool.files.Pathed
import optimus.buildtool.files.RelativePath
import optimus.buildtool.trace.ObtStats
import optimus.buildtool.trace.TaskTrace
import optimus.buildtool.utils.JarUtils.jarFileSystem
import optimus.buildtool.utils.JarUtils.nme
import optimus.platform._
import optimus.platform.metadatas.internal.MetaDataFiles
import sbt.internal.inc.zip.ZipCentralDir
import spray.json.JsonFormat

import java.io.InputStream
import java.nio.charset.StandardCharsets
import java.nio.file.FileSystem
import java.time.Instant
import java.time.LocalDateTime
import java.time.ZoneOffset
import scala.annotation.tailrec
import scala.jdk.CollectionConverters._
import scala.collection.immutable.Seq
import scala.collection.mutable
import scala.util.Using
import scala.util.control.NonFatal

class MismatchedManifestException(val key: AnyRef, val manifests: Seq[jar.Manifest], val values: Seq[AnyRef])
    extends IllegalArgumentException(
      s"Mismatched values for attribute '$key' when merging manifests: ${values.mkString(", ")}"
    )

/**
 * Utility methods for dealing with .jar files
 */
object Jars {

  /**
   * https://github.com/sbt/sbt-assembly/pull/430
   *
   * DOS doesn't support dates before 1980, that's why UNIX timestamp is no go here.
   * Running exactly 1980 is also error prone due to timezones.
   * 2010 is just another date which is also used by other tools such as bazel or sbt.
   */
  val FIXED_TIME: Instant = LocalDateTime.of(2010, 1, 1, 0, 0, 0).toInstant(ZoneOffset.UTC)
  val FIXED_EPOCH_MILLIS: Long = FIXED_TIME.toEpochMilli
  val FIXED_EPOCH_SECONDS: Int = FIXED_TIME.getEpochSecond.toInt
  private val FIXED_FILE_TIME = FileTime.from(FIXED_TIME)

  val incrementalHashingEnabled: Boolean =
    Option(System.getProperty("optimus.buildtool.jars.experimental.incrementalHashing")).exists(_.toBoolean)

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

  // would return the agent paths as written in pathing jar manifest
  private def parseAgentPaths(pathingJarAsset: JarAsset, manifestParsedAgents: String): Seq[JarAsset] = {
    manifestParsedAgents.split(";").toIndexedSeq.map { p =>
      val path = if (p.startsWith(PathUtils.FileProtocol)) {
        val subPath = p.substring(PathUtils.FileProtocol.length)
        utils.ClassPathUtils.demanglePathFromUrlPath(subPath)
      } else Paths.get(p)
      if (path.isAbsolute) JarAsset(path) else pathingJarAsset.parent.resolveFile(RelativePath(path)).asJar
    }
  }

  def extractAgentsInManifest(jarAsset: JarAsset, manifest: jar.Manifest): Seq[JarAsset] = {
    val loadedAgents = JarUtils.load(manifest, JarUtils.nme.AgentsPath)
    loadedAgents.map(parseAgentPaths(jarAsset, _)).getOrElse(Nil)
  }

  def extractManifestClasspath(jarAsset: JarAsset, manifest: jar.Manifest): Seq[JarAsset] =
    JarUtils
      .load(manifest, jar.Attributes.Name.CLASS_PATH)
      .map(parseManifestClasspath(jarAsset, _))
      .getOrElse(Nil)

  def extractClassJars(jarAsset: JarAsset, manifest: jar.Manifest): Seq[JarAsset] =
    JarUtils.load(manifest, nme.ClassJar).map(_.split(";").map(jarAsset.parent.resolveJar).toList).getOrElse(Seq.empty)

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
    catch {
      case NonFatal(e) => throw new IllegalStateException(s"read manifest from jar file failed! ${inputJar.path}", e)
    } finally manifestJar.close()
  }

  def mergeManifestsThenMergeJarContent(
      sourceJars: Seq[JarAsset],
      targetJar: JarAsset,
      manifest: Option[jar.Manifest] = None,
      compress: Boolean,
      incremental: Boolean = false,
      extraFiles: Seq[ExtraInJar] = Nil
  ): Long = {
    val manifests = manifest ++ sourceJars.flatMap(readManifestJar)
    val mergedManifest = mergeManifests(manifests.toList)
    mergeAndHashJarContentGivenManifest(
      sourceJars,
      targetJar,
      compress,
      incremental,
      mergedManifest,
      extraFiles
    )
  }

  /**
   * Merge source jars into a target jar with computing hash of each file and storing it in META-INF/optimus inside jar.
   * There are two implementations of this method, and are selected based on [[ incrementalCompressionEnabled ]] flag.
   *
   * This method does not merge manifests, it already assumes that @param manifest is the correct one.
   * If merging of source manifests is required use [[ mergeManifestsThenMergeJarContent ]] instead
   */
  def mergeAndHashJarContentGivenManifest(
      sourceJars: Seq[JarAsset],
      targetJar: JarAsset,
      compress: Boolean,
      incremental: Boolean = false,
      manifest: Option[jar.Manifest] = None,
      extraFiles: Seq[ExtraInJar] = Nil,
  ): Long = {
    if (incrementalHashingEnabled) {
      fastMergeJarContent(sourceJars, targetJar, manifest, incremental, extraFiles)
    } else {
      val outputJar = () => new ConsistentlyHashedJarOutputStream(targetJar, manifest, compress, incremental)
      mergeJarGivenTokens(sourceJars, outputJar, Map.empty, extraFiles)
    }
    Files.size(targetJar.path)
  }

  def mergeUnhashedContentGivenTokens(
      sourceJars: Seq[JarAsset],
      targetJar: JarAsset,
      tokens: Map[String, String],
      extraFiles: Seq[ExtraInJar] = Nil
  ): Unit = {
    val outputStream = () => new UnhashedJarOutputStream(targetJar, None, compressed = true)
    mergeJarGivenTokens(sourceJars, outputStream, tokens, extraFiles)
  }

  def getAllPathsInJar(jar: JarAsset): Seq[Path] = {
    try {
      Using.resource(JarUtils.jarFileSystem(jar)) { jarFs => Directory.findFiles(Directory.root(jarFs)).map(_.path) }
    } catch {
      case NonFatal(e) => throw new IllegalStateException(s"read content from jar file failed! ${jar.path}", e)
    }
  }

  private def mergeJarGivenTokens(
      sourceJars: Seq[JarAsset],
      outputJar: () => EnhancedJarOutputStream,
      tokens: Map[String, String],
      extraFiles: Seq[ExtraInJar] = Nil
  ): Unit = {
    val allFiles = mutable.Buffer[(String, JarAsset)]()
    val metadataContents = mutable.Map[String, Seq[String]]()
    val servicesContents = mutable.Map[String, Seq[String]]()
    val output = outputJar()
    try {
      sourceJars.foreach { j =>
        try {
          Using.resource(JarUtils.jarFileSystem(j)) { inputJar =>
            Using.resource(Files.walk(inputJar.getPath("/"))) { stream =>
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
                    name != JarFile.MANIFEST_NAME && name != Hashing.optimusHashPath && name != Hashing.optimusPerEntryHashPath &&
                    // incremental marker written separately
                    name != JarUtils.optimusIncrementalPath &&
                    // if we've specified any extra files to write, then they'll overwrite the versions in the source jars
                    !extraFiles.exists(extraFile => extraFile.inJarPath.pathString == name)
                  ) {
                    allFiles.append((name, j))
                    val target = RelativePath(name)
                    if (tokens.isEmpty) output.copyInFile(p, target)
                    else output.writeFile(Files.readString(p), target, tokens)
                  }
                }
              }
            }
          }
        } catch {
          case NonFatal(e) => throw new IllegalStateException(s"read content from jar file failed! ${j.path}", e)
        }
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
      extraFiles.foreach { file => file.write(output) }
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

  private def fastMergeJarContent(
      sourceJars: Seq[JarAsset],
      targetJar: JarAsset,
      manifest: Option[jar.Manifest],
      incremental: Boolean,
      extraFiles: Seq[ExtraInJar] = Nil
  ): Unit = {
    val additionalMetadataEntries =
      Set(
        Hashing.optimusPerEntryHashPath,
        Hashing.optimusHashPath,
        JarFile.MANIFEST_NAME,
        JarUtils.optimusIncrementalPath
      )

    def tempJarOutputStream(file: JarAsset, manifest: Option[jar.Manifest])(
        fn: EnhancedJarOutputStream => Unit
    ): Unit = {
      val output = new UnhashedJarOutputStream(file, manifest, true)
      try { fn(output) }
      finally { output.close() }
    }

    def getUnchangedEntries(
        jarFilesystem: FileSystem,
        nonMetaEntries: Seq[ZipCentralDir.Entry],
        changedEntries: Seq[RelativePath]
    ): Map[RelativePath, String] = {
      val previousCompilationEntryHashes = jarFilesystem.getPath(Hashing.optimusPerEntryHashPath)

      if (Files.exists(previousCompilationEntryHashes)) {
        val previousEntries = Files
          .readAllLines(previousCompilationEntryHashes)
          .asScala
          .map(entry => entry.split('\t').toList)
          .collect { case fileName :: hash :: Nil =>
            RelativePath(fileName) -> hash
          }
          .toMap

        val removedEntries = previousEntries.keySet diff nonMetaEntries.map(entry => RelativePath(entry.getName)).toSet
        previousEntries -- (removedEntries ++ changedEntries)
      } else Map.empty
    }

    def getManifestEntry: Option[(RelativePath, String)] =
      manifest.map { mf =>
        val bytes = new ByteArrayOutputStream()
        Jars.writeManifestToStreamWithLf(mf, bytes)
        RelativePath(JarFile.MANIFEST_NAME) -> JarHashingUtils.getHashForBytes(bytes.toByteArray)
      }

    def getServiceContents(jarFilesystem: FileSystem): Seq[(RelativePath, String)] =
      if (Files.exists(jarFilesystem.getPath("META-INF/services"))) {
        Using.resource(Files.walk(jarFilesystem.getPath("META-INF/services"))) {
          _.toList.asScala
            .filter(Files.isRegularFile(_))
            .map { p => RelativePath(p.toString.stripPrefix("/")) -> Files.readString(p) }
            .toList
        }
      } else Seq.empty

    def getMetadataContents(jarFileSystem: FileSystem): Seq[(RelativePath, String)] =
      MetaDataFiles.metadataFileNames.flatMap { metadataFile =>
        val path = jarFileSystem.getPath(metadataFile)
        if (Files.exists(path)) {
          Some(RelativePath(metadataFile) -> Files.readString(path))
        } else None
      }

    def rehashEntries(jarFilesystem: FileSystem, changedEntries: Seq[RelativePath]): Seq[(RelativePath, String)] = {
      changedEntries.flatMap { changedEntryPath =>
        val changedEntryNioPath = jarFilesystem.getPath(changedEntryPath.pathString)
        if (Files.isRegularFile(changedEntryNioPath)) {
          val inputStream = new JarHashingUtils.HashingInputStream(Files.newInputStream(changedEntryNioPath))
          try { Some(changedEntryPath -> inputStream.readAllAndGetHash()) }
          finally { inputStream.close() }
        } else None
      }
    }

    final case class RehashedEntries(
        centralDir: ZipCentralDir,
        path: Path,
        filesToHashes: Seq[(RelativePath, String)],
        metadata: Seq[(RelativePath, String)],
        services: Seq[(RelativePath, String)]
    )

    /**
     * This method is responsible for cleaning and rehashing entries inside the original jar.
     * The cleaning consists of separating all of the metadata files from central directory.
     * We return them separately as they will be merged together into a single metadata file for all jars.
     *
     * Removed files do not need to be handled, as they should not exist in [[sbt.internal.inc.IndexBasedZipFsOps.CentralDir]] in the first place.
     * [[sbt.internal.inc.IndexBasedZipFsOps.CentralDir]] is our source of truth and it is developer responsibility to ensure that this is the case.
     */
    def cleanAndRehashJars(): Seq[RehashedEntries] =
      sourceJars.map { jarAsset =>
        /**
         * This is slow operation, as we need to parse central directory entries.
         * It is latter passed directly to [[ TruncatingIndexBasedZipFsOps.mergeArchivesUnsafe ]]
         * to avoid rereading of the central directory
         */
        val centralDir = TruncatingIndexBasedZipFsOps.readCentralDirFromPath(jarAsset.path)

        Using.resource(JarUtils.jarFileSystem(jarAsset)) { inputJar =>
          val allHeaders = centralDir.getHeaders.asScala.toList
          val headersWithoutExtra = allHeaders.filter { entry =>
            !extraFiles.exists { extraFile => extraFile.inJarPath.pathString == entry.getName }
          }

          val serviceContents = getServiceContents(inputJar)
          val metadataContents = getMetadataContents(inputJar)

          val metadataFiles =
            (serviceContents ++ metadataContents).map(_._1.pathString).toSet ++ additionalMetadataEntries
          val nonMetaEntries = headersWithoutExtra.filter(entry => !metadataFiles.contains(entry.getName))
          centralDir.setHeaders(nonMetaEntries.asJava) // Ensure that there are no metadata files left in central dir

          /**
           * [[ FIXED_EPOCH_MILLIS ]] is set by us for all files in [[ TruncatingIndexBasedZipOps ]].
           * This allows us to reliably find changed entries by other tools e.g Zinc.
           */
          val changedEntries = nonMetaEntries
            .filter(entry => entry.getLastModifiedTime != FIXED_EPOCH_MILLIS)
            .map(entry => RelativePath(entry.getName))

          val unchangedEntries = getUnchangedEntries(inputJar, nonMetaEntries, changedEntries)
          val rehashedEntries = rehashEntries(inputJar, changedEntries)

          val filesToHashes = unchangedEntries ++ rehashedEntries

          RehashedEntries(
            centralDir,
            jarAsset.path,
            filesToHashes = filesToHashes.toList,
            metadata = metadataContents,
            services = serviceContents)
        }
      }

    def checkForDuplicates(rehashedEntries: Seq[RehashedEntries], allExtraEntries: Seq[ExtraInJar]): Unit = {
      val nonExtraEntriesToJars =
        rehashedEntries.flatMap(entry => entry.filesToHashes.map(_._1.pathString -> entry.path))
      val extraEntriesToFakeJar = {
        lazy val fakeExtraEntryJar = Paths.get("extra-entry")
        allExtraEntries.map(entry => entry.inJarPath.pathString -> fakeExtraEntryJar)
      }

      val allEntriesToJars = (nonExtraEntriesToJars ++ extraEntriesToFakeJar).toGroupedMap
      if (nonExtraEntriesToJars.length + extraEntriesToFakeJar.length != allEntriesToJars.size) {
        val duplicates = allEntriesToJars.filter(_._2.size > 1)
        val messages = duplicates.map { case (path, jars) =>
          s"$path (${jars.map(Pathed.pathString).mkString(", ")})"
        }
        throw new IllegalArgumentException(s"Duplicate file entries: ${messages.mkString(", ")}")
      }
    }

    val rehashedEntries = cleanAndRehashJars()

    val metadataContents = rehashedEntries
      .flatMap(_.metadata)
      .toGroupedMap
      .map { case (path, contents) => ExtraContentInJar(path, mergeMetadata(contents)) }

    val serviceContents = rehashedEntries
      .flatMap(_.services)
      .toGroupedMap
      .map { case (path, contents) => ExtraContentInJar(path, contents.mkString("\n")) }

    val allExtraEntries = extraFiles ++ metadataContents ++ serviceContents
    checkForDuplicates(rehashedEntries, allExtraEntries)

    tempJarOutputStream(targetJar, manifest) { tempOutput =>
      rehashedEntries.flatMap(_.filesToHashes.map(_._1.pathString)).foreach(tempOutput.writeParentDirectory)
      val extraFilesHashes = allExtraEntries.map { entry => entry.inJarPath -> entry.write(tempOutput) }

      /* sortedFileToHashes can merge files directly to map, because we check for duplicates above */
      val sortedFileToHashes = (rehashedEntries.flatMap(_.filesToHashes) ++ getManifestEntry ++ extraFilesHashes).sorted
      val entryHashContent = sortedFileToHashes.map { case (file, hash) => s"$file\t$hash\n" }.mkString
      val entryHashesHash =
        Hashing.consistentlyHashFileHashes(sortedFileToHashes.map { case (path, hash) => path.toString -> hash })

      tempOutput.writeFile(entryHashContent, RelativePath(Hashing.optimusPerEntryHashPath))
      tempOutput.writeFile(entryHashesHash, RelativePath(Hashing.optimusHashPath))
      Jars.stampJarWithIncrementalFlag(tempOutput, incremental)
    }

    val tempCentralDir = TruncatingIndexBasedZipFsOps.readCentralDir(targetJar.path.toFile)

    /**
     * We want to always write to our temp file, as it does not contain any previous artifacts.
     * The TruncatingIndexBasedZipFsOps will truncate all remaining bytes from previous compilations / merge.
     * To learn more read [[ TruncatingIndexBasedZipFsOps.mergeArchivesUnsafe ]]
     */
    val sources = rehashedEntries.map(entries => entries.centralDir -> entries.path)
    TruncatingIndexBasedZipFsOps.mergeArchivesUnsafe(tempCentralDir, targetJar.path, sources)
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

  def stampJarWithConsistentHash(
      jarAsset: JarAsset,
      compress: Boolean,
      trace: Option[TaskTrace],
      incremental: Boolean = false
  ): Unit = {
    trace.foreach(_.addToStat(ObtStats.StampedJars, 1))
    val manifest = readManifestJar(jarAsset).filterNot(_.getMainAttributes.isEmpty)
    // This has to be replaced with a method, that does not copy all entries, but rather modifies current jar
    AssetUtils.atomicallyWrite(jarAsset, replaceIfExists = true) { temp =>
      val writtenBytes =
        mergeAndHashJarContentGivenManifest(Seq(jarAsset), JarAsset(temp), compress, incremental, manifest)
      trace.foreach(_.addToStat(ObtStats.WrittenClassJars, writtenBytes))
    }
  }

  def stampJarWithIncrementalFlag(jarAsset: JarAsset, incremental: Boolean): Unit = {
    if (incremental) Using.resource(JarUtils.jarFileSystem(jarAsset)) { j =>
      val incrPath = RelativePath(j.getPath(JarUtils.optimusIncrementalPath))
      incrPath.parentOption.foreach(p => Files.createDirectories(p.path))
      // Note, we don't need to worry about deleting the marker file if incremental == false, since for a
      // non-incremental jar there will never be a previous jar containing an incremental file
      if (!Files.exists(incrPath.path)) Files.createFile(incrPath.path)
    }
    Hashing.writeFileAttribute(jarAsset, JarUtils.optimusIncrementalAttribute, incremental.toString)
  }

  def stampJarWithIncrementalFlag(os: EnhancedJarOutputStream, incremental: Boolean): Unit = {
    if (incremental) os.copyInFile(Array.empty[Byte], RelativePath(JarUtils.optimusIncrementalPath))
    Hashing.writeFileAttribute(os.file, JarUtils.optimusIncrementalAttribute, incremental.toString)
  }

  /** Creates a new [[ZipEntry]] with the given name and a modified date of [[FIXED_TIME]], for RTness. */
  def zipEntry(name: String): ZipEntry = {
    val entry = new ZipEntry(name)
    entry.setLastModifiedTime(FIXED_FILE_TIME)
    entry
  }

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
    val tempJarStream = new ConsistentlyHashedJarOutputStream(jar, None, compressed = true)
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

private object JarHashingUtils {
  def getHashForString(str: String): String = {
    getHashForBytes(str.getBytes(StandardCharsets.UTF_8))
  }

  def getHashForBytes(bytes: Array[Byte]): String = {
    val hasher = Hashing.hashFunction.newHasher()
    hasher.putBytes(bytes)
    s"HASH${hasher.hash()}"
  }

  class HashingInputStream(underlying: InputStream) extends InputStream {
    private val hasher = Hashing.hashFunction.newHasher()

    override def read(): Int = {
      val b = underlying.read()
      if (b != -1) {
        hasher.putBytes(Array(b.toByte), 0, 1)
      }
      b
    }

    override def read(b: Array[Byte], off: Int, len: Int): Int = {
      val readBytes = underlying.read(b)
      if (readBytes != -1) {
        hasher.putBytes(b, off, readBytes)
      }
      readBytes
    }

    def readAllAndGetHash(): String = {
      val buffer = new Array[Byte](4096)
      var len = underlying.read(buffer) // this method does not require clearing of the buffer
      while (len != -1) {
        hasher.putBytes(buffer, 0, len)
        len = underlying.read(buffer)
      }
      getHash
    }

    def getHash: String = s"HASH${hasher.hash()}"
  }
}

trait ExtraInJar {
  def inJarPath: RelativePath
  def write(outputStream: EnhancedJarOutputStream): String
}

final case class ExtraContentInJar(inJarPath: RelativePath, content: String) extends ExtraInJar {
  override def write(outputStream: EnhancedJarOutputStream): String = {
    outputStream.writeFile(content, inJarPath)
    JarHashingUtils.getHashForString(content)
  }
}

final case class ExtraFileInJar(inJarPath: RelativePath, path: Path) extends ExtraInJar {
  override def write(outputStream: EnhancedJarOutputStream): String = {
    val inStream = new JarHashingUtils.HashingInputStream(Files.newInputStream(path))
    try {
      outputStream.copyInFile(inStream, inJarPath)
      inStream.getHash
    } finally inStream.close()
  }
}
