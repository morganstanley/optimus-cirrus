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
package optimus.buildtool.artifacts

import optimus.buildtool.config.NpmConfiguration.NpmBuildMode
import optimus.buildtool.config.ScopeId
import optimus.buildtool.config.ScopeId.RootScopeId
import optimus.buildtool.dependencies.PythonDefinition
import optimus.buildtool.files.Asset
import optimus.buildtool.files.Directory
import optimus.buildtool.files.Directory.NoFilter
import optimus.buildtool.files.Directory.PathFilter
import optimus.buildtool.files.FileAsset
import optimus.buildtool.files.FileInJarAsset
import optimus.buildtool.files.GeneratedSourceUnitId
import optimus.buildtool.files.JarAsset
import optimus.buildtool.files.JsonAsset
import optimus.buildtool.files.Pathed
import optimus.buildtool.files.PathedEntity
import optimus.buildtool.files.RelativePath
import optimus.buildtool.files.SourceUnitId
import optimus.buildtool.generators.GeneratorType
import optimus.buildtool.resolvers.DependencyInfo
import optimus.buildtool.resolvers.ResolutionResult
import optimus.buildtool.trace
import optimus.buildtool.trace.CategoryTrace
import optimus.buildtool.trace.MessageTrace
import optimus.buildtool.trace.ObtStats
import optimus.buildtool.trace.ObtTrace
import optimus.buildtool.trace.Validation
import optimus.buildtool.utils.TrackedExternalState
import optimus.buildtool.utils.HashedContent
import optimus.buildtool.utils.Hashing
import optimus.buildtool.utils.Jars
import optimus.buildtool.utils.PathUtils
import optimus.buildtool.utils.OptimusBuildToolAssertions
import optimus.platform._

import java.nio.file.Path
import java.time.Instant
import scala.collection.compat._
import scala.collection.immutable.{IndexedSeq, Seq}
import scala.collection.immutable.SortedMap
import scala.jdk.CollectionConverters._

/** Represents a concrete artifact set produced by a compilation or from an external library */
@entity private[buildtool] sealed trait Artifact {
  def id: ArtifactId
  def hasErrors: Boolean = false
}

object Artifacts {
  def validate(allArtifacts: Seq[Artifact]): Seq[Artifact] =
    allArtifacts ++ checkDuplicates(allArtifacts)

  def checkDuplicates(allArtifacts: Seq[Artifact]): Seq[Artifact] = {
    val duplicateArtifacts = allArtifacts.groupBy(_.id).filter(_._2.size > 1)
    val scopedDuplicates = duplicateArtifacts.groupBy {
      case (InternalArtifactId(scopeId, _, _), _) =>
        scopeId
      case _ =>
        RootScopeId
    }
    scopedDuplicates
      .map { case (scopeId, dupes) =>
        val messages = dupes
          .map { case (id, as) =>
            val suffix = as match {
              case Seq(a: MessagesArtifact, b: MessagesArtifact) =>
                val aMessages = a.messages.toSet
                val bMessages = b.messages.toSet
                val onlyInA = aMessages.diff(bMessages)
                val onlyInB = bMessages.diff(aMessages)
                s"\t$a\n\tUnique messages:\n\t\t${onlyInA.mkString("\n\t\t")}\n\t$b\n\tUnique messages:\n\t\t${onlyInB.mkString("\n\t\t")}"
              case _ =>
                s"\t${as.mkString("\n\t")}"
            }
            CompilationMessage.error(s"Multiple (${as.size}) artifacts for key $id\n$suffix")
          }
          .to(Seq)
        InMemoryMessagesArtifact(
          InternalArtifactId(scopeId, ArtifactType.DuplicateMessages, None),
          messages,
          Validation
        )
      }
      .to(Seq)
  }
}

final case class Artifacts(scope: Seq[Artifact], upstream: Seq[Artifact]) {
  lazy val all: IndexedSeq[Artifact] = Vector(scope, upstream).flatten
}

private[buildtool] object PathedArtifact {
  // Represents the state of the artifact we have written.
  // By tweaking this we can invalidate the cache and cause a recreation of the artifact.
  @entity private class ArtifactVersion(path: String) extends TrackedExternalState
  @entersGraph def registerDeletion(p: Path): Tweak = {
    ObtTrace.addToStat(ObtStats.DeletedArtifacts, 1)
    ArtifactVersion(Pathed.pathString(p)).tweakVersion()
  }
  @node def version(asset: Asset): Int = ArtifactVersion(asset.pathString).version
}

@entity private[buildtool] sealed trait PathedArtifact extends Artifact with Pathed with PathedEntity {
  import PathedArtifact._

  if (OptimusBuildToolAssertions.enabled) validate()

  protected def validate(): Unit = id match {
    case InternalArtifactId(_, tpe, _) =>
      require(path.iterator.asScala.exists(_.toString == tpe.name), s"Invalid path $pathString for artifact type $tpe")
    case _ => ()
  }

  // don't store artifacts with errors (to prevent unexpected transient errors being cached on disk)
  protected val storeWithErrors: Boolean = false
  final def shouldBeStored: Boolean = storeWithErrors || !hasErrors

  @node def watchForDeletion(): this.type = {
    ArtifactVersion(pathString).establishDependency()
    this
  }

  override def toString: String = s"${getClass.getSimpleName}($id, $path)"
}

@entity private[buildtool] sealed trait StoreJson { self: PathedArtifact =>

  final def storeJson(): Unit =
    if (shouldBeStored) writeAsJson()
    else log.debug(s"Skipping store of json for $pathString (contains errors? $hasErrors)")

  protected def writeAsJson(): Unit
}

@entity private[buildtool] sealed trait IncrementalArtifact extends Artifact {
  def incremental: Boolean
  @node def withIncremental(incremental: Boolean): IncrementalArtifact
}

object Artifact {
  object InternalArtifact {
    def unapply(a: Artifact): Option[(InternalArtifactId, Artifact)] = a.id match {
      case id: InternalArtifactId => Some((id, a))
      case _                      => None
    }
  }
  final case class InternalPathedArtifact(id: InternalArtifactId, artifact: PathedArtifact)
  object InternalPathedArtifact {
    def unapply(a: Artifact): Option[(InternalArtifactId, PathedArtifact)] = (a.id, a) match {
      case (id: InternalArtifactId, pa: PathedArtifact) => Some((id, pa))
      case _                                            => None
    }
  }

  def onlyErrors[A <: Artifact](inputs: IndexedSeq[A]): Option[IndexedSeq[A]] = {
    val es = inputs.filter(_.hasErrors)
    if (es.isEmpty) None else Some(es)
  }

  def hasErrors(inputs: Seq[Artifact]): Boolean = inputs.exists(_.hasErrors)

  def messages(artifacts: Seq[Artifact]): Seq[MessagesArtifact] =
    artifacts.collect { case messages: MessagesArtifact => messages }

  def scopeIds(artifacts: Seq[Artifact]): Seq[ScopeId] =
    artifacts.collect {
      case InternalArtifact(InternalArtifactId(scope, _, _), _) if !scope.isRoot => scope
    }.distinct

  def transitiveIds(directIds: Set[ScopeId], artifacts: Seq[Artifact]): Set[ScopeId] =
    artifacts.collect {
      case InternalArtifact(InternalArtifactId(scopeId, _, _), _) if !scopeId.isRoot && !directIds.contains(scopeId) =>
        scopeId
    }.toSet

}

@entity private[buildtool] sealed trait HashedArtifact extends PathedArtifact {
  // the hash of the actual content of this artifact (not the hash of the inputs used to generate it)
  @node protected def contentsHash: String

  // Note that the fingerprint doesn't include the path since that often contains (a) a workspace-specific path, and
  // (b) a hash of the inputs used to generate this artifact. The fingerprint only cares about the content of this
  // jar (strictly, the id is not necessary but it makes debugging hash differences much easier).
  @node final def fingerprint: String = PathUtils.fingerprintElement(id.fingerprint, contentsHash)
}

@entity private[buildtool] sealed trait ExternalHashedArtifact extends HashedArtifact {
  type ThisCachedArtifact <: CachedArtifact[ExternalHashedArtifact]
  val isMaven: Boolean
  override val id: ExternalArtifactId
  def file: FileAsset
  private[buildtool] def cached: ThisCachedArtifact
}

sealed trait CachedArtifact[+T <: ExternalHashedArtifact] {
  @entersGraph def asEntity: T
}

@entity private[buildtool] final class ExternalBinaryArtifact(
    val id: ExternalArtifactId,
    val isMaven: Boolean,
    val file: FileAsset,
) extends ExternalHashedArtifact {
  type ThisCachedArtifact = ExternalBinaryArtifact.CachedExternalBinaryArtifact

  def cached: ExternalBinaryArtifact.CachedExternalBinaryArtifact =
    ExternalBinaryArtifact.CachedExternalBinaryArtifact(id, isMaven, file)
  @node override protected def contentsHash: String = Hashing.hashFileContent(file)
  override def path: Path = file.path

  @node def copy(newFile: FileAsset): ExternalBinaryArtifact =
    ExternalBinaryArtifact(id, isMaven, newFile)
}

object ExternalBinaryArtifact {
  final case class CachedExternalBinaryArtifact(
      id: ExternalArtifactId,
      isMaven: Boolean,
      file: FileAsset
  ) extends CachedArtifact[ExternalBinaryArtifact] {
    @entersGraph override def asEntity: ExternalBinaryArtifact = ExternalBinaryArtifact(id, isMaven, file)
  }
}

@entity private[buildtool] sealed trait MessagesArtifact extends Artifact {
  def id: InternalArtifactId
  def messages: Seq[CompilationMessage]
  def taskCategory: CategoryTrace
  override final def hasErrors: Boolean = cachedHasErrors
  protected val cachedHasErrors: Boolean // cached for performance
  final def errors: Int = messages.count(_.isError)
  final def warnings: Int = messages.count(_.isWarning)
}
object MessagesArtifact {
  def hasErrors(messages: Seq[CompilationMessage]): Boolean = messages.exists(_.isError)

  def messageSummary(messages: Seq[CompilationMessage]): String =
    if (messages.nonEmpty) {
      messages
        .groupBy(_.severity)
        .map { case (severity, msgs) =>
          s"$severity: ${msgs.size}"
        }
        .mkString(", ")
    } else "No messages"
}

@entity private[buildtool] sealed trait PreHashedArtifact extends HashedArtifact {
  def precomputedContentsHash: String
  @node final protected def contentsHash: String = precomputedContentsHash
}

@entity trait FingerprintArtifact extends Artifact {
  def id: InternalArtifactId
  val hash: String
}

@entity final class FingerprintArtifactImpl private[artifacts] (
    val id: InternalArtifactId,
    val fingerprintFile: FileAsset,
    val hash: String
) extends FingerprintArtifact
    with PathedArtifact {
  override def path: Path = fingerprintFile.path

  @node def copy(
      fingerprintFile: FileAsset = fingerprintFile,
      hash: String = hash,
  ): FingerprintArtifact with PathedArtifact =
    FingerprintArtifact.create(id, fingerprintFile, hash)
}

// we don't want to store empty fingerprints on disk
@entity private[artifacts] final class EmptyFingerprintArtifact private[artifacts] (
    val id: InternalArtifactId,
    val hash: String
) extends FingerprintArtifact {
  val fingerprint: Seq[String] = Nil
  override def toString: String = s"${getClass.getSimpleName}($id, $hash)"
}

object FingerprintArtifact {
  @node def create(
      id: InternalArtifactId,
      fingerprintFile: FileAsset,
      hash: String
  ): FingerprintArtifact with PathedArtifact =
    FingerprintArtifactImpl(id, fingerprintFile, hash).watchForDeletion()
  // Artifacts created with `unwatched` will not be monitored for deletion automatically, and so will need to be
  // watched separately. All uses of `unwatched` outside tests should be accompanied by a method detailing how the
  // deletion monitoring will be achieved.
  def unwatched(
      id: InternalArtifactId,
      fingerprintFile: FileAsset,
      hash: String
  ): FingerprintArtifact with PathedArtifact =
    FingerprintArtifactImpl(id, fingerprintFile, hash)

  def empty(
      id: InternalArtifactId,
      hash: String
  ): FingerprintArtifact =
    EmptyFingerprintArtifact(id, hash)
}

@entity private[buildtool] final class GeneratedSourceArtifact private (
    scopeId: ScopeId,
    val generatorType: GeneratorType,
    val generatorName: String,
    val sourceJar: JarAsset,
    val sourcePath: RelativePath,
    override val messages: Seq[CompilationMessage],
    override protected val cachedHasErrors: Boolean
) extends MessagesArtifact
    with PathedArtifact {
  override val id: InternalArtifactId = InternalArtifactId(
    scopeId,
    ArtifactType.GeneratedSource,
    GeneratedSourceArtifact.discriminator(generatorType, generatorName)
  )
  override val taskCategory: MessageTrace = trace.GenerateSource

  override def path: Path = sourceJar.path

  def generator: String = GeneratedSourceArtifact.generator(generatorType, generatorName)

  @node def hashedContent(filter: PathFilter = NoFilter): SortedMap[SourceUnitId, HashedContent] = {
    Jars.withJar(sourceJar) { root =>
      val sourceDir = root.resolveDir(sourcePath)
      // GeneratedSourceArtifact contents are RT, so we're safe to call findFilesUnsafe here
      SortedMap[SourceUnitId, HashedContent](Directory.findFilesUnsafe(sourceDir, filter).map { f =>
        val jarRootToSourceFilePath = root.relativize(f)
        val sourceFolderToFilePath = sourceDir.relativize(f)
        val id =
          GeneratedSourceUnitId(scopeId, generatorName, sourceJar, jarRootToSourceFilePath, sourceFolderToFilePath)
        val content = Hashing.hashFileWithContent(f)
        id -> content
      }: _*)
    }
  }

  override def toString: String = s"${getClass.getSimpleName}($id, $sourceJar)"
}

object GeneratedSourceArtifact {
  @node def create(
      scopeId: ScopeId,
      generatorType: GeneratorType,
      generatorName: String,
      sourceJar: JarAsset,
      sourcePath: RelativePath,
      messages: Seq[CompilationMessage]
  ): GeneratedSourceArtifact =
    create(scopeId, generatorType, generatorName, sourceJar, sourcePath, messages, MessagesArtifact.hasErrors(messages))

  @node private[artifacts] def create(
      scopeId: ScopeId,
      generatorType: GeneratorType,
      generatorName: String,
      sourceJar: JarAsset,
      sourcePath: RelativePath,
      messages: Seq[CompilationMessage],
      hasErrors: Boolean
  ): GeneratedSourceArtifact =
    GeneratedSourceArtifact(scopeId, generatorType, generatorName, sourceJar, sourcePath, messages, hasErrors)
      .watchForDeletion()

  // Artifacts created with `unwatched` will not be monitored for deletion automatically, and so will need to be
  // watched separately. All uses of `unwatched` outside tests should be accompanied by a method detailing how the
  // deletion monitoring will be achieved.
  def unwatched(
      scopeId: ScopeId,
      generatorType: GeneratorType,
      generatorName: String,
      sourceJar: JarAsset,
      sourcePath: RelativePath,
      messages: Seq[CompilationMessage]
  ): GeneratedSourceArtifact =
    GeneratedSourceArtifact(
      scopeId,
      generatorType,
      generatorName,
      sourceJar,
      sourcePath,
      messages,
      MessagesArtifact.hasErrors(messages)
    )

  def generator(generatorType: GeneratorType, generatorName: String): String =
    if (generatorType.name == generatorName) generatorName else s"${generatorType.name}-$generatorName"

  def discriminator(generatorType: GeneratorType, generatorName: String): Option[String] =
    Some(generator(generatorType, generatorName))
}

@entity private[buildtool] sealed trait ClassFileArtifact extends HashedArtifact {
  val file: JarAsset
  val containsPlugin: Boolean
  val containsAgent: Boolean
  val containsOrUsedByMacros: Boolean

  override def path: Path = file.path
  override def uriString: String = file.uriString

  @node def copy(
      file: JarAsset = file,
      containsPlugin: Boolean = containsPlugin,
      containsAgent: Boolean = containsAgent,
      containsOrUsedByMacros: Boolean = containsOrUsedByMacros
  ): ClassFileArtifact
}

/** Represents internal jar files which contain scala or java classfiles. */
@entity private[buildtool] final class InternalClassFileArtifact private (
    val id: InternalArtifactId,
    val file: JarAsset,
    val precomputedContentsHash: String,
    val incremental: Boolean,
    val containsPlugin: Boolean,
    val containsAgent: Boolean,
    val containsOrUsedByMacros: Boolean
) extends ClassFileArtifact
    with IncrementalArtifact
    with PreHashedArtifact {

  @node def copy(
      file: JarAsset = file,
      containsPlugin: Boolean = containsPlugin,
      containsAgent: Boolean = containsAgent,
      containsOrUsedByMacros: Boolean = containsOrUsedByMacros
  ): InternalClassFileArtifact = copyArtifact(
    file = file,
    containsPlugin = containsPlugin,
    containsAgent = containsAgent,
    containsOrUsedByMacros = containsOrUsedByMacros
  )

  @node def copyArtifact(
      file: JarAsset = file,
      incremental: Boolean = incremental,
      containsPlugin: Boolean = containsPlugin,
      containsAgent: Boolean = containsAgent,
      containsOrUsedByMacros: Boolean = containsOrUsedByMacros
  ): InternalClassFileArtifact =
    InternalClassFileArtifact.create(
      id,
      file,
      precomputedContentsHash,
      incremental = incremental,
      containsPlugin = containsPlugin,
      containsAgent = containsAgent,
      containsOrUsedByMacros = containsOrUsedByMacros
    )

  @node def withIncremental(incremental: Boolean): InternalClassFileArtifact =
    InternalClassFileArtifact.create(
      id,
      file,
      precomputedContentsHash,
      incremental = incremental,
      containsPlugin = containsPlugin,
      containsAgent = containsAgent,
      containsOrUsedByMacros = containsOrUsedByMacros
    )

  override def toString: String =
    s"${getClass.getSimpleName}($id, $file, $precomputedContentsHash, $incremental, $containsPlugin, agent?$containsAgent, $containsOrUsedByMacros)"
}

object InternalClassFileArtifact {

  def unapply(a: InternalClassFileArtifact): Option[(InternalArtifactId, JarAsset)] = Some((a.id, a.file))

  @node def create(
      id: InternalArtifactId,
      file: JarAsset,
      precomputedContentsHash: String,
      incremental: Boolean,
      containsPlugin: Boolean = false,
      containsAgent: Boolean = false,
      containsOrUsedByMacros: Boolean = false
  ): InternalClassFileArtifact =
    InternalClassFileArtifact(
      id,
      file,
      precomputedContentsHash,
      incremental,
      containsPlugin,
      containsAgent,
      containsOrUsedByMacros
    ).watchForDeletion()

  // Artifacts created with `unwatched` will not be monitored for deletion automatically, and so will need to be
  // watched separately. All uses of `unwatched` outside tests should be accompanied by a method detailing how the
  // deletion monitoring will be achieved.
  def unwatched(
      id: InternalArtifactId,
      file: JarAsset,
      precomputedContentsHash: String,
      incremental: Boolean,
      containsPlugin: Boolean = false,
      containsAgent: Boolean = false,
      containsOrUsedByMacros: Boolean = false
  ): InternalClassFileArtifact =
    InternalClassFileArtifact(
      id,
      file,
      precomputedContentsHash,
      incremental,
      containsPlugin,
      containsAgent,
      containsOrUsedByMacros
    )

}

/** Represents external jar files which contain scala or java classfiles, with optional sources and javadocs. */
@entity private[buildtool] final class ExternalClassFileArtifact private (
    val id: ExternalArtifactId,
    val file: JarAsset,
    val source: Option[JarAsset],
    val javadoc: Option[JarAsset],
    assumedImmutable: Boolean,
    val containsPlugin: Boolean,
    val containsAgent: Boolean,
    val containsOrUsedByMacros: Boolean,
    val isMaven: Boolean
) extends ClassFileArtifact
    with ExternalHashedArtifact {
  type ThisCachedArtifact = ExternalClassFileArtifact.CachedExternalClassFileArtifact
  private[buildtool] def cached: ExternalClassFileArtifact.CachedExternalClassFileArtifact =
    ExternalClassFileArtifact.CachedExternalClassFileArtifact(
      id,
      file,
      source,
      javadoc,
      assumedImmutable = assumedImmutable,
      containsPlugin = containsPlugin,
      containsAgent = containsAgent,
      containsOrUsedByMacros = containsOrUsedByMacros,
      isMaven = isMaven
    )

  @node override def contentsHash: String = {
    if (!assumedImmutable) ExternalClassFileArtifact.witnessMutableExternalArtifactState(file.path)
    Hashing.hashFileOrDirectoryContent(file, assumedImmutable = assumedImmutable)
  }

  @node def copy(
      file: JarAsset = file,
      containsPlugin: Boolean = containsPlugin,
      containsAgent: Boolean = containsAgent,
      containsOrUsedByMacros: Boolean = containsOrUsedByMacros
  ): ExternalClassFileArtifact =
    ExternalClassFileArtifact.create(
      id,
      file,
      source,
      javadoc,
      assumedImmutable = assumedImmutable,
      containsPlugin = containsPlugin,
      containsAgent = containsAgent,
      containsOrUsedByMacros = containsOrUsedByMacros
    )

  @node def copyWithUpdatedAssets(
      file: JarAsset,
      source: Option[JarAsset],
      javadoc: Option[JarAsset]
  ): ExternalClassFileArtifact =
    ExternalClassFileArtifact.create(
      id,
      file,
      source,
      javadoc,
      assumedImmutable = assumedImmutable,
      containsPlugin = containsPlugin,
      containsAgent = containsAgent,
      containsOrUsedByMacros = containsOrUsedByMacros
    )

  override def toString: String =
    s"${getClass.getSimpleName}($id, $file, $source, $javadoc, $assumedImmutable, $containsPlugin, agent?$containsAgent, $containsOrUsedByMacros)"
}

@entity object ExternalClassFileArtifact {
  final case class CachedExternalClassFileArtifact(
      id: ExternalArtifactId,
      file: JarAsset,
      source: Option[JarAsset],
      javadoc: Option[JarAsset],
      assumedImmutable: Boolean,
      containsPlugin: Boolean,
      containsAgent: Boolean,
      containsOrUsedByMacros: Boolean,
      isMaven: Boolean
  ) extends CachedArtifact[ExternalClassFileArtifact] {
    @entersGraph def asEntity: ExternalClassFileArtifact =
      create(
        id,
        file,
        source: Option[JarAsset],
        javadoc: Option[JarAsset],
        assumedImmutable = assumedImmutable,
        containsPlugin = containsPlugin,
        containsAgent = containsAgent,
        containsOrUsedByMacros = containsOrUsedByMacros,
        isMaven = isMaven
      )
  }

  def unapply(a: ExternalClassFileArtifact): Option[(ExternalArtifactId, JarAsset)] = Some((a.id, a.file))

  // We don't need to watch for deletion of ExternalClassFileArtifacts, since they're not generated by OBT and
  // won't be deleted by rubbish tidying
  @node def create(
      id: ExternalArtifactId,
      file: JarAsset,
      source: Option[JarAsset],
      javadoc: Option[JarAsset],
      assumedImmutable: Boolean,
      containsPlugin: Boolean = false,
      containsAgent: Boolean = false,
      containsOrUsedByMacros: Boolean = false,
      isMaven: Boolean = false
  ): ExternalClassFileArtifact =
    ExternalClassFileArtifact(
      id,
      file,
      source,
      javadoc,
      assumedImmutable = assumedImmutable,
      containsPlugin = containsPlugin,
      containsAgent = containsAgent,
      containsOrUsedByMacros = containsOrUsedByMacros,
      isMaven = isMaven
    )

  def nonInterned(
      id: ExternalArtifactId,
      file: JarAsset,
      source: Option[JarAsset],
      javadoc: Option[JarAsset],
      assumedImmutable: Boolean,
      containsPlugin: Boolean = false,
      containsAgent: Boolean = false,
      containsOrUsedByMacros: Boolean = false,
      isMaven: Boolean = false
  ): ExternalClassFileArtifact =
    ExternalClassFileArtifact(
      id,
      file,
      source,
      javadoc,
      assumedImmutable = assumedImmutable,
      containsPlugin = containsPlugin,
      containsAgent = containsAgent,
      containsOrUsedByMacros = containsOrUsedByMacros,
      isMaven = isMaven
    )

  /**
   * since mutable external artifacts are a very rare case (basically only for devs who work on dependencies of
   * codetree) we currently don't track changes to those libraries individually and instead have a single central
   * version which is incremented for every new build
   */
  @entity object mutableExternalArtifactState extends TrackedExternalState

  @node def witnessMutableExternalArtifactState(externalPath: Path): Unit = {
    ObtTrace.addToStat(ObtStats.MutableExternalDependencies, Set(externalPath))
    mutableExternalArtifactState.establishDependency()
  }
}

@entity sealed trait CppArtifact extends HashedArtifact

@entity final class InternalCppArtifact private (
    val scopeId: ScopeId,
    val file: JarAsset,
    val precomputedContentsHash: String,
    val osVersion: String,
    val release: Option[RelativePath],
    val debug: Option[RelativePath],
    val messages: Seq[CompilationMessage],
    protected val cachedHasErrors: Boolean
) extends CppArtifact
    with MessagesArtifact
    with PreHashedArtifact {

  override def id: InternalArtifactId = InternalArtifactId(scopeId, ArtifactType.Cpp, Some(osVersion))
  override def path: Path = file.path
  override val taskCategory: CategoryTrace = trace.Cpp

  def releaseFile: Option[FileInJarAsset] = release.map(f => FileInJarAsset(file, f))
  def debugFile: Option[FileInJarAsset] = debug.map(f => FileInJarAsset(file, f))

  @node def copy(file: JarAsset = file): InternalCppArtifact =
    InternalCppArtifact(scopeId, file, precomputedContentsHash, osVersion, release, debug, messages, cachedHasErrors)

  private def messageSummary = MessagesArtifact.messageSummary(messages)

  override def toString: String =
    s"${getClass.getSimpleName}($scopeId, $file, $precomputedContentsHash, [$messageSummary])"
}

object InternalCppArtifact {
  @node def create(
      scopeId: ScopeId,
      file: JarAsset,
      precomputedContentsHash: String,
      osVersion: String,
      release: Option[RelativePath],
      debug: Option[RelativePath],
      messages: Seq[CompilationMessage],
      hasErrors: Boolean
  ): InternalCppArtifact =
    InternalCppArtifact(
      scopeId = scopeId,
      file = file,
      precomputedContentsHash = precomputedContentsHash,
      osVersion = osVersion,
      release = release,
      debug = debug,
      messages = messages,
      cachedHasErrors = hasErrors
    ).watchForDeletion()
}

@entity final class PythonArtifact private (
    val scopeId: ScopeId,
    val file: FileAsset,
    val osVersion: String,
    val messages: Seq[CompilationMessage],
    protected val cachedHasErrors: Boolean,
    val inputsHash: String,
    val python: PythonDefinition)
    extends PathedArtifact
    with MessagesArtifact {
  override def id: InternalArtifactId = InternalArtifactId(scopeId, ArtifactType.Python, Some(osVersion))
  override def path: Path = file.path
  override def taskCategory: CategoryTrace = trace.Python

  @node def copy(file: FileAsset = file): PythonArtifact =
    PythonArtifact(scopeId, file, osVersion, messages, cachedHasErrors, inputsHash, python)
}

object PythonArtifact {
  @node def create(
      scopeId: ScopeId,
      file: FileAsset,
      osVersion: String,
      messages: Seq[CompilationMessage],
      hasErrors: Boolean,
      inputsHash: String,
      python: PythonDefinition): PythonArtifact = PythonArtifact(
    scopeId = scopeId,
    file = file,
    osVersion = osVersion,
    messages = messages,
    cachedHasErrors = hasErrors,
    inputsHash = inputsHash,
    python = python
  )
}

@entity final class ElectronArtifact private (
    val scopeId: ScopeId,
    val file: JarAsset,
    val precomputedContentsHash: String,
    val mode: NpmBuildMode,
    val executables: Seq[String])
    extends PreHashedArtifact {
  override def id: InternalArtifactId = InternalArtifactId(scopeId, ArtifactType.Electron, None)
  override def path: Path = file.path
  @node def copy(
      scopeId: ScopeId = scopeId,
      file: JarAsset = file,
      precomputedContentsHash: String = precomputedContentsHash
  ): ElectronArtifact = ElectronArtifact.create(scopeId, file, precomputedContentsHash, mode, executables)
}

object ElectronArtifact {
  @node def create(
      scopeId: ScopeId,
      file: JarAsset,
      precomputedContentsHash: String,
      mode: NpmBuildMode,
      executables: Seq[String]): ElectronArtifact =
    ElectronArtifact(scopeId, file, precomputedContentsHash, mode, executables)
}

/** Represents internal jar files which contain compiled runconf. */
@entity final class CompiledRunconfArtifact private (
    val scopeId: ScopeId,
    val file: JarAsset,
    val precomputedContentsHash: String
) extends PreHashedArtifact {

  override def id: InternalArtifactId = InternalArtifactId(scopeId, ArtifactType.CompiledRunconf, None)
  override def path: Path = file.path
  override def uriString: String = file.uriString

  override def toString: String =
    s"${getClass.getSimpleName}($scopeId, $file, $precomputedContentsHash)"

  @node def copy(
      scopeId: ScopeId = scopeId,
      file: JarAsset = file,
      precomputedContentsHash: String = precomputedContentsHash
  ): CompiledRunconfArtifact = CompiledRunconfArtifact.create(scopeId, file, precomputedContentsHash)
}

object CompiledRunconfArtifact {
  @node def create(
      scopeId: ScopeId,
      file: JarAsset,
      precomputedContentsHash: String
  ): CompiledRunconfArtifact =
    CompiledRunconfArtifact(scopeId, file, precomputedContentsHash).watchForDeletion()
}

/** Represents internal jar files which contain generic config/script files. */
@entity final class GenericFilesArtifact private (
    val scopeId: ScopeId,
    val file: JarAsset,
    val messages: Seq[CompilationMessage],
    override protected val cachedHasErrors: Boolean
) extends PathedArtifact
    with MessagesArtifact {

  override def id: InternalArtifactId = InternalArtifactId(scopeId, ArtifactType.GenericFiles, None)
  override def path: Path = file.path
  override val taskCategory: CategoryTrace = trace.GenericFiles

  override def toString: String =
    s"${getClass.getSimpleName}($scopeId, $file)"

  @node def copy(
      scopeId: ScopeId = scopeId,
      file: JarAsset = file
  ): GenericFilesArtifact = GenericFilesArtifact.create(scopeId, file, messages)
}

object GenericFilesArtifact {
  final case class Cached(messages: Seq[CompilationMessage], hasErrors: Boolean)
  val messages = "messages.json"
  @node def create(
      scopeId: ScopeId,
      file: JarAsset,
      messages: Seq[CompilationMessage]
  ): GenericFilesArtifact = create(scopeId, file, messages, MessagesArtifact.hasErrors(messages))

  @node private[artifacts] def create(
      scopeId: ScopeId,
      file: JarAsset,
      messages: Seq[CompilationMessage],
      hasErrors: Boolean
  ): GenericFilesArtifact =
    GenericFilesArtifact(scopeId, file, messages, hasErrors).watchForDeletion()
}

/** represents a jar of scala signatures (i.e. not classes) */
@entity private[buildtool] final class SignatureArtifact private (
    val id: ArtifactId,
    val signatureFile: JarAsset,
    val precomputedContentsHash: String,
    val incremental: Boolean
) extends PreHashedArtifact
    with IncrementalArtifact {
  override def path: Path = signatureFile.path

  override def toString: String =
    s"${getClass.getSimpleName}($id, $signatureFile, $precomputedContentsHash, $incremental)"

  @node def copy(
      id: ArtifactId = id,
      signatureFile: JarAsset = signatureFile,
      precomputedContentsHash: String = precomputedContentsHash,
      incremental: Boolean = incremental
  ): SignatureArtifact = SignatureArtifact.create(id, signatureFile, precomputedContentsHash, incremental)

  @node def withIncremental(incremental: Boolean): SignatureArtifact =
    SignatureArtifact.create(id, signatureFile, precomputedContentsHash, incremental)
}

object SignatureArtifact {
  @node def create(
      id: ArtifactId,
      signatureFile: JarAsset,
      precomputedContentsHash: String,
      incremental: Boolean
  ): SignatureArtifact =
    SignatureArtifact(id, signatureFile, precomputedContentsHash, incremental).watchForDeletion()

  // Artifacts created with `unwatched` will not be monitored for deletion automatically, and so will need to be
  // watched separately. All uses of `unwatched` outside tests should be accompanied by a method detailing how the
  // deletion monitoring will be achieved.
  def unwatched(
      id: ArtifactId,
      signatureFile: JarAsset,
      precomputedContentsHash: String,
      incremental: Boolean
  ): SignatureArtifact =
    SignatureArtifact(id, signatureFile, precomputedContentsHash, incremental)
}

@entity private[buildtool] final class AnalysisArtifact private (
    val id: ArtifactId,
    val analysisFile: JarAsset,
    val incremental: Boolean
) extends PathedArtifact
    with IncrementalArtifact {
  override def path: Path = analysisFile.path

  @node def withIncremental(incremental: Boolean): AnalysisArtifact =
    AnalysisArtifact.create(id, analysisFile, incremental = incremental)
}

object AnalysisArtifact {
  @node def create(
      id: ArtifactId,
      analysisFile: JarAsset,
      incremental: Boolean
  ): AnalysisArtifact = AnalysisArtifact(id, analysisFile, incremental).watchForDeletion()

  // Artifacts created with `unwatched` will not be monitored for deletion automatically, and so will need to be
  // watched separately. All uses of `unwatched` outside tests should be accompanied by a method detailing how the
  // deletion monitoring will be achieved.
  def unwatched(
      id: ArtifactId,
      analysisFile: JarAsset,
      incremental: Boolean
  ): AnalysisArtifact = AnalysisArtifact(id, analysisFile, incremental)
}

/** represents a pathing jar (i.e. a jar with a Class-Path in the META-INF/MANIFEST.MF file and no other content) */
@entity private[buildtool] final class PathingArtifact private (
    val scopeId: ScopeId,
    val pathingFile: JarAsset,
    val precomputedContentsHash: String
) extends PreHashedArtifact {
  override def path: Path = pathingFile.path
  override def id: InternalArtifactId = InternalArtifactId(scopeId, ArtifactType.Pathing, None)

  override def toString: String = s"${getClass.getSimpleName}($scopeId, $pathingFile, $precomputedContentsHash)"
}

object PathingArtifact {
  @node def create(scopeId: ScopeId, pathingFile: JarAsset, precomputedContentsHash: String): PathingArtifact =
    PathingArtifact(scopeId, pathingFile, precomputedContentsHash).watchForDeletion()
}

@entity final class ResolutionArtifact private (
    val id: InternalArtifactId,
    val result: ResolutionResult,
    val resolutionFile: JsonAsset,
    override val taskCategory: trace.ResolveTrace,
    override protected val cachedHasErrors: Boolean
) extends MessagesArtifact
    with PathedArtifact
    with StoreJson {

  override def messages: Seq[CompilationMessage] = result.messages
  override def path: Path = resolutionFile.path
  override def toString: String = s"${getClass.getSimpleName}($id, $resolutionFile)"

  import JsonImplicits.resolutionArtifactValueCodec
  override protected def writeAsJson(): Unit =
    resolutionFile.storeJson(
      ResolutionArtifact.Cached(
        id,
        result.resolvedArtifactsToDepInfos.map { case (art, deps) => (art.cached, deps) },
        result.messages,
        result.jniPaths,
        result.moduleLoads,
        taskCategory,
        hasErrors,
        result.finalDependencies,
        result.mappedDependencies
      ),
      replace = false
    )
}

object ResolutionArtifact {
  final case class Cached(
      id: InternalArtifactId,
      resolvedArtifactsToDepInfos: Seq[(CachedArtifact[ExternalHashedArtifact], Seq[DependencyInfo])],
      messages: Seq[CompilationMessage],
      // IMPORTANT - why are we not using Asset for jniPaths and sticking with String?
      // one client ivy file uses an objectively unusual workaround where the linux-specific AFS .exec path is
      // specified starting with a single slash, whereas everything else is specified starting with two slashes.
      // This has the effect of Windows thinking the file doesn't exist, rather than handing it over to the AFS driver,
      // which causes Java to throw.
      // Asset's pathString normalizes the string representing the path to always start with double slashes,
      // which would break apps under Windows. For more information, look at OPTIMUS-46629.
      jniPaths: Seq[String],
      moduleLoads: Seq[String],
      taskCategory: trace.ResolveTrace,
      hasErrors: Boolean,
      transitiveDependencies: Map[DependencyInfo, Seq[DependencyInfo]],
      mappedDependencies: Map[DependencyInfo, Seq[DependencyInfo]]
  )

  @node def create(
      id: InternalArtifactId,
      result: ResolutionResult,
      resolutionFile: JsonAsset,
      taskCategory: trace.ResolveTrace
  ): ResolutionArtifact =
    ResolutionArtifact(id, result, resolutionFile, taskCategory, MessagesArtifact.hasErrors(result.messages))
      .watchForDeletion()

  @node private[artifacts] def create(
      id: InternalArtifactId,
      result: ResolutionResult,
      resolutionFile: JsonAsset,
      taskCategory: trace.ResolveTrace,
      hasErrors: Boolean
  ): ResolutionArtifact =
    ResolutionArtifact(id, result, resolutionFile, taskCategory, hasErrors).watchForDeletion()
}

@entity private[buildtool] final class InMemoryMessagesArtifact(
    val id: InternalArtifactId,
    val messages: Seq[CompilationMessage],
    val taskCategory: MessageTrace,
    override protected val cachedHasErrors: Boolean
) extends MessagesArtifact {
  // exclude the actual messages since they can be huge
  override def toString: String =
    s"InMemoryMessagesArtifact($id, [${MessagesArtifact.messageSummary(messages)}], $taskCategory)"
}

object InMemoryMessagesArtifact {
  def apply(
      id: InternalArtifactId,
      messages: Seq[CompilationMessage],
      taskCategory: MessageTrace
  ): InMemoryMessagesArtifact =
    InMemoryMessagesArtifact(id, messages, taskCategory, MessagesArtifact.hasErrors(messages))
}

@entity private[buildtool] final class CompilerMessagesArtifact private (
    val id: InternalArtifactId,
    val messageFile: JsonAsset,
    val messages: Seq[CompilationMessage],
    val internalDeps: Seq[DependencyLookup.Internal],
    val externalDeps: Seq[DependencyLookup.External],
    val taskCategory: MessageTrace,
    val incremental: Boolean,
    override protected val cachedHasErrors: Boolean
) extends HashedArtifact
    with MessagesArtifact
    with IncrementalArtifact
    with StoreJson {

  override def path: Path = messageFile.path

  @node override def contentsHash: String =
    Hashing.hashStrings(messages.map(_.toString) ++ internalDeps.map(_.toString) ++ externalDeps.map(_.toString))
  import JsonImplicits.compilerMessagesArtifactValueCodec
  override protected def writeAsJson(): Unit =
    messageFile.storeJson(
      CompilerMessagesArtifact.Cached(id, messages, internalDeps, externalDeps, taskCategory, incremental, hasErrors),
      replace = false
    )
  // exclude the actual messages since they can be huge
  override def toString: String =
    s"CompilerMessagesArtifact($id, $messageFile, [${MessagesArtifact.messageSummary(messages)}], $taskCategory, $incremental)"

  @node def withIncremental(incremental: Boolean): CompilerMessagesArtifact =
    CompilerMessagesArtifact.create(id, messageFile, messages, internalDeps, externalDeps, taskCategory, incremental)

  @node def copy(
      id: InternalArtifactId = id,
      messageFile: JsonAsset = messageFile,
      messages: Seq[CompilationMessage] = messages,
      internalDeps: Seq[DependencyLookup.Internal] = internalDeps,
      externalDeps: Seq[DependencyLookup.External] = externalDeps,
      taskCategory: MessageTrace = taskCategory,
      incremental: Boolean = incremental
  ): CompilerMessagesArtifact =
    CompilerMessagesArtifact.create(id, messageFile, messages, internalDeps, externalDeps, taskCategory, incremental)
}
object CompilerMessagesArtifact {
  final case class Cached(
      id: InternalArtifactId,
      messages: Seq[CompilationMessage],
      internalDeps: Seq[DependencyLookup.Internal],
      externalDeps: Seq[DependencyLookup.External],
      category: MessageTrace,
      incremental: Boolean,
      hasErrors: Boolean
  )

  @node def create(
      id: InternalArtifactId,
      messageFile: JsonAsset,
      messages: Seq[CompilationMessage],
      internalDeps: Seq[DependencyLookup.Internal],
      externalDeps: Seq[DependencyLookup.External],
      taskCategory: MessageTrace,
      incremental: Boolean
  ): CompilerMessagesArtifact =
    CompilerMessagesArtifact(
      id,
      messageFile,
      messages,
      internalDeps,
      externalDeps,
      taskCategory,
      incremental,
      MessagesArtifact.hasErrors(messages))
      .watchForDeletion()

  // no dependency tracking:
  @node def create(
      id: InternalArtifactId,
      messageFile: JsonAsset,
      messages: Seq[CompilationMessage],
      taskCategory: MessageTrace,
      incremental: Boolean
  ): CompilerMessagesArtifact =
    CompilerMessagesArtifact(
      id,
      messageFile,
      messages,
      Seq.empty,
      Seq.empty,
      taskCategory,
      incremental,
      MessagesArtifact.hasErrors(messages))
      .watchForDeletion()

  // Artifacts created with `unwatched` will not be monitored for deletion automatically, and so will need to be
  // watched separately. All uses of `unwatched` outside tests should be accompanied by a method detailing how the
  // deletion monitoring will be achieved.
  def unwatched(
      id: InternalArtifactId,
      messageFile: JsonAsset,
      messages: Seq[CompilationMessage],
      internalDeps: Seq[DependencyLookup.Internal],
      externalDeps: Seq[DependencyLookup.External],
      taskCategory: MessageTrace,
      incremental: Boolean
  ): CompilerMessagesArtifact =
    CompilerMessagesArtifact(
      id,
      messageFile,
      messages,
      internalDeps,
      externalDeps,
      taskCategory,
      incremental,
      MessagesArtifact.hasErrors(messages))

  // no dependency tracking:
  def unwatched(
      id: InternalArtifactId,
      messageFile: JsonAsset,
      messages: Seq[CompilationMessage],
      taskCategory: MessageTrace,
      incremental: Boolean
  ): CompilerMessagesArtifact =
    CompilerMessagesArtifact(
      id,
      messageFile,
      messages,
      Seq.empty,
      Seq.empty,
      taskCategory,
      incremental,
      MessagesArtifact.hasErrors(messages))

  private[artifacts] def unwatched(
      id: InternalArtifactId,
      messageFile: JsonAsset,
      messages: Seq[CompilationMessage],
      internalDeps: Seq[DependencyLookup.Internal],
      externalDeps: Seq[DependencyLookup.External],
      taskCategory: MessageTrace,
      incremental: Boolean,
      hasErrors: Boolean
  ): CompilerMessagesArtifact =
    CompilerMessagesArtifact(
      id,
      messageFile,
      messages,
      internalDeps,
      externalDeps,
      taskCategory,
      incremental,
      hasErrors)
}

@entity private[buildtool] final class LocatorArtifact private (
    val scopeId: ScopeId,
    val analysisType: AnalysisArtifactType,
    val locatorFile: JsonAsset,
    val commitHash: Option[String],
    val artifactHash: String,
    val timestamp: Instant
) extends PathedArtifact
    with StoreJson {

  override def path: Path = locatorFile.path

  override def id: InternalArtifactId = InternalArtifactId(scopeId, ArtifactType.Locator, Some(analysisType.name))
  import JsonImplicits.locatorArtifactValueCodec
  override protected def writeAsJson(): Unit =
    locatorFile.storeJson(
      LocatorArtifact.Cached(analysisType, commitHash, artifactHash, timestamp),
      replace = false,
      zip = false
    )

  def withTimestamp(newTimestamp: Instant) =
    LocatorArtifact(scopeId, analysisType, locatorFile, commitHash, artifactHash, newTimestamp)

  def summary: String = s"${commitHash.getOrElse("NOCOMMIT")} ($timestamp)"

  override def toString: String =
    s"${getClass.getSimpleName}($scopeId, $locatorFile, $commitHash, $artifactHash, $timestamp)"
}
object LocatorArtifact {
  final case class Cached(
      analysisType: AnalysisArtifactType,
      commitHash: Option[String],
      artifactHash: String,
      timestamp: Instant)

  @node def create(
      scopeId: ScopeId,
      analysisType: AnalysisArtifactType,
      locatorFile: JsonAsset,
      commitHash: Option[String],
      artifactHash: String,
      timestamp: Instant
  ): LocatorArtifact =
    LocatorArtifact(scopeId, analysisType, locatorFile, commitHash, artifactHash, timestamp).watchForDeletion()

  def unwatched(
      scopeId: ScopeId,
      analysisType: AnalysisArtifactType,
      locatorFile: JsonAsset,
      commitHash: Option[String],
      artifactHash: String,
      timestamp: Instant
  ): LocatorArtifact = LocatorArtifact(scopeId, analysisType, locatorFile, commitHash, artifactHash, timestamp)
}

@entity final class ProcessorArtifact private (
    val scopeId: ScopeId,
    val processorName: String,
    val tpe: ProcessorArtifactType,
    val file: JarAsset,
    val messages: Seq[CompilationMessage],
    override protected val cachedHasErrors: Boolean
) extends PathedArtifact
    with MessagesArtifact {

  override def id: InternalArtifactId = InternalArtifactId(scopeId, tpe, Some(processorName))
  override def path: Path = file.path
  override val taskCategory: CategoryTrace = trace.ProcessScope

  override def toString: String =
    s"${getClass.getSimpleName}($scopeId, $file)"
}

object ProcessorArtifact {
  @node def create(
      scopeId: ScopeId,
      processorName: String,
      tpe: ProcessorArtifactType,
      file: JarAsset,
      messages: Seq[CompilationMessage]
  ): ProcessorArtifact = create(scopeId, processorName, tpe, file, messages, MessagesArtifact.hasErrors(messages))
  @node private[artifacts] def create(
      scopeId: ScopeId,
      processorName: String,
      tpe: ProcessorArtifactType,
      file: JarAsset,
      messages: Seq[CompilationMessage],
      hasErrors: Boolean
  ): ProcessorArtifact =
    ProcessorArtifact(scopeId, processorName, tpe, file, messages, hasErrors).watchForDeletion()

  // Artifacts created with `unwatched` will not be monitored for deletion automatically, and so will need to be
  // watched separately. All uses of `unwatched` outside tests should be accompanied by a method detailing how the
  // deletion monitoring will be achieved.
  def unwatched(
      scopeId: ScopeId,
      processorName: String,
      tpe: ProcessorArtifactType,
      sourceJar: JarAsset,
      messages: Seq[CompilationMessage]
  ): ProcessorArtifact =
    ProcessorArtifact(scopeId, processorName, tpe, sourceJar, messages, MessagesArtifact.hasErrors(messages))
}
