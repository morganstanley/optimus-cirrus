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

import optimus.buildtool.cache.RemoteArtifactCacheTracker.addCorruptedFile
import optimus.buildtool.config.NamingConventions._
import optimus.buildtool.config.ScopeId
import optimus.buildtool.files.Asset
import optimus.buildtool.files.Directory
import optimus.buildtool.files.FileAsset
import optimus.buildtool.files.JarAsset
import optimus.buildtool.files.JsonAsset
import optimus.buildtool.resolvers.ResolutionResult
import optimus.buildtool.trace.CompileOnlyResolve
import optimus.buildtool.trace.CompileResolve
import optimus.buildtool.trace.ObtStats
import optimus.buildtool.trace.ObtTrace
import optimus.buildtool.trace.ResolveTrace
import optimus.buildtool.trace.RuntimeResolve
import optimus.buildtool.utils.AssetUtils
import optimus.buildtool.utils.AssetUtils.isJarReadable
import optimus.buildtool.utils.AssetUtils.isTarJsonReadable
import optimus.buildtool.utils.AsyncUtils.asyncTry
import optimus.buildtool.utils.Hashing
import optimus.buildtool.utils.JarUtils
import optimus.buildtool.utils.Jars
import optimus.platform._
import optimus.utils.CaseObjectExtractor

import java.nio.file.Path
import scala.collection.compat._

trait ArtifactType {
  type A <: Artifact
  def name: String
  def suffix: String
  protected def hash(file: FileAsset): String = Hashing.hashFileContent(file)
}

sealed trait CachedArtifactType extends ArtifactType {
  type A <: PathedArtifact

  def isReadable(a: FileAsset): Boolean

  // should check downloaded assets from remote store
  @node def fromRemoteAsset(
      downloadedAsset: Option[FileAsset],
      id: ScopeId,
      keyStr: String,
      stat: ObtStats.Cache): Option[A] =
    downloadedAsset match {
      case Some(a) =>
        if (isReadable(a)) Some(fromAsset(id, a))
        else {
          addCorruptedFile(keyStr, id)
          ObtTrace.addToStat(stat.Corrupted, 1)
          AssetUtils.safeDelete(a)
          None // when remote cache not readable, auto recompile it locally
        }
      case _ => None
    }

  @node def fromAsset(id: ScopeId, a: Asset): A
}

sealed trait ResolutionArtifactType extends CachedArtifactType {
  type A = ResolutionArtifact
  override val suffix = RgzExt

  override def isReadable(a: FileAsset): Boolean = isTarJsonReadable(a)
  @node override def fromAsset(id: ScopeId, a: Asset): A = {
    import JsonImplicits.resolutionArtifactValueCodec
    val json = JsonAsset(a.path)
    val cached = AssetUtils.readJson[ResolutionArtifact.Cached](json)
    ResolutionArtifact.create(
      InternalArtifactId(id, this, None),
      ResolutionResult(
        cached.resolvedArtifactsToDepInfos.map { case (art, deps) => (art.asEntity, deps.toVector) }.toVector,
        cached.messages.toVector,
        cached.jniPaths.toVector,
        cached.moduleLoads.toVector,
        cached.transitiveDependencies.map { case (k, v) => (k, v.toVector) },
        cached.mappedDependencies.map { case (k, v) => (k, v.toVector) }
      ),
      json,
      category,
      cached.hasErrors
    )
  }
  def category: ResolveTrace
  def fingerprintType: FingerprintArtifactType
}

trait FingerprintArtifactType extends ArtifactType {
  type A <: FingerprintArtifact
  override val suffix = "fingerprint.zstd"
}

trait GeneratedSourceArtifactType extends CachedArtifactType {
  type A = GeneratedSourceArtifact
  override val suffix = JarExt

  override def isReadable(a: FileAsset): Boolean = isJarReadable(a)
  @node override def fromAsset(id: ScopeId, a: Asset): GeneratedSourceArtifact = {
    val sourceJar = JarAsset(a.path)
    val fs = JarUtils.jarFileSystem(sourceJar)
    asyncTry {
      val jarRoot = Directory.root(fs)
      val metadataFile = jarRoot.resolveFile(CachedMetadata.MetadataFile).asJson
      import JsonImplicits.generatedSourceMetadataValueCodec
      val md = AssetUtils.readJson[GeneratedSourceMetadata](metadataFile, unzip = false)
      GeneratedSourceArtifact.create(
        id,
        md.generatorType,
        md.generatorName,
        JarAsset(a.path),
        md.sourcePath,
        md.messages,
        md.hasErrors
      )
    } asyncFinally fs.close()
  }
}

trait InternalClassFileArtifactType extends CachedArtifactType {
  type A = InternalClassFileArtifact
  override val suffix = JarExt

  override def isReadable(a: FileAsset): Boolean = isJarReadable(a)
  @node override def fromAsset(id: ScopeId, a: Asset): InternalClassFileArtifact = {
    val file = JarAsset(a.path)
    InternalClassFileArtifact.create(
      InternalArtifactId(id, this, None),
      file,
      hash(file),
      JarUtils.incremental(file)
    )
  }
}

sealed trait MessageArtifactType extends ArtifactType {
  type A <: MessagesArtifact
}

sealed trait InMemoryMessageArtifactType extends MessageArtifactType {
  override type A = InMemoryMessagesArtifact
  override val suffix = ""
}

sealed trait CachedMessageArtifactType extends MessageArtifactType with CachedArtifactType {
  override type A = CompilerMessagesArtifact
  override val suffix = MgzExt

  override def isReadable(a: FileAsset): Boolean = isTarJsonReadable(a)
  @node override def fromAsset(id: ScopeId, a: Asset): CompilerMessagesArtifact =
    CachedMessageArtifactType.fromUnwatchedPath(a.path).watchForDeletion()
}

object CachedMessageArtifactType {
  // Artifacts created with `fromUnwatchedPath` will not be monitored for deletion automatically, and so will need to be
  // watched separately. All uses of `fromUnwatchedPath` outside tests should be accompanied by a method detailing
  // how the deletion monitoring will be achieved.
  def fromUnwatchedPath(p: Path): CompilerMessagesArtifact = {
    val messageFile = JsonAsset(p)
    import JsonImplicits.compilerMessagesArtifactValueCodec
    val cached = AssetUtils.readJson[CompilerMessagesArtifact.Cached](messageFile)
    CompilerMessagesArtifact.unwatched(
      cached.id,
      messageFile,
      cached.messages,
      cached.internalDeps,
      cached.externalDeps,
      cached.category,
      cached.incremental,
      cached.hasErrors
    )
  }
}

trait AnalysisArtifactType extends CachedArtifactType {
  type A = AnalysisArtifact
  override val suffix = JarExt

  override def isReadable(a: FileAsset): Boolean = isJarReadable(a)
  @node override def fromAsset(id: ScopeId, a: Asset): AnalysisArtifact = {
    val file = JarAsset(a.path)
    AnalysisArtifact.create(InternalArtifactId(id, this, None), file, JarUtils.incremental(file))
  }
}

trait SignatureArtifactType extends CachedArtifactType {
  type A = SignatureArtifact
  override val suffix = JarExt

  override def isReadable(a: FileAsset): Boolean = isJarReadable(a)
  @node override def fromAsset(id: ScopeId, a: Asset): SignatureArtifact = {
    val file = JarAsset(a.path)
    SignatureArtifact.create(InternalArtifactId(id, this, None), file, hash(file), JarUtils.incremental(file))
  }
}

trait PathingArtifactType extends ArtifactType {
  type A = PathingArtifact
  override val suffix = JarExt

  @node def fromAsset(id: ScopeId, a: Asset): PathingArtifact = {
    val file = JarAsset(a.path)
    PathingArtifact.create(id, file, hash(file))
  }
}

trait CppArtifactType extends CachedArtifactType {
  type A = InternalCppArtifact
  override val suffix = JarExt

  override def isReadable(a: FileAsset): Boolean = isJarReadable(a)
  @node override def fromAsset(id: ScopeId, a: Asset): InternalCppArtifact = {
    val file = JarAsset(a.path)
    val cached = Jars.withJar(file) { root =>
      val metadata = root.resolveFile(CachedMetadata.MetadataFile).asJson
      import JsonImplicits.cppMetadataValueCodec
      AssetUtils.readJson[CppMetadata](metadata, unzip = false)
    }
    InternalCppArtifact.create(
      scopeId = id,
      file = file,
      precomputedContentsHash = hash(file),
      osVersion = cached.osVersion,
      release = cached.releaseFile,
      debug = cached.debugFile,
      messages = cached.messages,
      hasErrors = cached.hasErrors
    )
  }
}

trait ElectronArtifactType extends CachedArtifactType {
  type A = ElectronArtifact
  override val suffix = JarExt

  override def isReadable(a: FileAsset): Boolean = isJarReadable(a)
  @node override def fromAsset(id: ScopeId, a: Asset): ElectronArtifact = {
    val file = JarAsset(a.path)
    val cached = Jars.withJar(file) { root =>
      val metadata = root.resolveFile(CachedMetadata.MetadataFile).asJson
      import JsonImplicits.electronMetadataValueCodec
      AssetUtils.readJson[ElectronMetadata](metadata, unzip = false)
    }
    ElectronArtifact.create(id, file, hash(file), cached.mode, cached.executables)
  }
}

trait PythonArtifactType extends CachedArtifactType {
  type A = PythonArtifact
  override val suffix = TpaExt

  override def isReadable(a: FileAsset): Boolean = isJarReadable(a)
  @node override def fromAsset(id: ScopeId, a: Asset): PythonArtifact = {
    val parsed = PythonMetadata.load(a.path)

    PythonArtifact.create(
      id,
      FileAsset(a.path),
      parsed.osVersion,
      parsed.messages,
      parsed.hasErrors,
      parsed.inputsHash,
      parsed.python)
  }
}

trait CompiledRunconfArtifactType extends CachedArtifactType {
  type A = CompiledRunconfArtifact
  override val suffix = JarExt

  override def isReadable(a: FileAsset): Boolean = isJarReadable(a)
  @node override def fromAsset(id: ScopeId, a: Asset): CompiledRunconfArtifact = {
    val file = JarAsset(a.path)
    CompiledRunconfArtifact.create(id, file, hash(file))
  }
}

trait GenericFilesArtifactType extends CachedArtifactType {
  type A = GenericFilesArtifact
  override val suffix = JarExt

  override def isReadable(a: FileAsset): Boolean = isJarReadable(a)
  @node override def fromAsset(id: ScopeId, a: Asset): GenericFilesArtifact = {
    val file = JarAsset(a.path)
    val cached = Jars.withJar(file) { root =>
      val msgsJson = root.resolveFile(GenericFilesArtifact.messages).asJson
      import JsonImplicits.genericFilesArtifactValueCodec
      AssetUtils.readJson[GenericFilesArtifact.Cached](msgsJson, unzip = false)
    }
    GenericFilesArtifact.create(id, file, cached.messages)
  }
}

trait ProcessorArtifactType extends CachedArtifactType {
  type A = ProcessorArtifact
  override val suffix = JarExt

  override def isReadable(a: FileAsset): Boolean = isJarReadable(a)
  @node override def fromAsset(id: ScopeId, a: Asset): ProcessorArtifact = {
    val file = JarAsset(a.path)
    val md = Jars.withJar(file) { root =>
      val metadataFile = root.resolveFile(CachedMetadata.MetadataFile).asJson
      import JsonImplicits.processorMetadataValueCodec
      AssetUtils.readJson[ProcessorMetadata](metadataFile, unzip = false)
    }
    ProcessorArtifact.create(id, md.processorName, this, file, md.messages, md.hasErrors)
  }
}

object ArtifactType {
  abstract class BaseArtifactType(val name: String) extends ArtifactType

  // automatically get all obt artifact types in this object by scala runtime reflection
  private[buildtool] val parseMap: Map[String, ArtifactType] =
    CaseObjectExtractor.extractFieldValueToCaseObjects[String, ArtifactType](_.name)
  val known: Seq[ArtifactType] = parseMap.values.toIndexedSeq

  def parse(name: String): ArtifactType = parseMap(name)

  case object Sources extends BaseArtifactType("sources") with InternalClassFileArtifactType {
    override val suffix = "src.jar"
  }
  case object GeneratedSource extends BaseArtifactType("generated-source") with GeneratedSourceArtifactType
  case object Scala extends BaseArtifactType("scala") with InternalClassFileArtifactType
  case object Java extends BaseArtifactType("java") with InternalClassFileArtifactType
  case object Jmh extends BaseArtifactType("jmh") with InternalClassFileArtifactType
  case object Resources extends BaseArtifactType("resources") with InternalClassFileArtifactType
  case object ArchiveContent extends BaseArtifactType("archive-content") with InternalClassFileArtifactType
  case object JavaMessages extends BaseArtifactType("java-messages") with CachedMessageArtifactType
  case object RegexMessages extends BaseArtifactType("regex-messages") with CachedMessageArtifactType
  case object ScalaMessages extends BaseArtifactType("scala-messages") with CachedMessageArtifactType
  case object JmhMessages extends BaseArtifactType("jmh-messages") with CachedMessageArtifactType
  case object ConfigMessages extends BaseArtifactType("config-messages") with InMemoryMessageArtifactType
  case object DuplicateMessages extends BaseArtifactType("duplicate-messages") with CachedMessageArtifactType
  case object ValidationMessages extends BaseArtifactType("validation-messages") with InMemoryMessageArtifactType
  case object ScalaAnalysis extends BaseArtifactType("scala-analysis") with AnalysisArtifactType
  case object JavaAnalysis extends BaseArtifactType("java-analysis") with AnalysisArtifactType
  case object JavaAndScalaSignatures extends BaseArtifactType("signatures") with SignatureArtifactType
  case object SignatureMessages extends BaseArtifactType("signature-messages") with CachedMessageArtifactType
  case object SignatureAnalysis extends BaseArtifactType("signature-analysis") with AnalysisArtifactType
  case object Pathing extends BaseArtifactType("pathing") with PathingArtifactType
  case object Cpp extends BaseArtifactType("cpp") with CppArtifactType
  case object Electron extends BaseArtifactType("electron") with ElectronArtifactType
  case object Python extends BaseArtifactType("python") with PythonArtifactType
  case object CompiledRunconf extends BaseArtifactType("runconf") with CompiledRunconfArtifactType
  case object CompiledRunconfMessages extends BaseArtifactType("runconf-messages") with CachedMessageArtifactType
  case object GenericFiles extends BaseArtifactType("generic-files") with GenericFilesArtifactType
  case object RegexMessagesFingerprint
      extends BaseArtifactType("regex-messages-fingerprint")
      with FingerprintArtifactType
  case object StructureFingerprint extends BaseArtifactType("structure-fingerprint") with FingerprintArtifactType
  case object GenerationFingerprint extends BaseArtifactType("generation-fingerprint") with FingerprintArtifactType
  case object CompilationFingerprint
      extends BaseArtifactType("compilation-fingerprint")
      with FingerprintArtifactType
      with CachedArtifactType {
    private val HashRegex = s".*\\.(HASH[^.]*)\\.fingerprint\\.zstd".r
    override type A = FingerprintArtifact with PathedArtifact

    override def isReadable(a: FileAsset): Boolean = AssetUtils.isTextContentReadable(a)
    @node def fromAsset(id: ScopeId, a: Asset): FingerprintArtifact with PathedArtifact = {
      // slightly hacky
      val hash = a.pathString match {
        case HashRegex(hash) => hash
        case p               => throw new IllegalArgumentException(s"Unexpected fingerprint path: $p")
      }
      FingerprintArtifact.create(InternalArtifactId(id, this, None), FileAsset(a.path), hash)
    }
  }
  case object ResourceFingerprint extends BaseArtifactType("resource-fingerprint") with FingerprintArtifactType
  case object SourceFingerprint extends BaseArtifactType("source-fingerprint") with FingerprintArtifactType
  case object ArchiveFingerprint extends BaseArtifactType("archive-fingerprint") with FingerprintArtifactType
  case object CompileResolutionFingerprint
      extends BaseArtifactType("compile-resolution-fingerprint")
      with FingerprintArtifactType
  case object CompileOnlyResolutionFingerprint
      extends BaseArtifactType("compile-only-resolution-fingerprint")
      with FingerprintArtifactType
  case object RuntimeResolutionFingerprint
      extends BaseArtifactType("runtime-resolution-fingerprint")
      with FingerprintArtifactType
  case object PathingFingerprint extends BaseArtifactType("pathing-fingerprint") with FingerprintArtifactType
  case object CppFingerprint extends BaseArtifactType("cpp-fingerprint") with FingerprintArtifactType
  case object PythonFingerprint extends BaseArtifactType("python-fingerprint") with FingerprintArtifactType
  case object RunconfFingerprint extends BaseArtifactType("runconf-fingerprint") with FingerprintArtifactType
  case object GenericFilesFingerprint extends BaseArtifactType("generic-files-fingerprint") with FingerprintArtifactType
  case object WebFingerprint extends BaseArtifactType("web-fingerprint") with FingerprintArtifactType
  case object ElectronFingerprint extends BaseArtifactType("electron-fingerprint") with FingerprintArtifactType
  case object CompileResolution extends BaseArtifactType("compile-resolution") with ResolutionArtifactType {
    override def fingerprintType: FingerprintArtifactType = CompileResolutionFingerprint
    override def category: ResolveTrace = CompileResolve
  }
  case object CompileOnlyResolution extends BaseArtifactType("compile-only-resolution") with ResolutionArtifactType {
    override def fingerprintType: FingerprintArtifactType = CompileOnlyResolutionFingerprint
    override def category: ResolveTrace = CompileOnlyResolve
  }
  case object RuntimeResolution extends BaseArtifactType("runtime-resolution") with ResolutionArtifactType {
    override def fingerprintType: FingerprintArtifactType = RuntimeResolutionFingerprint
    override def category: ResolveTrace = RuntimeResolve
  }
  case object Locator extends BaseArtifactType("locator") with CachedArtifactType {
    type A = LocatorArtifact
    override val suffix = JsonExt

    override def isReadable(a: FileAsset): Boolean = isTarJsonReadable(a, isZip = false)
    @node override def fromAsset(id: ScopeId, a: Asset): LocatorArtifact = {
      val locatorFile = JsonAsset(a.path)
      import JsonImplicits.locatorArtifactValueCodec
      val cached = AssetUtils.readJson[LocatorArtifact.Cached](locatorFile, unzip = false)
      LocatorArtifact.create(
        id,
        cached.analysisType,
        locatorFile,
        cached.commitHash,
        cached.artifactHash,
        cached.timestamp
      )
    }
  }
  case object Velocity extends BaseArtifactType("velocity") with ProcessorArtifactType
  case object DeploymentScript extends BaseArtifactType("deployment-script") with ProcessorArtifactType
  case object Freemarker extends BaseArtifactType("freemarker") with ProcessorArtifactType
  case object ProcessingFingerprint extends BaseArtifactType("processing-fingerprint") with FingerprintArtifactType
  case object OpenApi extends BaseArtifactType("openapi") with ProcessorArtifactType
}
