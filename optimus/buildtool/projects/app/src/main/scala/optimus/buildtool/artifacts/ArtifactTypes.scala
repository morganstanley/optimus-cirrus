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

import optimus.buildtool.config.ScopeId
import optimus.buildtool.files.Asset
import optimus.buildtool.files.Directory
import optimus.buildtool.files.FileAsset
import optimus.buildtool.files.JarAsset
import optimus.buildtool.files.JsonAsset
import optimus.buildtool.resolvers.ResolutionResult
import optimus.buildtool.trace.CompileOnlyResolve
import optimus.buildtool.trace.CompileResolve
import optimus.buildtool.trace.ResolveTrace
import optimus.buildtool.trace.RuntimeResolve
import optimus.buildtool.utils.AssetUtils
import optimus.buildtool.utils.AsyncUtils.asyncTry
import optimus.buildtool.utils.Hashing
import optimus.buildtool.utils.JarUtils
import optimus.buildtool.utils.Jars
import optimus.buildtool.utils.TarUtils
import optimus.platform._
import spray.json.JsonParser

import java.nio.file.Path
import scala.collection.immutable.Seq
import scala.collection.mutable

trait ArtifactType {
  def name: String
  def suffix: String
  protected def hash(file: FileAsset): String = Hashing.hashFileContent(file)
}

sealed trait CachedArtifactType extends ArtifactType {
  type A <: PathedArtifact
  @node def fromAsset(id: ScopeId, a: Asset): A
  // by default, artifacts are not incremental
  @node def fromAsset(id: ScopeId, a: Asset, incremental: Boolean): A = fromAsset(id, a)
}

sealed trait ResolutionArtifactType extends CachedArtifactType {
  type A = ResolutionArtifact
  override val suffix = "rgz"
  @node override def fromAsset(id: ScopeId, a: Asset): ResolutionArtifact = {
    import JsonImplicits._
    val json = JsonAsset(a.path)
    val cached = AssetUtils.readJson[ResolutionArtifact.Cached](json)
    ResolutionArtifact.create(
      InternalArtifactId(id, this, None),
      ResolutionResult(
        cached.resolvedArtifactsToDepInfos.map { case (art, deps) => (art.asEntity, deps) },
        cached.messages,
        cached.jniPaths,
        cached.moduleLoads,
        cached.transitiveDependencies,
        cached.mappedDependencies
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
  type A = FingerprintArtifact
  override val suffix = "fingerprint"
  @node def fromAsset(id: ScopeId, a: Asset): FingerprintArtifact =
    FingerprintArtifact.create(InternalArtifactId(id, this, None), FileAsset(a.path))
}

trait GeneratedSourceArtifactType extends CachedArtifactType {
  type A = GeneratedSourceArtifact
  @node override def fromAsset(id: ScopeId, a: Asset): GeneratedSourceArtifact = {
    val sourceJar = JarAsset(a.path)
    val fs = JarUtils.jarFileSystem(sourceJar)
    asyncTry {
      val jarRoot = Directory.root(fs)
      val metadataFile = jarRoot.resolveFile(CachedMetadata.MetadataFile).asJson
      import JsonImplicits._
      val md = AssetUtils.readJson[GeneratedSourceMetadata](metadataFile, unzip = false)
      GeneratedSourceArtifact.create(
        id,
        md.generatorName,
        this,
        JarAsset(a.path),
        md.sourcePath,
        md.messages,
        md.hasErrors
      )
    } asyncFinally fs.close()
  }

  override val suffix = "jar"
}

trait InternalClassFileArtifactType extends CachedArtifactType {
  type A = InternalClassFileArtifact

  @node override def fromAsset(id: ScopeId, a: Asset): InternalClassFileArtifact =
    fromAsset(id, a, incremental = false)

  @node override def fromAsset(id: ScopeId, a: Asset, incremental: Boolean): InternalClassFileArtifact = {
    val file = JarAsset(a.path)
    InternalClassFileArtifact.create(InternalArtifactId(id, this, None), file, hash(file), incremental = incremental)
  }
  override val suffix = "jar"
}

trait MessageArtifactType extends CachedArtifactType {
  type A = CompilerMessagesArtifact
  override val suffix = "mgz"

  @node override def fromAsset(id: ScopeId, a: Asset): CompilerMessagesArtifact =
    MessageArtifactType.fromUnwatchedPath(a.path).watchForDeletion()
}

object MessageArtifactType {
  // Artifacts created with `fromUnwatchedPath` will not be monitored for deletion automatically, and so will need to be
  // watched separately. All uses of `fromUnwatchedPath` outside tests should be accompanied by a method detailing
  // how the deletion monitoring will be achieved.
  def fromUnwatchedPath(p: Path): CompilerMessagesArtifact = {
    val messageFile = JsonAsset(p)
    import JsonImplicits._
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
  override val suffix = "jar"

  @node override def fromAsset(id: ScopeId, a: Asset, incremental: Boolean): AnalysisArtifact =
    AnalysisArtifact.create(InternalArtifactId(id, this, None), JarAsset(a.path), incremental)
  @node override def fromAsset(id: ScopeId, a: Asset): AnalysisArtifact = fromAsset(id, a, incremental = false)
}

trait SignatureArtifactType extends CachedArtifactType {
  type A = SignatureArtifact
  override val suffix = "jar"

  @node override def fromAsset(id: ScopeId, a: Asset, incremental: Boolean): SignatureArtifact = {
    val file = JarAsset(a.path)
    SignatureArtifact.create(InternalArtifactId(id, this, None), file, hash(file), incremental)
  }
  @node override def fromAsset(id: ScopeId, a: Asset): SignatureArtifact = fromAsset(id, a, incremental = false)
}

trait PathingArtifactType extends ArtifactType {
  type A = PathingArtifact
  override val suffix = "jar"
  @node def fromAsset(id: ScopeId, a: Asset): PathingArtifact = {
    val file = JarAsset(a.path)
    PathingArtifact.create(id, file, hash(file))
  }
}

trait CppArtifactType extends CachedArtifactType {
  type A = InternalCppArtifact
  override val suffix = "jar"
  @node override def fromAsset(id: ScopeId, a: Asset): InternalCppArtifact = {
    val file = JarAsset(a.path)
    val cached = Jars.withJar(file) { root =>
      val metadata = root.resolveFile(CachedMetadata.MetadataFile).asJson
      import JsonImplicits._
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
  override val suffix = "jar"
  @node override def fromAsset(id: ScopeId, a: Asset): ElectronArtifact = {
    val file = JarAsset(a.path)
    ElectronArtifact.create(id, file, hash(file))
  }
}

trait PythonArtifactType extends CachedArtifactType {
  type A = PythonArtifact
  override val suffix = "tar.gz"
  @node override def fromAsset(id: ScopeId, a: Asset): PythonArtifact = {
    TarUtils
      .readFileInTarGz(a.path, CachedMetadata.MetadataFile)
      .map { metadata =>
        import JsonImplicits._
        val parsed = JsonParser(metadata).convertTo[MessagesMetadata]
        PythonArtifact.create(id, FileAsset(a.path), parsed.messages, parsed.hasErrors)
      }
      .getOrThrow(s"Couldn't read cached metadata in ${a.path}")
  }
}

trait CompiledRunconfArtifactType extends CachedArtifactType {
  type A = CompiledRunconfArtifact
  override val suffix = "jar"
  @node override def fromAsset(id: ScopeId, a: Asset): CompiledRunconfArtifact = {
    val file = JarAsset(a.path)
    CompiledRunconfArtifact.create(id, file, hash(file))
  }
}

trait GenericFilesArtifactType extends CachedArtifactType {
  type A = GenericFilesArtifact
  override val suffix = "jar"
  @node override def fromAsset(id: ScopeId, a: Asset): GenericFilesArtifact = {
    val file = JarAsset(a.path)
    val cached = Jars.withJar(file) { root =>
      val msgsJson = root.resolveFile(GenericFilesArtifact.messages).asJson
      import JsonImplicits._
      AssetUtils.readJson[GenericFilesArtifact.Cached](msgsJson, unzip = false)
    }
    GenericFilesArtifact.create(id, file, cached.messages, cached.hasErrors)
  }
}

trait ProcessorArtifactType extends CachedArtifactType {
  type A = ProcessorArtifact
  override val suffix = "jar"
  @node override def fromAsset(id: ScopeId, a: Asset): ProcessorArtifact = {
    val file = JarAsset(a.path)
    val md = Jars.withJar(file) { root =>
      val metadataFile = root.resolveFile(CachedMetadata.MetadataFile).asJson
      import JsonImplicits._
      AssetUtils.readJson[ProcessorMetadata](metadataFile, unzip = false)
    }
    ProcessorArtifact.create(id, md.processorName, this, file, md.messages, md.hasErrors)
  }
}

object ArtifactType {

  abstract class BaseArtifactType(val name: String) extends ArtifactType

  private val parseMap = new mutable.HashMap[String, ArtifactType]
  private def add(a: BaseArtifactType): Unit = {
    parseMap += (a.name -> a)
  }
  def parse(name: String): ArtifactType = parseMap(name)
  lazy val known: Seq[ArtifactType] = parseMap.values.toIndexedSeq

  case object Sources extends BaseArtifactType("sources") with InternalClassFileArtifactType {
    override val suffix = "src.jar"
  }
  add(Sources)

  case object CppBridge extends BaseArtifactType("cpp-bridge") with GeneratedSourceArtifactType
  add(CppBridge)
  case object ProtoBuf extends BaseArtifactType("protobuf") with GeneratedSourceArtifactType
  add(ProtoBuf)
  case object Jaxb extends BaseArtifactType("jaxb") with GeneratedSourceArtifactType
  add(Jaxb)
  case object Scalaxb extends BaseArtifactType("scalaxb") with GeneratedSourceArtifactType
  add(Scalaxb)
  case object Jxb extends BaseArtifactType("jxb") with GeneratedSourceArtifactType
  add(Jxb)
  case object FlatBuffer extends BaseArtifactType("flatbuffer") with GeneratedSourceArtifactType
  add(FlatBuffer)

  case object Scala extends BaseArtifactType("scala") with InternalClassFileArtifactType
  add(Scala)
  case object Java extends BaseArtifactType("java") with InternalClassFileArtifactType
  add(Java)
  case object Jmh extends BaseArtifactType("jmh") with InternalClassFileArtifactType
  add(Jmh)
  case object Resources extends BaseArtifactType("resources") with InternalClassFileArtifactType
  add(Resources)
  case object ArchiveContent extends BaseArtifactType("archive-content") with InternalClassFileArtifactType
  add(ArchiveContent)

  case object JavaMessages extends BaseArtifactType("java-messages") with MessageArtifactType
  add(JavaMessages)
  case object RegexMessages extends BaseArtifactType("regex-messages") with MessageArtifactType
  add(RegexMessages)
  case object ScalaMessages extends BaseArtifactType("scala-messages") with MessageArtifactType
  add(ScalaMessages)
  case object JmhMessages extends BaseArtifactType("jmh-messages") with MessageArtifactType
  add(JmhMessages)
  case object ConfigMessages extends BaseArtifactType("config-messages") with MessageArtifactType
  add(ConfigMessages)

  case object ScalaAnalysis extends BaseArtifactType("scala-analysis") with AnalysisArtifactType
  add(ScalaAnalysis)
  case object JavaAnalysis extends BaseArtifactType("java-analysis") with AnalysisArtifactType
  add(JavaAnalysis)

  case object JavaAndScalaSignatures extends BaseArtifactType("signatures") with SignatureArtifactType
  add(JavaAndScalaSignatures)
  case object SignatureMessages extends BaseArtifactType("signature-messages") with MessageArtifactType
  add(SignatureMessages)
  case object SignatureAnalysis extends BaseArtifactType("signature-analysis") with AnalysisArtifactType
  add(SignatureAnalysis)

  case object Pathing extends BaseArtifactType("pathing") with PathingArtifactType
  add(Pathing)

  case object Cpp extends BaseArtifactType("cpp") with CppArtifactType
  add(Cpp)

  case object Electron extends BaseArtifactType("electron") with ElectronArtifactType
  add(Electron)

  case object Python extends BaseArtifactType("python") with PythonArtifactType
  add(Python)

  case object CompiledRunconf extends BaseArtifactType("runconf") with CompiledRunconfArtifactType
  add(CompiledRunconf)
  case object CompiledRunconfMessages extends BaseArtifactType("runconf-messages") with MessageArtifactType
  add(CompiledRunconfMessages)
  case object GenericFiles extends BaseArtifactType("generic-files") with GenericFilesArtifactType
  add(GenericFiles)

  case object RegexMessagesFingerprint
      extends BaseArtifactType("regex-messages-fingerprint")
      with FingerprintArtifactType
  add(RegexMessagesFingerprint)
  case object StructureFingerprint extends BaseArtifactType("structure-fingerprint") with FingerprintArtifactType
  add(StructureFingerprint)
  case object GenerationFingerprint extends BaseArtifactType("generation-fingerprint") with FingerprintArtifactType
  add(GenerationFingerprint)
  case object CompilationFingerprint
      extends BaseArtifactType("compilation-fingerprint")
      with FingerprintArtifactType
      with CachedArtifactType
  add(CompilationFingerprint)
  case object ResourceFingerprint extends BaseArtifactType("resource-fingerprint") with FingerprintArtifactType
  add(ResourceFingerprint)
  case object SourceFingerprint extends BaseArtifactType("source-fingerprint") with FingerprintArtifactType
  add(SourceFingerprint)
  case object ArchiveFingerprint extends BaseArtifactType("archive-fingerprint") with FingerprintArtifactType
  add(ArchiveFingerprint)
  case object CompileResolutionFingerprint
      extends BaseArtifactType("compile-resolution-fingerprint")
      with FingerprintArtifactType
  add(CompileResolutionFingerprint)
  case object CompileOnlyResolutionFingerprint
      extends BaseArtifactType("compile-only-resolution-fingerprint")
      with FingerprintArtifactType
  add(CompileOnlyResolutionFingerprint)
  case object RuntimeResolutionFingerprint
      extends BaseArtifactType("runtime-resolution-fingerprint")
      with FingerprintArtifactType
  add(RuntimeResolutionFingerprint)
  case object PathingFingerprint extends BaseArtifactType("pathing-fingerprint") with FingerprintArtifactType
  add(PathingFingerprint)
  case object CppFingerprint extends BaseArtifactType("cpp-fingerprint") with FingerprintArtifactType
  add(CppFingerprint)
  case object PythonFingerprint extends BaseArtifactType("python-fingerprint") with FingerprintArtifactType
  add(PythonFingerprint)
  case object RunconfFingerprint extends BaseArtifactType("runconf-fingerprint") with FingerprintArtifactType
  add(RunconfFingerprint)
  case object GenericFilesFingerprint extends BaseArtifactType("generic-files-fingerprint") with FingerprintArtifactType
  add(GenericFilesFingerprint)
  case object WebFingerprint extends BaseArtifactType("web-fingerprint") with FingerprintArtifactType
  add(WebFingerprint)
  case object ElectronFingerprint extends BaseArtifactType("electron-fingerprint") with FingerprintArtifactType
  add(WebFingerprint)

  case object CompileResolution extends BaseArtifactType("compile-resolution") with ResolutionArtifactType {
    override def fingerprintType: FingerprintArtifactType = CompileResolutionFingerprint
    override def category: ResolveTrace = CompileResolve
  }
  add(CompileResolution)
  case object CompileOnlyResolution extends BaseArtifactType("compile-only-resolution") with ResolutionArtifactType {
    override def fingerprintType: FingerprintArtifactType = CompileOnlyResolutionFingerprint
    override def category: ResolveTrace = CompileOnlyResolve
  }
  add(CompileOnlyResolution)
  case object RuntimeResolution extends BaseArtifactType("runtime-resolution") with ResolutionArtifactType {
    override def fingerprintType: FingerprintArtifactType = RuntimeResolutionFingerprint
    override def category: ResolveTrace = RuntimeResolve
  }
  add(RuntimeResolution)
  case object Locator extends BaseArtifactType("locator") with CachedArtifactType {
    type A = LocatorArtifact
    override val suffix = "json"
    @node override def fromAsset(id: ScopeId, a: Asset): LocatorArtifact = {
      val locatorFile = JsonAsset(a.path)
      import JsonImplicits._
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
  add(Locator)
  case object RootLocator extends BaseArtifactType("root-locator") with CachedArtifactType {
    type A = RootLocatorArtifact
    override val suffix = "idz"
    @node override def fromAsset(id: ScopeId, a: Asset): RootLocatorArtifact = {
      val locatorFile = JsonAsset(a.path)
      import JsonImplicits._
      val cached = AssetUtils.readJson[RootLocatorArtifact.Cached](locatorFile)
      RootLocatorArtifact.create(locatorFile, cached.commitHash, cached.artifactVersion)
    }
  }
  add(RootLocator)

  case object Velocity extends BaseArtifactType("velocity") with ProcessorArtifactType
  add(Velocity)

  case object DeploymentScript extends BaseArtifactType("deployment-script") with ProcessorArtifactType
  add(DeploymentScript)

  case object Freemarker extends BaseArtifactType("freemarker") with ProcessorArtifactType
  add(Freemarker)
  case object ProcessingFingerprint extends BaseArtifactType("processing-fingerprint") with FingerprintArtifactType
  add(ProcessingFingerprint)
  case object OpenApi extends BaseArtifactType("openapi") with ProcessorArtifactType
  add(OpenApi)

  // Backward compatibility (needed for root locators)
  parseMap.put("pickles", JavaAndScalaSignatures)
  parseMap.put("early-messages", SignatureMessages)
  parseMap.put("early-analysis", SignatureAnalysis)
  parseMap.put("analysis", ScalaAnalysis)
}
