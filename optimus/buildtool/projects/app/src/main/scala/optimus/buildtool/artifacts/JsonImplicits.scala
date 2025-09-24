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

import optimus.buildtool.compilers.venv.PythonConstants.tpa.TpaConfig

import java.net.URL
import java.nio.file.Path
import optimus.buildtool.config.NamingConventions
import optimus.buildtool.config.ScopeId
import optimus.buildtool.files._
import optimus.buildtool.trace.MessageTrace
import optimus.buildtool.utils.PathUtils
import optimus.buildtool.config.ScalaVersionConfig
import optimus.buildtool.config.ScopeConfiguration

import java.net.URI
import com.github.plokhotnyuk.jsoniter_scala.macros.JsonCodecMaker
import com.github.plokhotnyuk.jsoniter_scala.core._
import com.github.plokhotnyuk.jsoniter_scala.macros.CodecMakerConfig
import optimus.buildtool.compilers.venv.RuffCheckOutput
import optimus.buildtool.resolvers.DependencyInfo
import org.apache.commons.lang3.StringUtils

private[buildtool] object JsonImplicits {

  implicit val jsonAssetValueCodec: JsonValueCodec[JsonAsset] = new JsonValueCodec[JsonAsset] {
    override def decodeValue(in: JsonReader, default: JsonAsset): JsonAsset =
      JsonAsset(PathUtils.get(in.readString(null)))
    override def encodeValue(x: JsonAsset, out: JsonWriter): Unit = out.writeVal(x.pathString)
    override def nullValue: JsonAsset = null
  }
  implicit val scopeIdValueCodec: JsonValueCodec[ScopeId] = JsonCodecMaker.make
  implicit val messageTraceValueCodec: JsonValueCodec[MessageTrace] = JsonCodecMaker.make
  implicit val compilationMessageSeverityValueCodec: JsonValueCodec[CompilationMessage.Severity] = {
    new JsonValueCodec[CompilationMessage.Severity] {
      override def decodeValue(in: JsonReader, default: CompilationMessage.Severity): CompilationMessage.Severity =
        CompilationMessage.Severity.parse(in.readString(null))
      override def encodeValue(x: CompilationMessage.Severity, out: JsonWriter): Unit = out.writeVal(x.toString)
      override def nullValue: CompilationMessage.Severity = null
    }
  }

  implicit val compilerMessagesArtifactValueCodec: JsonValueCodec[CompilerMessagesArtifact.Cached] =
    JsonCodecMaker.make(CodecMakerConfig.withSetMaxInsertNumber(65536))
  implicit val locatorArtifactValueCodec: JsonValueCodec[LocatorArtifact.Cached] = JsonCodecMaker.make

  implicit val resolutionArtifactValueCodec: JsonValueCodec[ResolutionArtifact.Cached] =
    JsonCodecMaker.make(CodecMakerConfig.withSetMaxInsertNumber(65536).withMapMaxInsertNumber(65536))
  implicit val genericFilesArtifactValueCodec: JsonValueCodec[GenericFilesArtifact.Cached] =
    JsonCodecMaker.make(CodecMakerConfig.withSetMaxInsertNumber(65536))

  implicit val cppMetadataValueCodec: JsonValueCodec[CppMetadata] =
    JsonCodecMaker.make(CodecMakerConfig.withSetMaxInsertNumber(65536))
  implicit val electronMetadataValueCodec: JsonValueCodec[ElectronMetadata] =
    JsonCodecMaker.make(CodecMakerConfig.withSetMaxInsertNumber(65536))
  implicit val generatedSourceMetadataValueCodec: JsonValueCodec[GeneratedSourceMetadata] =
    JsonCodecMaker.make(CodecMakerConfig.withSetMaxInsertNumber(65536))
  implicit val processorMetadataValueCodec: JsonValueCodec[ProcessorMetadata] =
    JsonCodecMaker.make(CodecMakerConfig.withSetMaxInsertNumber(65536))
  implicit val pythonMetadataValueCodec: JsonValueCodec[PythonMetadata] =
    JsonCodecMaker.make(CodecMakerConfig.withSetMaxInsertNumber(65536))
  implicit val ruffOutputValueCodec: JsonValueCodec[Seq[RuffCheckOutput]] =
    JsonCodecMaker.make(CodecMakerConfig.withSetMaxInsertNumber(65536))
  implicit val messagesMetadataValueCodec: JsonValueCodec[MessagesMetadata] =
    JsonCodecMaker.make(CodecMakerConfig.withSetMaxInsertNumber(65536))

  // We should instead use some kind of RelativePath / AbsolutePath everywhere so they are easy to serialize and will always have correct format
  implicit val pathValueCodec: JsonValueCodec[Path] = new JsonValueCodec[Path] {
    override def decodeValue(in: JsonReader, default: Path): Path = PathUtils.get(in.readString(null))
    override def encodeValue(x: Path, out: JsonWriter): Unit = out.writeVal(PathUtils.platformIndependentString(x))
    override def nullValue: Path = null
  }

  implicit val urlValueCodec: JsonValueCodec[URL] = new JsonValueCodec[URL] {
    override def decodeValue(in: JsonReader, default: URL): URL = new URI(in.readString(null)).toURL
    override def encodeValue(x: URL, out: JsonWriter): Unit = out.writeVal(x.toString)
    override def nullValue: URL = null
  }

  implicit val AssetFormat: JsonValueCodec[Asset] = new JsonValueCodec[Asset] {
    override def decodeValue(in: JsonReader, default: Asset): Asset = Asset(pathValueCodec.decodeValue(in, null))
    override def encodeValue(x: Asset, out: JsonWriter): Unit = pathValueCodec.encodeValue(x.path, out)
    override def nullValue: Asset = null
  }

  implicit val FileAssetFormatter: JsonValueCodec[FileAsset] = new JsonValueCodec[FileAsset] {
    override def decodeValue(in: JsonReader, default: FileAsset): FileAsset = FileAsset(
      pathValueCodec.decodeValue(in, null))
    override def encodeValue(x: FileAsset, out: JsonWriter): Unit = pathValueCodec.encodeValue(x.path, out)
    override def nullValue: FileAsset = null
  }

  implicit val JarAssetFormatter: JsonValueCodec[JarAsset] = new JsonValueCodec[JarAsset] {
    override def decodeValue(in: JsonReader, default: JarAsset): JarAsset = {
      val path = in.readString(null)
      if (NamingConventions.isHttpOrHttps(path)) JarAsset(new URI(path).toURL)
      else JarAsset(PathUtils.get(path))
    }
    override def encodeValue(x: JarAsset, out: JsonWriter): Unit = out.writeVal(x.pathString)
    override def nullValue: JarAsset = null
  }

  private def createArtifactTypeCodec[T <: ArtifactType]: JsonValueCodec[T] = new JsonValueCodec[T] {
    override def decodeValue(in: JsonReader, default: T): T =
      ArtifactType.parse(in.readString(null)).asInstanceOf[T]
    override def encodeValue(x: T, out: JsonWriter): Unit = out.writeVal(x.name)
    override def nullValue: T = null.asInstanceOf[T]
  }

  implicit val artifactTypeValueCodec: JsonValueCodec[ArtifactType] = createArtifactTypeCodec
  implicit val cachedArtifactTypeValueCodec: JsonValueCodec[CachedArtifactType] = createArtifactTypeCodec
  implicit val analysisArtifactTypeValueCodec: JsonValueCodec[AnalysisArtifactType] = createArtifactTypeCodec

  implicit val directoryValueCodec: JsonValueCodec[Directory] = new JsonValueCodec[Directory] {
    override def decodeValue(in: JsonReader, default: Directory): Directory =
      Directory(pathValueCodec.decodeValue(in, null))
    override def encodeValue(x: Directory, out: JsonWriter): Unit = pathValueCodec.encodeValue(x.path, out)
    override def nullValue: Directory = null
  }

  implicit val relativePathKeyCodec: JsonKeyCodec[RelativePath] = new JsonKeyCodec[RelativePath] {
    override def decodeKey(in: JsonReader): RelativePath = RelativePath(in.readKeyAsString())
    override def encodeKey(x: RelativePath, out: JsonWriter): Unit = out.writeKey(x.pathString)
  }

  implicit val reactiveDirectoryValueCodec: JsonValueCodec[ReactiveDirectory] = new JsonValueCodec[ReactiveDirectory] {
    override def decodeValue(in: JsonReader, default: ReactiveDirectory): ReactiveDirectory =
      throw new UnsupportedOperationException()
    override def encodeValue(x: ReactiveDirectory, out: JsonWriter): Unit = pathValueCodec.encodeValue(x.path, out)
    override def nullValue: ReactiveDirectory = null
  }

  implicit val sourceFolderValueCodec: JsonValueCodec[SourceFolder] = new JsonValueCodec[SourceFolder] {
    override def decodeValue(in: JsonReader, default: SourceFolder): SourceFolder =
      throw new UnsupportedOperationException()
    override def encodeValue(x: SourceFolder, out: JsonWriter): Unit =
      pathValueCodec.encodeValue(x.workspaceSrcRootToSourceFolderPath.path, out)
    override def nullValue: SourceFolder = null
  }

  implicit val scalaVersionJsonValueCodec: JsonValueCodec[ScalaVersionConfig] = JsonCodecMaker.make
  implicit val tpaConfigJsonValueCodec: JsonValueCodec[TpaConfig] = JsonCodecMaker.make
  implicit val scopeConfigurationJsonValueCodec: JsonValueCodec[ScopeConfiguration] =
    JsonCodecMaker.make(
      CodecMakerConfig
        .withSetMaxInsertNumber(65536)
        .withMapMaxInsertNumber(65536)
        .withAllowRecursiveTypes(true)
    )

  implicit val dependencyInfoCodec: JsonCodec[DependencyInfo] = new JsonCodec[DependencyInfo] {
    val delimiter = ",,"

    def decode(str: String): DependencyInfo = {
      val Array(group, name, config, version, isMaven) =
        StringUtils.splitByWholeSeparatorPreserveAllTokens(str, delimiter)
      DependencyInfo(group, name, config, version, isMaven.toBoolean)
    }

    def encode(info: DependencyInfo): String =
      Seq(info.group, info.name, info.config, info.version, info.isMaven.toString).mkString(delimiter)

    override def decodeKey(in: JsonReader): DependencyInfo = decode(in.readKeyAsString())
    override def decodeValue(in: JsonReader, default: DependencyInfo): DependencyInfo = decode(in.readString(null))
    override def encodeKey(x: DependencyInfo, out: JsonWriter): Unit = out.writeKey(encode(x))
    override def encodeValue(x: DependencyInfo, out: JsonWriter): Unit = out.writeVal(encode(x))
    override def nullValue: DependencyInfo = null
  }

}
