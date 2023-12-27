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

import java.net.URL
import java.nio.file.Path
import java.nio.file.Paths
import java.time.Instant

import optimus.buildtool.config.NamingConventions
import optimus.buildtool.config.ScopeId
import optimus.buildtool.files.Asset
import optimus.buildtool.files.FileAsset
import optimus.buildtool.files.JarAsset
import optimus.buildtool.files.JsonAsset
import optimus.buildtool.files.RelativePath
import optimus.buildtool.resolvers.DependencyInfo
import optimus.buildtool.trace.MessageTrace
import optimus.buildtool.trace.ResolveTrace
import optimus.buildtool.utils.PathUtils

//noinspection TypeAnnotation
private[buildtool] object JsonImplicits {
  import spray.json.DefaultJsonProtocol._
  import spray.json._

  implicit val JsonAssetFormatter: JsonFormat[JsonAsset] = new JsonFormat[JsonAsset] {
    override def write(obj: JsonAsset): JsValue = obj.path.toString.toJson
    override def read(json: JsValue): JsonAsset = JsonAsset(Paths.get(json.convertTo[String]))
  }
  implicit val ScopeIdFormatter: RootJsonFormat[ScopeId] = jsonFormat4(ScopeId.apply)
  class ArtifactTypeFormatter[A <: ArtifactType] extends RootJsonFormat[A] {
    override def write(obj: A): JsValue = obj.name.toJson
    override def read(json: JsValue): A = ArtifactType.parse(json.convertTo[String]).asInstanceOf[A]
  }
  implicit val BasicArtifactTypeFormatter: JsonFormat[ArtifactType] = new ArtifactTypeFormatter[ArtifactType]
  implicit val CachedArtifactTypeFormatter: JsonFormat[CachedArtifactType] =
    new ArtifactTypeFormatter[CachedArtifactType]
  implicit val GeneratedSourceArtifactTypeFormatter: JsonFormat[GeneratedSourceArtifactType] =
    new ArtifactTypeFormatter[GeneratedSourceArtifactType]
  implicit val AnalysisArtifactTypeFormatter: JsonFormat[AnalysisArtifactType] =
    new ArtifactTypeFormatter[AnalysisArtifactType]
  implicit val MessageTraceFormatter: JsonFormat[MessageTrace] = new JsonFormat[MessageTrace] {
    override def write(obj: MessageTrace): JsValue = obj.name.toJson
    override def read(json: JsValue): MessageTrace = MessageTrace.parse(json.convertTo[String])
  }
  implicit val ResolveTraceFormatter: JsonFormat[ResolveTrace] = new JsonFormat[ResolveTrace] {
    override def write(obj: ResolveTrace): JsValue = obj.name.toJson
    override def read(json: JsValue): ResolveTrace = ResolveTrace.parse(json.convertTo[String])
  }
  implicit val InternalArtifactIdFormatter: RootJsonFormat[InternalArtifactId] = jsonFormat3(InternalArtifactId.apply)
  implicit val MessagePositionFormatter: RootJsonFormat[MessagePosition] = jsonFormat7(MessagePosition.apply)
  implicit val SeverityFormatter: JsonFormat[Severity] { def write(severity: Severity): JsString } =
    new JsonFormat[Severity] {
      import spray.json.DefaultJsonProtocol._
      override def read(json: JsValue): Severity = Severity.parse(json.convertTo[String])
      override def write(severity: Severity): JsString = JsString(severity.toString)
    }
  implicit val CompilationMessageFormatter: RootJsonFormat[CompilationMessage] = jsonFormat6(CompilationMessage.apply)
  implicit val ExternalIdFormatter: RootJsonFormat[ExternalId] = jsonFormat3(ExternalId.apply)
  implicit val InternalDependencyLookupFormatter: RootJsonFormat[DependencyLookup[ScopeId]] = jsonFormat2(
    DependencyLookup.apply[ScopeId])
  implicit val ExternalDependencyLookupFormatter: RootJsonFormat[DependencyLookup[ExternalId]] = jsonFormat2(
    DependencyLookup.apply[ExternalId])
  implicit val PathFormatter: JsonFormat[Path] = new JsonFormat[Path] {
    override def write(obj: Path): JsValue = PathUtils.platformIndependentString(obj).toJson
    override def read(json: JsValue): Path = Paths.get(json.convertTo[String])
  }
  implicit val UrlFormatter: JsonFormat[URL] = new JsonFormat[URL] {
    override def write(obj: URL): JsValue = obj.toString.toJson
    override def read(json: JsValue): URL = new URL(json.convertTo[String])
  }
  implicit val AssetFormat: JsonFormat[Asset] = new JsonFormat[Asset] {
    override def write(obj: Asset): JsValue = obj.path.toJson
    override def read(json: JsValue): Asset = Asset(json.convertTo[Path])
  }
  implicit val RelativePathFormatter: JsonFormat[RelativePath] = new JsonFormat[RelativePath] {
    override def write(obj: RelativePath): JsValue = obj.path.toJson
    override def read(json: JsValue): RelativePath = RelativePath(json.convertTo[Path])
  }
  implicit val FileAssetFormatter: JsonFormat[FileAsset] = new JsonFormat[FileAsset] {
    override def write(obj: FileAsset): JsValue = obj.path.toJson
    override def read(json: JsValue): FileAsset = FileAsset(json.convertTo[Path])
  }
  implicit val JarAssetFormatter: JsonFormat[JarAsset] = new JsonFormat[JarAsset] {
    override def write(obj: JarAsset): JsValue = obj.pathString.toJson
    override def read(json: JsValue): JarAsset = {
      if (NamingConventions.isHttpOrHttps(json.convertTo[String])) JarAsset(json.convertTo[URL])
      else JarAsset(json.convertTo[Path])
    }
  }
  implicit var InstantFormatter: JsonFormat[Instant] = new JsonFormat[Instant] {
    override def write(obj: Instant): JsValue = obj.toString.toJson
    override def read(json: JsValue): Instant = Instant.parse(json.convertTo[String])
  }
  implicit val CompilerMessagesArtifactCachedFormatter: RootJsonFormat[CompilerMessagesArtifact.Cached] = jsonFormat7(
    CompilerMessagesArtifact.Cached.apply)
  implicit val LocatorArtifactFormatter: RootJsonFormat[LocatorArtifact.Cached] = jsonFormat4(
    LocatorArtifact.Cached.apply)
  implicit val RootLocatorArtifactFormatter: RootJsonFormat[RootLocatorArtifact.Cached] = jsonFormat2(
    RootLocatorArtifact.Cached.apply)

  implicit object ExternalArtifactTypeFormat extends RootJsonFormat[ExternalArtifactType] {
    override def write(t: ExternalArtifactType): JsValue = t.name.toJson
    override def read(json: JsValue): ExternalArtifactType = ExternalArtifactType.parse(json.convertTo[String]) match {
      case cat: ExternalArtifactType => cat
      case t                         => throw new IllegalArgumentException(s"Unexpected external artifact type: $t")
    }
  }
  implicit val VersionedExternalArtifactIdFormatter: RootJsonFormat[VersionedExternalArtifactId] = jsonFormat5(
    VersionedExternalArtifactId.apply)
  implicit val SingletonArtifactIdFormatter: RootJsonFormat[SingletonArtifactId] = jsonFormat1(
    SingletonArtifactId.apply)
  implicit val PathedExternalArtifactIdFormatter: RootJsonFormat[PathedExternalArtifactId] = jsonFormat2(
    PathedExternalArtifactId.apply)

  implicit val ExternalArtifactIdFormatter: JsonFormat[ExternalArtifactId] = new JsonFormat[ExternalArtifactId] {
    override def write(obj: ExternalArtifactId): JsValue = {
      val j = obj match {
        case v: VersionedExternalArtifactId => VersionedExternalArtifactIdFormatter.write(v)
        case p: PathedExternalArtifactId    => PathedExternalArtifactIdFormatter.write(p)
      }
      JsObject("tpe" -> obj.getClass.getSimpleName.toJson, "value" -> j)
    }
    override def read(json: JsValue): ExternalArtifactId = {
      val o = json.asJsObject
      val v = o.fields("value").asJsObject
      o.fields("tpe").convertTo[String] match {
        case "VersionedExternalArtifactId" => VersionedExternalArtifactIdFormatter.read(v)
        case "PathedExternalArtifactId"    => PathedExternalArtifactIdFormatter.read(v)
      }
    }
  }

  implicit val ArtifactIdFormatter: JsonFormat[ArtifactId] = new JsonFormat[ArtifactId] {
    override def write(obj: ArtifactId): JsValue = {
      def withType(j: JsValue): JsObject =
        JsObject("tpe" -> obj.getClass.getSimpleName.toJson, "value" -> j)
      obj match {
        case s: SingletonArtifactId => withType(SingletonArtifactIdFormatter.write(s))
        case i: InternalArtifactId  => withType(InternalArtifactIdFormatter.write(i))
        case e: ExternalArtifactId  => ExternalArtifactIdFormatter.write(e)
      }
    }
    override def read(json: JsValue): ArtifactId = {
      val o = json.asJsObject
      val v = o.fields("value").asJsObject
      o.fields("tpe").convertTo[String] match {
        case "SingletonArtifactId" => SingletonArtifactIdFormatter.read(v)
        case "InternalArtifactId"  => InternalArtifactIdFormatter.read(v)
        case _                     => ExternalArtifactIdFormatter.read(json)
      }
    }
  }

  implicit val visualiserInfoFormatter: JsonFormat[DependencyInfo] = new JsonFormat[DependencyInfo] {
    override def write(obj: DependencyInfo): JsValue = {
      JsString(obj.module + "," + obj.config + "," + obj.version)
    }
    override def read(json: JsValue): DependencyInfo = {
      val obj = json.convertTo[String]
      val Array(module, config, version) = obj.split(",", 3)
      DependencyInfo(module, config, version)
    }
  }

  implicit val CachedExternalClassFileArtifactFormatter: RootJsonFormat[ExternalClassFileArtifact.Cached] = jsonFormat8(
    ExternalClassFileArtifact.Cached.apply)
  implicit val CachedResolutionArtifactFormatter: RootJsonFormat[ResolutionArtifact.Cached] = jsonFormat9(
    ResolutionArtifact.Cached.apply)
  implicit val CachedGenericFilesArtifactFormatter: RootJsonFormat[GenericFilesArtifact.Cached] = jsonFormat2(
    GenericFilesArtifact.Cached)

  implicit val MessagesMetadataFormatter: RootJsonFormat[MessagesMetadata] = jsonFormat2(MessagesMetadata.apply)
  implicit val GeneratedSourceMetadataFormatter: RootJsonFormat[GeneratedSourceMetadata] = jsonFormat4(
    GeneratedSourceMetadata.apply)
  implicit val CppMetadataFormatter: RootJsonFormat[CppMetadata] = jsonFormat5(CppMetadata.apply)
  implicit val ProcessorMetadataFormatter: RootJsonFormat[ProcessorMetadata] = jsonFormat3(ProcessorMetadata.apply)

}
