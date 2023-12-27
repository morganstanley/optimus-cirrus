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
package optimus.buildtool.cache.silverking

import java.nio.file.Path
import java.nio.file.Paths

import optimus.buildtool.artifacts.ArtifactType
import optimus.buildtool.artifacts.CachedArtifactType
import optimus.buildtool.artifacts.JsonImplicits._
import optimus.buildtool.files.RelativePath
import spray.json._

object ArtifactJsonProtocol extends DefaultJsonProtocol {
  import SilverKingStore._

  implicit val PathFormatter: JsonFormat[Path] = new JsonFormat[Path] {
    override def write(obj: Path): JsValue = obj.toString.toJson
    override def read(json: JsValue): Path = Paths.get(json.convertTo[String])
  }

  implicit val RelativePathFormatter: JsonFormat[RelativePath] = new JsonFormat[RelativePath] {
    override def write(obj: RelativePath): JsValue = obj.pathString.toJson
    override def read(json: JsValue): RelativePath = RelativePath(Paths.get(json.convertTo[String]))
  }

  private[cache] implicit object AssetContentFormat extends RootJsonFormat[AssetContent] {
    override def write(obj: AssetContent): JsValue = obj match {
      case c: DirectoryContent => toJson(c, "DirectoryContent")
      case c: FileContent      => toJson(c, "FileContent")
    }
    override def read(json: JsValue): AssetContent = objectType(json) match {
      case "DirectoryContent" => fromJson[DirectoryContent](json)
      case "FileContent"      => fromJson[FileContent](json)
    }

    private def objectType(json: JsValue): String =
      json.asJsObject.fields("type").convertTo[String]

    private def fromJson[A: JsonReader](json: JsValue): A = json.convertTo[A]

    private def toJson[A: JsonWriter](obj: A, objType: String): JsObject = {
      val o = obj.toJson.asJsObject
      o.copy(fields = o.fields + ("type" -> objType.toJson))
    }
  }

  private[cache] implicit val FCFormat: JsonFormat[FileContent] = jsonFormat1(FileContent.apply)
  private[cache] implicit val DCFormat: JsonFormat[DirectoryContent] = jsonFormat1(DirectoryContent.apply)

  private[cache] implicit object CachedArtifactTypeFormat extends RootJsonFormat[CachedArtifactType] {
    override def write(t: CachedArtifactType): JsValue = if (t != null) t.name.toJson else JsNull
    override def read(json: JsValue): CachedArtifactType = json match {
      case JsNull => null
      case _ =>
        ArtifactType.parse(json.convertTo[String]) match {
          case cat: CachedArtifactType => cat
          case t => throw new IllegalArgumentException(s"Unexpected (non-cached) artifact type: $t")
        }
    }
  }

  implicit val ArtifactKeyFormat: JsonFormat[ArtifactKey] = jsonFormat5(ArtifactKey.apply)

  private[cache] implicit val FileSegmentKeyFormat: JsonFormat[FileSegmentKey] = jsonFormat3(FileSegmentKey.apply)

  private[cache] implicit val DirectoryFileKeyFormat: JsonFormat[DirectoryFileKey] = jsonFormat2(DirectoryFileKey.apply)

  private[cache] implicit val DirectoryFileFormat: JsonFormat[DirectoryFile] = jsonFormat2(DirectoryFile.apply)

}
