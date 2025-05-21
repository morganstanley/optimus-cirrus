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
package optimus.buildtool.builders.postbuilders.metadata

import com.github.plokhotnyuk.jsoniter_scala.core._
import com.github.plokhotnyuk.jsoniter_scala.macros._

object MetadataJsonImplicits {
  implicit val metaBundleReportCodec: JsonValueCodec[MetaBundleReport] =
    JsonCodecMaker.make(CodecMakerConfig.withSetMaxInsertNumber(65536))

  implicit val qualifierReportFormat: JsonValueCodec[QualifierReport] = new JsonValueCodec[QualifierReport] {

    override def decodeValue(in: JsonReader, default: QualifierReport): QualifierReport = in.readString(null) match {
      case "at_build"   => Compile
      case "at_runtime" => Runtime
      case "at_test"    => TestOnly
      case "tooling"    => Tooling
    }

    override def encodeValue(x: QualifierReport, out: JsonWriter): Unit = {
      val serialized = x match {
        case Compile  => "at_build"
        case Runtime  => "at_runtime"
        case TestOnly => "at_test"
        case Tooling  => "tooling"
      }
      out.writeVal(serialized)
    }

    override def nullValue: QualifierReport = null
  }

}
