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
package optimus.buildtool.codereview

import spray.json.DefaultJsonProtocol._
import spray.json._

final case class CodeReviewAnalysis(
    prCommit: String,
    targetCommit: String,
    compilerInfo: Map[String, Map[String, Seq[CodeReviewMessage]]] // for each file, for each line, list its messages
)

object CodeReviewAnalysis {
  implicit val analysisFormat: RootJsonFormat[CodeReviewAnalysis] = jsonFormat(
    CodeReviewAnalysis.apply,
    fieldName1 = "commit_hash",
    fieldName2 = "target_branch_commit_hash",
    fieldName3 = "compiler_info")
}

final case class CodeReviewMessage(start: Int, end: Int, `type`: CodeReviewMessageType, msg: String)

object CodeReviewMessage {
  implicit val messageFormat: JsonFormat[CodeReviewMessage] = jsonFormat4(CodeReviewMessage.apply)
}

sealed trait CodeReviewMessageType

object CodeReviewMessageType {

  case object Info extends CodeReviewMessageType
  case object Warn extends CodeReviewMessageType
  case object Error extends CodeReviewMessageType

  implicit val messageTypeFormat: JsonFormat[CodeReviewMessageType] = new JsonFormat[CodeReviewMessageType] {
    override def write(obj: CodeReviewMessageType): JsValue = obj.toString.toLowerCase.toJson
    override def read(json: JsValue): CodeReviewMessageType = json match {
      case JsString(x) if x.equalsIgnoreCase(Info.toString)  => Info
      case JsString(x) if x.equalsIgnoreCase(Warn.toString)  => Warn
      case JsString(x) if x.equalsIgnoreCase(Error.toString) => Error
      case _ =>
        throw new IllegalArgumentException(
          s"value ${json.compactPrint} is not one of ${Info.toString}, ${Warn.toString} or ${Error.toString}")
    }
  }

}
