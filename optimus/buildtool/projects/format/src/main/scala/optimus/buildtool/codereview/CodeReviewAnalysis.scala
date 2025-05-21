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

import com.github.plokhotnyuk.jsoniter_scala.core.JsonValueCodec
import com.github.plokhotnyuk.jsoniter_scala.macros.named
import com.github.plokhotnyuk.jsoniter_scala.macros.JsonCodecMaker

final case class CodeReviewAnalysis(
    @named("commit_hash") prCommit: String,
    @named("target_branch_commit_hash") targetCommit: String,
    @named("compiler_info") compilerInfo: Map[String, Map[String, Seq[
      CodeReviewMessage
    ]]] // for each file, for each line, list its messages
)

object CodeReviewCodecs {
  implicit lazy val codeReviewValueCodec: JsonValueCodec[CodeReviewAnalysis] = JsonCodecMaker.make
}

final case class CodeReviewMessage(start: Int, end: Int, `type`: CodeReviewMessageType.Value, msg: String)

object CodeReviewMessageType extends Enumeration {
  val Info, Warn, Error = Value
}
