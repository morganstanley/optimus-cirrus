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
package optimus.buildtool.format

import scala.collection.immutable.Seq

trait Message {
  def msg: String
  def file: ObtFile
  def line: Int
  def isError: Boolean

  override def toString: String = {
    val errorStr = if (isError) "ERROR" else "WARNING"
    s"[$errorStr] (at: ${file.path}:$line) $msg"
  }
}
object Message {
  def apply(msg: String, file: ObtFile, line: Int = 0, isError: Boolean = true): Message =
    if (isError) Error(msg, file, line)
    else Warning(msg, file, line)
}

final case class Warning(msg: String, file: ObtFile, line: Int = 0) extends Message {
  override val isError: Boolean = false
}
final case class Error(msg: String, file: ObtFile, line: Int = 0) extends Message {
  def failure: Failure = Failure(Seq(this))
  override val isError: Boolean = true
}
