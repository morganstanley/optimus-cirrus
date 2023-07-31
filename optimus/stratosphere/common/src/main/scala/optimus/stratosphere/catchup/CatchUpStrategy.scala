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
package optimus.stratosphere.catchup

sealed trait CatchUpStrategy

object CatchUpStrategy {
  val All: Seq[CatchUpStrategy] = List(Mixed, Git, Obt)
  val GitCompatible: Seq[CatchUpStrategy] = List(Mixed, Git)
  val ObtCompatible: Seq[CatchUpStrategy] = List(Mixed, Obt)

  case object Mixed extends CatchUpStrategy
  case object Git extends CatchUpStrategy
  case object Obt extends CatchUpStrategy

  def apply(txt: String): CatchUpStrategy =
    txt.toLowerCase match {
      case "mixed" => Mixed
      case "git"   => Git
      case "obt"   => Obt
      case _ =>
        throw new IllegalArgumentException(s"Unknown catchup strategy $txt. Possible values are ${All.mkString(", ")}.")
    }
}
