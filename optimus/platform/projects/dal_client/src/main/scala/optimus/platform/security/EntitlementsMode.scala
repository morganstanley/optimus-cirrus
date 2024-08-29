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
package optimus.platform.security

// TODO (OPTIMUS-13429): 20130919 we can remove this when we no longer have to support multiple entitlements mode. It will be great!
sealed trait EntitlementsMode
object EntitlementsMode {
  case object New extends EntitlementsMode
  case object Mixed extends EntitlementsMode
  case object Old extends EntitlementsMode

  def fromString(model: String): EntitlementsMode = fromString2(Option(model))

  private[this] def fromString2(model: Option[String]): EntitlementsMode = model match {
    case Some("mixed") => Mixed
    case Some("new")   => New
    // TODO (OPTIMUS-13429): is defaulting to old-mode the right thing to do?
    case _ => Old
  }

  def isValid(model: String) = model == "old" || model == "new" || model == "mixed"
}

class EntitlementsModeException(message: Option[String], cause: Option[Throwable])
    extends RuntimeException(message.getOrElse(null), cause.getOrElse(null)) {
  def this(message: String) = this(Some(message), None)
}
