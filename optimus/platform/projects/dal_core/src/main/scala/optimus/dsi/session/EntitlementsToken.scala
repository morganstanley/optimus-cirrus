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
package optimus.dsi.session

import java.time.Instant

sealed trait EntitlementsToken {
  def toLoggableString() = toString
}

final case class EntitlementsTokenSuccess(
    // TODO (OPTIMUS-22471): move to using only a non-temporal token
    temporalHash: String,
    nonTemporalHash: Option[String],
    grantedFor: String,
    tokenSignature: Vector[Byte],
    description: String,
    env: String,
    sessionTxTime: Instant)
    extends EntitlementsToken {

  // we exclude tokenSignature here
  override def toLoggableString() =
    s"EntitlementsTokenSuccess($temporalHash, $grantedFor, $description, $env, $sessionTxTime)"

}

final case class EntitlementsTokenFailure(reason: String) extends EntitlementsToken

final case class EntitlementsTokenUnsupported(reason: String) extends EntitlementsToken
