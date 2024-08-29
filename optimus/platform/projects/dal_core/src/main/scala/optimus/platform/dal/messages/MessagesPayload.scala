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
package optimus.platform.dal.messages

import optimus.breadcrumbs.ChainedID
import optimus.platform.storable.SerializedContainedEvent
import optimus.platform.dsi.bitemporal.ClientAppIdentifier
import optimus.platform.storable.SerializedUpsertableTransaction

import java.time.Instant

sealed trait MessagesPayloadBase {
  def publishTime: Instant
  def clientAppId: ClientAppIdentifier
  def user: String
  def publisherChainId: ChainedID
  def rId: String
}

/** MessagePayload used for ContainedEvent case. */
final case class MessagesPayload(
    publishTime: Instant,
    clientAppId: ClientAppIdentifier,
    user: String,
    publisherChainId: ChainedID,
    rId: String,
    message: SerializedContainedEvent
) extends MessagesPayloadBase

/** MessagePayload used for UpsertableTransaction case. */
final case class MessagesTransactionPayload(
    publishTime: Instant,
    clientAppId: ClientAppIdentifier,
    user: String,
    publisherChainId: ChainedID,
    rId: String,
    message: SerializedUpsertableTransaction
) extends MessagesPayloadBase
