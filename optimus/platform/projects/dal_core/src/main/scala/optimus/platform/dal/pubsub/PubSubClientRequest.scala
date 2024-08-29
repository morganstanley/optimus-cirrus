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
package optimus.platform.dal.pubsub

import optimus.dsi.partitioning.Partition
import optimus.platform.RuntimeEnvironment
import optimus.platform.dal.DALPubSub.ClientStreamId
import optimus.platform.dal.DALPubSub.NotificationStreamCallback
import optimus.platform.dsi.bitemporal.ChangeSubscription
import optimus.platform.dsi.bitemporal.ClosePubSubStream
import optimus.platform.dsi.bitemporal.CreatePubSubStream
import optimus.platform.dsi.bitemporal.PubSubCommand

sealed trait PubSubClientRequest {
  val cmd: PubSubCommand
}

object PubSubClientRequest {
  final case class ChangeSubRequest(cmd: ChangeSubscription) extends PubSubClientRequest
  final case class CreateStream(
      cmd: CreatePubSubStream,
      id: ClientStreamId,
      callback: NotificationStreamCallback,
      partition: Partition,
      env: RuntimeEnvironment
  ) extends PubSubClientRequest
  final case class CloseStream(cmd: ClosePubSubStream) extends PubSubClientRequest
}
