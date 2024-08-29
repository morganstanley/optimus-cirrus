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
package optimus.platform.dal.client

import optimus.platform.dsi.bitemporal.proto.CommandAppNameTag
import optimus.platform.dsi.bitemporal.proto.CommandLocation
import optimus.platform.dsi.bitemporal.proto.Dsi.ClientAppIdentifierProto
import optimus.platform.dsi.bitemporal.proto.Dsi.ContextProto
import optimus.platform.dsi.protobufutils.BatchContext

private[platform] trait BatchedRequestSender {
  type MessageType

  private[platform] def start(): Unit
  private[platform] def shutdown(): Unit
  private[platform] def assertConnected(): Unit
  private[platform] def buildMessage(
      seqId: Int,
      batchContext: BatchContext,
      contextProto: ContextProto,
      clientAppIdProto: ClientAppIdentifierProto,
      commandLocations: Seq[CommandLocation],
      commandTags: Seq[CommandAppNameTag]): MessageType
  private[platform] def asyncSend(message: MessageType): Unit
}

trait BatchedRequestSenderFactory {
  def createSender(): BatchedRequestSender
}
