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
package optimus.platform.dsi.bitemporal

import optimus.dsi.partitioning.PartitionMap
import optimus.dsi.session.EstablishSession
import optimus.dsi.session.EstablishedClientSession
import optimus.graph.{Node, AlreadyFailedNode}
import optimus.platform.async
import optimus.platform.dsi.SupportedFeatures

trait DSI {
  def baseContext: Context
  def partitionMap: PartitionMap

  final def execute(cmd: Command): Seq[Result] = cmd match {
    case ro: ReadOnlyCommand   => executeReadOnlyCommands(ro :: Nil)
    case wp: LeadWriterCommand => executeLeadWriterCommands(wp :: Nil)
    case msg: MessagesCommand  => executeMessagesCommands(msg :: Nil)
    case _: PubSubCommand      =>
      // TODO (OPTIMUS-20203): Handle this..
      throw new IllegalArgumentException(s"Unexpected pub sub command.")
    case _: ServiceDiscoveryCommand =>
      throw new IllegalArgumentException(s"Unexpected service discovery command.")
  }

  def executeReadOnlyCommands(reads: Seq[ReadOnlyCommand]): Seq[Result]
  def executeLeadWriterCommands(cmds: Seq[LeadWriterCommand]): Seq[Result]
  def executeMessagesCommands(cmds: Seq[MessagesCommand]): Seq[MessagesResult]

  // shutdown=false; would allow cleaning/renewing some state to keep reusing the dsi
  // shutdown=true; would allow shutting down the dsi completely
  protected[optimus] def close(shutdown: Boolean): Unit

  // the EstablishSession command is not always used in the method, and it takes long time to calculate the classpathHash
  // which is a member of the EstablishSession command. To save the unnecessary cost, we make the command generation lazy
  private[optimus] def setEstablishSession(establishSession: => EstablishSession): Unit
  protected[optimus] def serverFeatures(): SupportedFeatures

  def supportsImpersonation: Boolean

  @async private[optimus] def getSession(tryEstablish: Boolean = true): EstablishedClientSession =
    getSession$queued(tryEstablish).result
  private[optimus] def getSession$queued(tryEstablish: Boolean): Node[EstablishedClientSession] =
    new AlreadyFailedNode(notImplemented("getSession"))

  @async private[optimus] def createNewSession(roles: Set[String]): EstablishedClientSession =
    createNewSession$queued(roles).result
  private[optimus] def createNewSession$queued(roles: Set[String]): Node[EstablishedClientSession] =
    new AlreadyFailedNode(notImplemented("createNewSession"))

  private[this] def notImplemented(what: String): Throwable =
    new UnsupportedSessionException(s"$what is not implemented for ${getClass.getSimpleName}")
}
