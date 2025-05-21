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
package optimus.platform.dal.versioning.worker

import optimus.dsi.partitioning.PartitionMap
import optimus.dsi.session.EstablishSession
import optimus.platform.async
import optimus.platform.dal.session.ClientSessionContext.SessionData
import optimus.platform.dal.ClientSideDSI
import optimus.platform.dal.DSIClient
import optimus.platform.dal.SessionFetcher
import optimus.platform.dsi.SupportedFeatures
import optimus.platform.dsi.bitemporal._

trait WriteFail {
  final protected def failLeadWriterCommands(cmds: Seq[LeadWriterCommand]): Nothing =
    throw new WriteCommandExecutedInReadOnlyDsiException(cmds.mkString(";"))
}

final case class ReadOnlyClientSideDsi(private val underlying: ClientSideDSI, override val partitionMap: PartitionMap)
    extends ClientSideDSI
    with WriteFail {
  @async override def executeLeadWriterCommands(cmds: Seq[LeadWriterCommand]): Seq[Result] =
    failLeadWriterCommands(cmds)

  @async override def executeReadOnlyCommands(reads: Seq[ReadOnlyCommand]): Seq[Result] =
    underlying.executeReadOnlyCommands(reads)

  private[optimus] override def getDSIClient: DSIClient = underlying.getDSIClient

  override def sessionData: SessionData =
    underlying.sessionData

  override protected[optimus] def serverFeatures(): SupportedFeatures =
    underlying.serverFeatures()

  override private[optimus] def setEstablishSession(establishSession: => EstablishSession): Unit =
    underlying.setEstablishSession(establishSession)

  private[optimus] override def bindSessionFetcher(sessionFetcher: SessionFetcher): Unit = {
    underlying.bindSessionFetcher(sessionFetcher)
  }

  override def baseContext: Context =
    underlying.baseContext

  override def close(shutdown: Boolean) = underlying.close(shutdown)
}

final case class ReadOnlyDsi(private val underlying: DSI, override val partitionMap: PartitionMap)
    extends DSI
    with WriteFail {
  override def baseContext: Context =
    underlying.baseContext

  override protected[optimus] def serverFeatures(): SupportedFeatures =
    underlying.serverFeatures()

  override def executeReadOnlyCommands(reads: Seq[ReadOnlyCommand]): Seq[Result] =
    underlying.executeReadOnlyCommands(reads)

  override def executeLeadWriterCommands(cmds: Seq[LeadWriterCommand]): Seq[Result] = failLeadWriterCommands(cmds)

  override private[optimus] def setEstablishSession(establishSession: => EstablishSession): Unit =
    underlying.setEstablishSession(establishSession)

  override def executeMessagesCommands(cmds: Seq[MessagesCommand]): Seq[MessagesResult] =
    throw new UnsupportedOperationException(s"Messages commands are not supported by this DSI!")

  override def close(shutdown: Boolean) = underlying.close(shutdown)

  override final def supportsImpersonation = underlying.supportsImpersonation
}
