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
import optimus.platform.dsi.FeatureSets
import optimus.platform.dsi.SupportedFeatures
import optimus.platform.dsi.bitemporal.proto.FeatureInfo

/**
 * A fake DSI which is used when you specify --env none. Unlike --env mock, this DSI does not provide any DAL emulation.
 * Use this if you don't need a DAL at all and you want faster initialization for tests or simple apps.
 *
 * It basically only supports GetInfo command (which is needed for initialization) and nothing else.
 */
object NoDSI extends DSI {
  def baseContext: Context = DefaultContext
  def partitionMap: PartitionMap = PartitionMap.empty
  def executeReadOnlyCommands(reads: Seq[ReadOnlyCommand]): Seq[Result] = reads.map {
    case GetInfo() => GetInfoResult(patch.MilliInstant.now(), FeatureInfo(-1, "NoDSI"))
    case c         => throw new UnsupportedOperationException(s"NoDSI (i.e. --env none) does not support command: $c")
  }
  def executeLeadWriterCommands(cmds: Seq[LeadWriterCommand]): Seq[Result] =
    throw new UnsupportedOperationException(s"NoDSI (i.e. --env none) does not support any write commands")
  override def executeMessagesCommands(cmds: Seq[MessagesCommand]): Seq[MessagesResult] =
    throw new UnsupportedOperationException(s"NoDSI (i.e. --env none) does not support any message commands")
  protected[optimus] def close(shutdown: Boolean): Unit = ()
  private[optimus] def setEstablishSession(establishSession: => EstablishSession): Unit = ()
  protected[optimus] def serverFeatures(): SupportedFeatures = FeatureSets.None
  def supportsImpersonation: Boolean = false
}
