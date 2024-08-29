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
package optimus.platform.dsi.prc.cache

import com.ms.silverking.cloud.dht.common.KeyUtil
import com.ms.silverking.cloud.dht.common.SimpleKey
import optimus.dsi.session.EstablishSession
import optimus.platform.dal.prc.NormalizedNonCacheableCommand
import optimus.platform.dsi.bitemporal.Context
import optimus.platform.dsi.bitemporal.GetInfo
import optimus.platform.dsi.bitemporal.ResolvedClientAppIdentifier
import optimus.platform.dsi.prc.session.PrcClientSessionInfo

sealed trait PrcUserOptions

sealed trait CommandsPrcUserOptions extends PrcUserOptions {
  def prcKeyUserOptsList: Seq[NonTemporalPrcKeyUserOpts]
  def prcClientSessionInfo: PrcClientSessionInfo
  def establishCommand: Option[(Int, EstablishSession)]
  def resolvedClientAppIdentifier: ResolvedClientAppIdentifier
}

// User options for carrying the specific DAL commands being executed when a SilverKing retrieval is handled by the PRC
// trigger code. Since the trigger only gets the SK DHT Keys being loaded, and those are MD5 hashes of some actual DAL
// command, we need to supply the specific command(s) that should be executed for a DHT Key. We pack those in user
// options in this structure.
final case class ClientCommandsPrcUserOptions(
    prcKeyUserOptsList: Seq[NonTemporalPrcKeyUserOpts],
    prcClientSessionInfo: PrcClientSessionInfo,
    establishCommand: Option[(Int, EstablishSession)],
    resolvedClientAppIdentifier: ResolvedClientAppIdentifier)
    extends CommandsPrcUserOptions

// Same as the above, but used only by PrcDualReader to allow us to distinguish dual and non-dual read load
final case class DualReadCommandsPrcUserOptions(
    prcKeyUserOptsList: Seq[NonTemporalPrcKeyUserOpts],
    prcClientSessionInfo: PrcClientSessionInfo,
    establishCommand: Option[(Int, EstablishSession)],
    resolvedClientAppIdentifier: ResolvedClientAppIdentifier
) extends CommandsPrcUserOptions

// User options for requesting the raw bytes stored against a SilverKing DHT Key. There are several cases where we
// need this:
// (1) in a replicating PRC trigger
//     We need this because the PRC trigger intercepts SK retrievals and does all sorts of additional logic --
//     basically, deserializing the value stored against a DHT Key, turning it into a bitemporal space, and using the
//     temporality from CommandsPrcUserOptions to retrieve query results for the corresponding command. In this case,
//     we sometimes (principally for debugging purposes or tests) want to access the raw bytes stored against a key so
//     that we can check the cached data, without the PRC trigger trying to perform all that fancy logic on our behalf.
//     We use the RawValuePrcUserOptions to request that.
// (2) in a non-replicating PRC trigger
//     This is used by replication and read brokers using the SK namespace as a backend to request LSQTs, bitemporal
//     spaces of query results, etc.
case object RawValuePrcUserOptions extends PrcUserOptions

object LsqtQueryPrcUserOptions {
  // SK requires queries to be associated with a key, but we never actually read it so it can be any valid key.
  val lsqtDhtKey: Array[Byte] = KeyUtil.keyToBytes(SimpleKey.randomKey())
}

// User options for requesting the LSQT stored against a context. GetInfo is a valid NonTemporalPrcCommand,
// but using CommandsPrcUserOptions requires extra boilerplate like a placeholder session. This Option type allows
// for the LSQT to be retrieved without that extra work.
// Note that the PRC broker will reject this request if the user is not authorized as the broker's proid
// i.e. only admin tooling is expected and allowed to use this.
final case class LsqtQueryPrcUserOptions(ctxt: Context) extends PrcUserOptions {
  val key: NonTemporalPrcKey = NonTemporalPrcKey(NormalizedNonCacheableCommand(GetInfo()), ctxt)
}
