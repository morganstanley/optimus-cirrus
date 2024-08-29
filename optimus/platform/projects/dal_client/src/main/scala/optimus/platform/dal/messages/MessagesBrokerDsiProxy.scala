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

import optimus.platform.dal.AbstractRemoteDSIProxyWithLeaderElector
import optimus.platform.dal.ClientBrokerContext
import optimus.platform.dal.config.DalEnv
import optimus.platform.dsi.bitemporal._
import optimus.platform.dsi.protobufutils.DALProtoClient
import optimus.platform.runtime.BrokerProvider
import optimus.platform.runtime.BrokerProviderResolver
import optimus.platform.runtime.Brokers
import optimus.platform.runtime.OptimusCompositeLeaderElectorClient

class MessagesBrokerDsiProxy(
    override val ctx: ClientBrokerContext,
    brokerProvider: BrokerProvider,
    env: DalEnv
) extends AbstractRemoteDSIProxyWithLeaderElector(ctx, brokerProvider)
    with MessagesDsiProxyBase {

  def this(ctx: ClientBrokerContext, brokerProviderResolver: BrokerProviderResolver, env: DalEnv) =
    this(ctx, brokerProviderResolver.resolve(ctx.broker), env)

  protected val messagesBrokerUri: String = ctx.broker

  override protected def createBrokerClient(): MessagesClient = {
    leaderElectorClient.withRandomBroker { randomBroker =>
      this synchronized {
        connStr = Some(randomBroker getOrElse {
          throw new DALRetryableActionException(s"No broker registered of type: ${ctx.broker}", None)
        })
        MessagesProtoClient(
          uriString = connStr.get,
          clientBrkContext = ctx,
          config = Some(new DALProtoClient.Configuration(ctx.asyncConfig, name = "MessagesProtoClient")),
          listener = callback,
          sessionCtx = sessionCtx,
          env = env
        )
      }
    }
  }

  override protected[messages] val dalEnv: DalEnv = env

  override protected def shouldCloseClient(brokers: Brokers): Boolean = {
    val leader = brokers.writeBroker
    val followers = brokers.readBrokers
    MessagesDsiProxyBase.shouldCloseIfConnStrIsNoLongerInLeaderOrFollower(connStr, leader, followers)
  }

  override def close(shutdown: Boolean): Unit = {
    super.close(shutdown)
    if (shutdown) closeStreamManager()
  }
}
