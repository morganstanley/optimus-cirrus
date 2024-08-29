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
package optimus.platform.dal

import optimus.platform.dal.config.DALEnvs
import optimus.platform.dal.config.DalAsyncBatchingConfig
import optimus.platform.dal.config.DalAsyncConfig
import optimus.platform.dal.config.DalAppId
import optimus.platform.dal.config.DalEnv
import optimus.platform.dal.config.DalZoneId
import optimus.platform.dsi.bitemporal.Context

final case class ClientContext(
    context: Context,
    zoneId: DalZoneId,
    appId: DalAppId,
    asyncConfig: DalAsyncConfig,
    brokerVirtualHostname: Option[String])

object ClientContext {
  def apply(context: Context, zoneId: DalZoneId, appId: DalAppId): ClientContext =
    this(context, zoneId, appId, DalAsyncBatchingConfig.default, None)
  def apply(context: Context, asyncConfig: DalAsyncConfig = DalAsyncBatchingConfig.default): ClientContext =
    this(context, DalZoneId.unknown, DalAppId.unknown, asyncConfig, None)
}

final case class ClientBrokerContext(
    context: Context,
    broker: String, // broker leader election group
    zoneId: DalZoneId,
    appId: DalAppId,
    cmdLimit: Int,
    asyncConfig: DalAsyncConfig,
    brokerVirtualHostname: Option[String] // broker hostname for authentication
) {
  lazy val clientContext = ClientContext(context, zoneId, appId, asyncConfig, brokerVirtualHostname)
  def leadCtx: ClientBrokerContext = if (cmdLimit == Int.MaxValue) this else this.copy(cmdLimit = Int.MaxValue)
}

object ClientBrokerContext {
  def apply(
      context: Context,
      zoneId: DalZoneId,
      appId: DalAppId,
      cmdLimt: Int,
      asyncConfig: DalAsyncConfig): ClientBrokerContext =
    this(context, "", zoneId, appId, cmdLimt, asyncConfig, None)
  def apply(context: Context, zoneId: DalZoneId, appId: DalAppId, cmdLimt: Int): ClientBrokerContext =
    this(context, "", zoneId, appId, cmdLimt)
  def apply(context: Context, broker: String, zoneId: DalZoneId, appId: DalAppId, cmdLimt: Int): ClientBrokerContext =
    apply(context, broker, zoneId, appId, cmdLimt, DalAsyncBatchingConfig.default, None)
  def apply(context: Context, broker: String, cmdLimt: Int, asyncConfig: DalAsyncConfig): ClientBrokerContext =
    this(context, broker, DalZoneId.unknown, DalAppId.unknown, cmdLimt, asyncConfig, None)
  def apply(context: Context, broker: String, cmdLimt: Int): ClientBrokerContext =
    apply(context, broker, cmdLimt, DalAsyncBatchingConfig.default)
  def apply(context: Context, cmdLimt: Int, asyncConfig: DalAsyncConfig): ClientBrokerContext =
    this(context, "", cmdLimt, asyncConfig)
  def apply(context: Context, cmdLimt: Int): ClientBrokerContext =
    apply(context, cmdLimt, DalAsyncBatchingConfig.default)

  final def brokerVirtualHostname(env: DalEnv): Option[String] =
    Some(DALEnvs.brokerVirtualHostname(env))
}
