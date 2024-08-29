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
package optimus.platform.dal.session

import java.time.Instant

import optimus.config.RuntimeConfiguration
import optimus.dsi.session.EstablishSession
import optimus.dsi.session.SlotMap
import optimus.platform.dal.ClientMachineIdentifier
import optimus.platform.dal.OnBehalfTokenType
import optimus.platform.dal.RuntimeProperties
import optimus.logging.Pid
import optimus.platform.dal.config.DalAppId
import optimus.platform.dal.config.DalZoneId
import optimus.platform.dsi.SupportedFeatures
import optimus.platform.dsi.bitemporal.ClientAppIdentifier
import optimus.platform.dsi.bitemporal.Context
import optimus.platform.dsi.bitemporal.DSI
import optimus.session.utils.ClientEnvironment

class InitSessionInitialisers {
  private[dal] lazy val supportedFeatures = SupportedFeatures.myFeatures
  private[dal] lazy val classpathHash = ClientEnvironment.classpathHash
  private[dal] lazy val clientPath = ClientEnvironment.clientPath
}

trait InitSession {
  private val initialisers = new InitSessionInitialisers
  private def supportedFeatures = initialisers.supportedFeatures
  private def classpathHash = initialisers.classpathHash
  private def clientPath = initialisers.clientPath

  private[optimus] def initSession(
      dsi: DSI,
      appIdentifier: ClientAppIdentifier,
      realId: String,
      effectiveId: Option[String],
      rolesetMode: RolesetMode,
      establishmentTime: Option[Instant],
      context: Context,
      slotMapOverrides: Option[SlotMap],
      onBehalfTokenType: OnBehalfTokenType,
      onBehalfSessionToken: Option[Vector[Byte]],
      clientMachineIdentifier: ClientMachineIdentifier
  ): Unit = {
    def command = EstablishSession(
      appIdentifier,
      supportedFeatures,
      context,
      realId,
      effectiveId,
      rolesetMode,
      establishmentTime,
      classpathHash,
      None,
      Pid.pidInt,
      slotMapOverrides,
      onBehalfTokenType,
      onBehalfSessionToken,
      clientPath,
      clientMachineIdentifier
    )
    dsi.setEstablishSession(command)
  }

  protected final def initSession(
      dsi: DSI,
      runtimeConfig: RuntimeConfiguration,
      overrideRolesetMode: Option[RolesetMode] = None): Unit = {
    val appId: DalAppId =
      runtimeConfig.getString(RuntimeProperties.DsiAppIdProperty).map(DalAppId(_)).getOrElse(DalAppId.unknown)
    val zoneId: DalZoneId = runtimeConfig
      .getString(RuntimeProperties.DsiZoneProperty)
      .map(DalZoneId(_))
      .getOrElse(RuntimeProperties.DsiZonePropertyDefaultValue)
    val realId: String =
      runtimeConfig.getString(RuntimeProperties.DsiSessionRealIdProperty).getOrElse(System.getProperty("user.name"))
    val effectiveId: Option[String] = runtimeConfig.getString(RuntimeProperties.DsiSessionEffectiveIdProperty)
    val establishmentTime: Option[Instant] =
      runtimeConfig.getInstant(RuntimeProperties.DsiSessionEstablishmentTimeProperty)

    val rolesetMode: RolesetMode =
      overrideRolesetMode
        .orElse(runtimeConfig.get(RuntimeProperties.DsiSessionRolesetModeProperty).map(_.asInstanceOf[RolesetMode]))
        .getOrElse(RolesetMode.Default)

    val slotMapOverrides: Option[SlotMap] =
      runtimeConfig.get(RuntimeProperties.DsiSessionSlotMapOverridesProperty) collect { case sm: SlotMap =>
        sm
      }
    val onBehalfTokenType = runtimeConfig
      .get(RuntimeProperties.DsiOnBehalfTokenType)
      .map(_.asInstanceOf[OnBehalfTokenType])
      .getOrElse(OnBehalfTokenType.Default)
    val onBehalfSessionToken: Option[Vector[Byte]] =
      runtimeConfig.getByteVector(RuntimeProperties.DsiOnBehalfSessionToken)
    val appIdentifier: ClientAppIdentifier = ClientAppIdentifier(zoneId, appId)
    val clientMachineIdentifier: ClientMachineIdentifier = ClientEnvironment.getClientMachineIdentifier
    initSession(
      dsi,
      appIdentifier,
      realId,
      effectiveId,
      rolesetMode,
      establishmentTime,
      dsi.baseContext,
      slotMapOverrides,
      onBehalfTokenType,
      onBehalfSessionToken,
      clientMachineIdentifier
    )
  }
}
