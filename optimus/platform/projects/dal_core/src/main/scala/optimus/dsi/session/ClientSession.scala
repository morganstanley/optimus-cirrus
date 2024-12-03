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
package optimus.dsi.session

import optimus.platform.ImmutableArray
import optimus.platform.dal.config.DalAppId
import optimus.platform.dsi.bitemporal.ResolvedClientAppIdentifier
import optimus.platform.dsi.SupportedFeatures
import optimus.platform.dsi.bitemporal.{ClientAppIdentifier, Context}
import java.time.Instant

import optimus.dsi.session.JarPathT.ExistingNonStorableJarPath
import optimus.platform.dal.ClientMachineIdentifier
import optimus.platform.dal.OnBehalfTokenType
import optimus.platform.dal.session.RolesetMode
import optimus.platform.dsi.versioning.PathHashRepr
import optimus.platform.dsi.versioning.PathHashT

final case class EstablishedClientSession(
    // The application identifier that actually gets used can differ from that sent in the establish
    // This is the resolved one
    applicationIdentifier: ResolvedClientAppIdentifier,
    applicationConfigFound: Boolean,
    sessionId: Int,
    serverFeatures: SupportedFeatures,
    rolesetMode: RolesetMode.Established,
    establishmentTime: Instant,
    establishingCommand: EstablishSession,
    encryptedSessionToken: Option[Vector[Byte]], // this must be a OnBehalfSessionToken if present
    entitlementsToken: EntitlementsToken,
    profileKeyHash: Option[ProfileKeyHash]) {
  val effectiveId: String = establishingCommand.effectiveId getOrElse establishingCommand.realId
  val clientFeatures: SupportedFeatures = establishingCommand.supportedFeatures

  def reestablishCommand: EstablishSession = {
    // No need to redo an AllRoles lookup again if we did one first time around -- use the RolesetMode.Established
    // from the established session instead
    val resolvedRolesetMode: RolesetMode.Established = rolesetMode
    establishingCommand.copy(
      sessionInfo = establishingCommand.sessionInfo
        .copy(establishmentTime = Some(establishmentTime), rolesetMode = resolvedRolesetMode)
    )
  }
}

final case class ClientSessionInfo(
    realId: String,
    effectiveId: Option[String],
    rolesetMode: RolesetMode,
    establishmentTime: Option[Instant],
    onBehalfTokenType: OnBehalfTokenType,
    onBehalfSessionToken: Option[Vector[Byte]],
    applicationIdentifier: ClientAppIdentifier,
    clientMachineIdentifier: ClientMachineIdentifier) {
  private[dsi] val appId: DalAppId = applicationIdentifier.appId

  override def toString: String = {
    // exclude session token
    s"ClientSessionInfo($realId,$effectiveId,$rolesetMode,$establishmentTime,$onBehalfTokenType,$applicationIdentifier,$clientMachineIdentifier)"
  }
}

final case class EstablishSession(
    sessionInfo: ClientSessionInfo,
    supportedFeatures: SupportedFeatures,
    context: Context,
    classpathHash: PathHashT,
    fullClasspath: Option[Seq[ExistingNonStorableJarPath]],
    pid: Option[Int],
    slotMapOverrides: Option[SlotMap],
    clientPath: String) {
  def applicationIdentifier = sessionInfo.applicationIdentifier
  def realId: String = sessionInfo.realId
  def effectiveId: Option[String] = sessionInfo.effectiveId
  def rolesetMode: RolesetMode = sessionInfo.rolesetMode
  def establishmentTime: Option[Instant] = sessionInfo.establishmentTime
  def onBehalfTokenType: OnBehalfTokenType = sessionInfo.onBehalfTokenType
  def onBehalfSessionToken: Option[Vector[Byte]] = sessionInfo.onBehalfSessionToken
  private[optimus] def establishAllRoles: Boolean = rolesetMode == RolesetMode.AllRoles
}
object EstablishSession {
  def apply(
      applicationIdentifier: ClientAppIdentifier,
      supportedFeatures: SupportedFeatures,
      context: Context,
      realId: String,
      effectiveId: Option[String],
      rolesets: Seq[Set[String]],
      establishmentTime: Option[Instant],
      classpathHash: PathHashT,
      fullClasspath: Option[Seq[ExistingNonStorableJarPath]],
      pid: Option[Int],
      slotMapOverrides: Option[SlotMap],
      onBehalfTokenType: OnBehalfTokenType,
      onBehalfSessionToken: Option[Vector[Byte]],
      clientPath: String,
      clientMachineIdentifier: ClientMachineIdentifier,
      shouldEstablishRoles: Boolean): EstablishSession = {
    val rolesetMode =
      if (shouldEstablishRoles) RolesetMode.AllRoles
      else if (rolesets.isEmpty) RolesetMode.UseLegacyEntitlements
      else RolesetMode.SpecificRoleset(rolesets.flatten.toSet)
    val csi =
      ClientSessionInfo(
        realId,
        effectiveId,
        rolesetMode,
        establishmentTime,
        onBehalfTokenType,
        onBehalfSessionToken,
        applicationIdentifier,
        clientMachineIdentifier)
    apply(csi, supportedFeatures, context, classpathHash, fullClasspath, pid, slotMapOverrides, clientPath)
  }

  def apply(
      applicationIdentifier: ClientAppIdentifier,
      supportedFeatures: SupportedFeatures,
      context: Context,
      realId: String,
      effectiveId: Option[String],
      rolesetMode: RolesetMode,
      establishmentTime: Option[Instant],
      classpathHash: PathHashT,
      fullClasspath: Option[Seq[ExistingNonStorableJarPath]],
      pid: Option[Int],
      slotMapOverrides: Option[SlotMap],
      onBehalfTokenType: OnBehalfTokenType,
      onBehalfSessionToken: Option[Vector[Byte]],
      clientPath: String,
      clientMachineIdentifier: ClientMachineIdentifier): EstablishSession = {
    val csi = ClientSessionInfo(
      realId,
      effectiveId,
      rolesetMode,
      establishmentTime,
      onBehalfTokenType,
      onBehalfSessionToken,
      applicationIdentifier,
      clientMachineIdentifier)
    apply(
      csi,
      supportedFeatures,
      context,
      classpathHash,
      fullClasspath,
      pid,
      slotMapOverrides,
      clientPath
    )
  }

  def apply(
      applicationIdentifier: ClientAppIdentifier,
      supportedFeatures: SupportedFeatures,
      context: Context,
      realId: String,
      effectiveId: Option[String],
      rolesetMode: RolesetMode,
      establishmentTime: Option[Instant],
      classpathHash: ImmutableArray[Byte],
      fullClasspath: Option[Seq[ExistingNonStorableJarPath]],
      pid: Option[Int],
      slotMapOverrides: Option[SlotMap],
      onBehalfTokenType: OnBehalfTokenType,
      onBehalfSessionToken: Option[Vector[Byte]],
      clientPath: String,
      clientMachineIdentifier: ClientMachineIdentifier): EstablishSession = {
    apply(
      applicationIdentifier,
      supportedFeatures,
      context,
      realId,
      effectiveId,
      rolesetMode,
      establishmentTime,
      PathHashRepr(classpathHash),
      fullClasspath,
      pid,
      slotMapOverrides,
      onBehalfTokenType,
      onBehalfSessionToken,
      clientPath,
      clientMachineIdentifier
    )
  }
}

sealed abstract class EstablishSessionResult

final case class EstablishSessionSuccess(
    applicationIdentifier: ResolvedClientAppIdentifier,
    applicationConfigFound: Boolean,
    sessionId: Int,
    features: SupportedFeatures,
    rolesetMode: RolesetMode.Established,
    establishmentTime: Instant,
    encryptedSessionToken: Option[Vector[Byte]],
    entitlementsToken: EntitlementsToken,
    profileKeyHash: Option[ProfileKeyHash])
    extends EstablishSessionResult

final case class EstablishSessionFailure(failureType: EstablishSessionFailure.Type, message: String)
    extends EstablishSessionResult

object EstablishSessionFailure {
  sealed abstract class Type
  object Type {
    case object Generic extends Type
    case object SessionParametersChanged extends Type
    case object UnregisteredClasspath extends Type
    case object NonMprClasspath extends Type
    case object BrokerProxyRequired extends Type
    case object OnBehalfSessionInvalidProid extends Type
    case object OnBehalfSessionInvalidToken extends Type
  }
}
