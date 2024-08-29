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
package optimus.platform.storable

import java.time.Instant

import optimus.platform.dal.config.DalAppId
import optimus.platform.dal.config.DalZoneId
import optimus.platform.dal.config.HostPort

final case class SerializedAppEvent(
    id: AppEventReference,
    tt: Instant,
    // The real id from the writing session.
    // See optimus.dsi.security.AuthToken#realId.
    user: String,
    // The effective id from the writing session.
    // See optimus.dsi.security.AuthToken#effectiveId.
    effectiveUser: Option[String],
    application: Int,
    contentOwner: Int,
    reqId: String,
    reqHostPort: HostPort,
    auditDetails: Option[String],
    dalTT: Option[Instant],
    appId: DalAppId,
    zoneId: DalZoneId,
    // The elevated-for user from transaction.
    // See optimus.platform.dal.AppEventBlock#elevatedForUser.
    elevatedForUser: Option[String],
    writerHostPort: Option[HostPort],
    receivedAt: Option[Instant],
    teaIds: Set[String]) {
  effectiveUser.foreach(u => require(u.trim.nonEmpty, "effectiveUser must not be empty string"))
  elevatedForUser.foreach(u => require(u.trim.nonEmpty, "elevatedForUser must not be empty string"))
}

object SerializedAppEvent {
  def apply(
      id: AppEventReference,
      tt: Instant,
      user: String,
      effectiveUser: Option[String],
      application: Int,
      contentOwner: Int,
      reqId: String,
      reqHostPort: HostPort,
      auditDetails: Option[String],
      dalTT: Option[Instant],
      appId: DalAppId,
      zoneId: DalZoneId,
      elevatedForUser: Option[String],
      receivedAt: Option[Instant] = Some(patch.MilliInstant.now()),
      teaIds: Set[String] = Set.empty[String]): SerializedAppEvent = {
    SerializedAppEvent(
      id,
      tt,
      user,
      effectiveUser,
      application,
      contentOwner,
      reqId,
      reqHostPort,
      auditDetails,
      dalTT,
      appId,
      zoneId,
      elevatedForUser,
      None,
      receivedAt,
      teaIds)
  }

  def empty(
      tt: Instant,
      user: String,
      effectiveUser: Option[String],
      application: Int,
      contentOwner: Int,
      reqId: String,
      reqHostPort: HostPort,
      appId: DalAppId,
      zoneId: DalZoneId,
      receivedAt: Option[Instant] = Some(patch.MilliInstant.now()),
      teaIds: Set[String] = Set.empty[String]
  ): SerializedAppEvent = {
    SerializedAppEvent(
      AppEventReference.Nil,
      tt,
      user,
      effectiveUser,
      application,
      contentOwner,
      reqId,
      reqHostPort,
      None,
      None,
      appId,
      zoneId,
      None,
      None,
      receivedAt,
      teaIds)
  }
}
