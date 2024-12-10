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
package optimus.platform.dsi.bitemporal.proto

import optimus.platform.dal.session.RolesetMode
import optimus.platform.dsi.bitemporal.proto.Dsi.RolesetModeProto
import optimus.platform.dsi.bitemporal.proto.Dsi.RolesetProto

import scala.jdk.CollectionConverters._

trait RolesetSerialization extends ProtoSerialization {
  final def toProto(rolesetMode: RolesetMode): RolesetModeProto = ??? /* {
    val builder = RolesetModeProto.newBuilder()
    rolesetMode match {
      case RolesetMode.AllRoles              => builder.setType(RolesetModeProto.Type.ALL_ROLES)
      case RolesetMode.UseLegacyEntitlements => builder.setType(RolesetModeProto.Type.LEGACY_ENTITLEMENTS)
      case RolesetMode.SpecificRoleset(rs) =>
        builder.setType(RolesetModeProto.Type.SPECIFIC_ROLESET).setSpecificRoleset(toProto(rs))
    }
    builder.build()
  } */

  final def fromProto(rolesetModeProto: RolesetModeProto): RolesetMode = ??? /* rolesetModeProto.getType match {
    case RolesetModeProto.Type.ALL_ROLES           => RolesetMode.AllRoles
    case RolesetModeProto.Type.LEGACY_ENTITLEMENTS => RolesetMode.UseLegacyEntitlements
    case RolesetModeProto.Type.SPECIFIC_ROLESET =>
      val roleset: Set[String] = fromProto(rolesetModeProto.getSpecificRoleset)
      RolesetMode.SpecificRoleset(roleset)
  } */

  final def fromProto(proto: RolesetProto): Set[String] = ??? /* {
    proto.getRoleIdList.asScala.toSet
  } */

  final def toProto(roleSet: Set[String]): RolesetProto = ??? /* {
    RolesetProto.newBuilder().addAllRoleId(roleSet.asJava).build()
  } */
}
