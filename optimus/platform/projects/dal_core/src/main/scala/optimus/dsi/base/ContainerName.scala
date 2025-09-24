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
package optimus.dsi.base

object ContainerName {
  def apply(name: String): ContainerName = name match {
    case KeyContainerName.name              => KeyContainerName
    case UnqIdxGroupingsContainerName.name  => UnqIdxGroupingsContainerName
    case UnqIdxTimeslicesContainerName.name => UnqIdxTimeslicesContainerName
    case GroupingsContainerName.name        => GroupingsContainerName
    case TimesliceContainerName.name        => TimesliceContainerName
    case IndexContainerName.name            => IndexContainerName
    case RegisteredIndexContainerName.name  => RegisteredIndexContainerName
    case ErefIndexContainerName.name        => ErefIndexContainerName
    case EventIndexContainerName.name       => EventIndexContainerName
    case LinkageContainerName.name          => LinkageContainerName
    case AppEventContainerName.name         => AppEventContainerName
    case EffectsContainerName.name          => EffectsContainerName
    case DbRawEffectsContainerName.name     => DbRawEffectsContainerName
    case TombstoneContainerName.name        => TombstoneContainerName
    case BlobTombstoneContainerName.name    => BlobTombstoneContainerName
    case BeventGroupingsContainerName.name  => BeventGroupingsContainerName
    case BeventTimesliceContainerName.name  => BeventTimesliceContainerName
    case BeventKeyContainerName.name        => BeventKeyContainerName
    case BEventPayloadContainerName.name    => BEventPayloadContainerName
    case EntitiesContainerName.name         => EntitiesContainerName
    case unknown                            => UnknownContainerName(unknown)
  }
}
sealed trait ContainerName {
  def name: String
  override def toString: String = name
}
case object KeyContainerName extends ContainerName { val name = "keys" }
case object UnqIdxGroupingsContainerName extends ContainerName { val name = "unqidxgroupings" }
case object UnqIdxTimeslicesContainerName extends ContainerName { val name = "unqidxtimeslices" }
case object GroupingsContainerName extends ContainerName { val name = "groupings" }
case object TimesliceContainerName extends ContainerName { val name = "timeslices" }
case object IndexContainerName extends ContainerName { val name = "index" }
case object RegisteredIndexContainerName extends ContainerName { val name = "regindex" }
case object ErefIndexContainerName extends ContainerName { val name = "erefindex" }
case object EventIndexContainerName extends ContainerName { val name = "eventindex" }
case object LinkageContainerName extends ContainerName { val name = "linkages" }
case object AppEventContainerName extends ContainerName { val name = "appevent" }
case object EffectsContainerName extends ContainerName { val name = "effects" }
case object DbRawEffectsContainerName extends ContainerName { val name = "dbraweffects" }
case object TombstoneContainerName extends ContainerName { val name = "tombstones" }
case object BlobTombstoneContainerName extends ContainerName { val name = "blobtombstones" }
case object BeventGroupingsContainerName extends ContainerName { val name = "beventgroupings" }
case object BeventTimesliceContainerName extends ContainerName { val name = "beventtimeslices" }
case object BeventKeyContainerName extends ContainerName { val name = "beventkeys" }
case object BEventPayloadContainerName extends ContainerName { val name = "beventpayload" }
case object EntitiesContainerName extends ContainerName { val name = "entities" }
case object ClassInfoContainerName extends ContainerName { val name = "classinfo" }
final case class UnknownContainerName(name: String) extends ContainerName
