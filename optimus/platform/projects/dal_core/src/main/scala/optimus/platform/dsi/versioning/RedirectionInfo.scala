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
package optimus.platform.dsi.versioning

import optimus.platform.storable.SerializedEntity

final case class VersioningRedirectionInfo(slotRedirections: Map[SerializedEntity.TypeRef, Set[SlotRedirectionInfo]]) {
  override def toString(): String = s"VersioningRedirectionInfo(${slotRedirections.mkString(", ")}"
  def classpathHashes: Set[PathHashT] = slotRedirections.values.flatMap(_.flatMap(_.classpathHashes)).toSet
  def classNames: Set[SerializedEntity.TypeRef] = slotRedirections.keySet
  def impacts(className: SerializedEntity.TypeRef): Boolean = slotRedirections.isDefinedAt(className)
}

final case class SlotRedirectionInfo(
    // The source of this redirection is a specific slot
    className: SerializedEntity.TypeRef,
    // The destination is some set of target slots, combined with the classpath hash we can use to get there. This is the
    // last step in the path
    destinationSlots: (PathHashT, SlotMetadata.Key, Set[SlotMetadata.Key]),
    // If there are any intermediate steps along the path, we must only need a single slot at any given one of them
    intermediateSlots: Seq[(PathHashT, SlotMetadata.Key, SlotMetadata.Key)]
) {
  def classpathHashes: Set[PathHashT] = intermediateSlots.map(_._1).toSet + destinationSlots._1
  def sourceSlot: SlotMetadata.Key = intermediateSlots.headOption.map(_._2).getOrElse(destinationSlots._2)
  override def toString(): String = {
    val intermediateString = if (intermediateSlots.nonEmpty) s" via ${intermediateSlots.length} other(s)" else ""
    s"SlotRedirectionInfo($sourceSlot -> $destinationSlots$intermediateString)"
  }
}
