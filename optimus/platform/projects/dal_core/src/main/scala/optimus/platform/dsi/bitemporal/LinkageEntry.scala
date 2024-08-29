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

import optimus.platform.{TimeInterval, ValidTimeInterval}
import optimus.platform.storable._

/**
 * Represents a linkage entry.
 *
 * @param parentRef
 *   the entity reference of the container entity, i.e. the Parent
 * @param childRef
 *   the entity reference of the contained entity, i.e. the Child
 * @param parentPropertyName
 *   the name of the stored property on the Parent which carries a collection of Children
 * @param parentTypeName
 *   the type name of the Parent
 * @param parentTypeid
 *   the type id of the Parent's concrete class
 * @param childType
 *   the type name of Child
 * @param vtInterval
 *   the valid time interval
 * @param ttInterval
 *   the transaction time interval
 */
final case class LinkageEntry(
    id: LinkageReference,
    parentRef: EntityReference,
    childRef: EntityReference,
    parentPropertyName: String,
    parentType: String,
    parentTypeId: Option[Int],
    childType: Option[String],
    override val vtInterval: ValidTimeInterval,
    override val txInterval: TimeInterval)
    extends TemporalMatching
