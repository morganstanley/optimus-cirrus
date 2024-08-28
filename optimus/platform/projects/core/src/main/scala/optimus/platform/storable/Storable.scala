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

import optimus.platform.pickling._
import optimus.platform.annotations.excludeDoc

trait Storable {
  type InfoType <: optimus.entity.StorableInfo

  /**
   * This has to be overwritten if custom pickling is required <code>
   * out.writePropertyInfo([EntityTypeName].[PropertyName]) out.write(this.[PropertyName]) </code>
   */
  @excludeDoc def pickle(out: PickledOutputStream): Unit

  def $info: InfoType
  def $permReference: InfoType#PermRefType

  def dal$isTemporary: Boolean

  def toMap: Map[String, Any] = {
    val out = new StorablePropertyMapOutputStream
    out.writeStartObject()
    this.pickle(out)
    out.writeEndObject()
    out.value.asInstanceOf[Map[String, Any]]
  }
}

private[optimus] class StorablePropertyMapOutputStream extends PropertyMapOutputStream(Map.empty) {
  // Contract of this class is that we'll give you the "real" entity referred to.
  override def forceHydration = true
  override def writeEntity(entity: Entity): Unit = writeRawObject(entity)
}
