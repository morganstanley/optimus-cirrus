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

import msjava.slf4jutils.scalalog.Logger
import optimus.entity._
import optimus.graph.PropertyInfo
import optimus.platform.EvaluationContext
import optimus.platform.TemporalContext
import optimus.platform.annotations.internal.EntityMetaDataAnnotation
import optimus.platform.pickling.ContainsUnresolvedReference

import java.io._
import java.util.concurrent.ConcurrentHashMap

private[optimus /*platform*/ ] final case class ReferenceEntityToken(
    ref: EntityReference,
    loadContext: TemporalContext) {
  @throws(classOf[java.io.ObjectStreamException])
  def readResolve(): AnyRef = {
    EvaluationContext.entityResolver.findByReference(ref, loadContext)
  }
}

/** this is used in the pickled representation of entities (aka "SerializedEntity") */
private[optimus] final case class ModuleEntityToken private (className: String) extends ContainsUnresolvedReference {
  // n.b. we don't implement readResolve because the pickled representation of an entity should retain ModuleEntityToken
  // after java deserialization. This method is called manually during unpickling, NOT as part of deserialization
  lazy val resolve: Entity = {
    val moduleClass = Class.forName(className)
    moduleClass.getField("MODULE$").get(null)
  }.asInstanceOf[Entity]
}

private[optimus] object ModuleEntityToken {
  private val cache = new ConcurrentHashMap[String, ModuleEntityToken]
  def apply(className: String): ModuleEntityToken = {
    cache.computeIfAbsent(className, cls => new ModuleEntityToken(cls))
  }
}

/** this is used in the serialized (i.e. java.io.Serializable) representation of entities */
private[optimus] final case class ModuleEntitySerializationToken(className: String) {
  // readResolve back to the actual entity instance - this is called during deserialization and undoes the
  // writeReplace() of Entity
  def readResolve(): AnyRef = ModuleEntityToken(className).resolve
}

//Usually this annotation is written by the entity plugin for @entities, but we need to create it manually here
@EntityMetaDataAnnotation(isStorable = true, isTrait = true)
trait Entity extends Storable with Serializable {
  type InfoType = EntityInfo

  def log: Logger
  override def $info: InfoType
  override def $permReference: EntityInfo#PermRefType

  def dal$entityRef: EntityReference
  def dal$storageInfo: StorageInfo
  def dal$loadContext: TemporalContext
  def dal$temporalContext: TemporalContext
  private[optimus] def $inline: Boolean

  private[storable] def entityFlavorInternal: EntityFlavor

  // noinspection ScalaUnusedSymbol (ONLY called by the generated uniqueInstance method)
  def mkUnique(): Unit = {}

  protected[optimus] def equalsForCachingInternal(other: Entity): Boolean = equals(other)
  protected[optimus] def hashCodeForCachingInternal: Int = hashCode()

  protected def argsEquals(other: Entity): Boolean = true // Assumes no arguments
  protected def argsHash: Int = 0 // Assumes no arguments
  def writeReplace(): AnyRef = { null: AnyRef }
}

// Inlining is all or nothing at the type level.
@EntityMetaDataAnnotation(isStorable = true, isTrait = true)
trait InlineEntity extends Entity {
  private[optimus] var inlineRoot: Entity = _
  final override def $inline = true
}

@EntityMetaDataAnnotation(isObject = true)
private[optimus] abstract class FakeModuleEntity(properties: Seq[PropertyInfo[_]]) extends EntityImpl {
  override val $info: EntityInfo = new ModuleEntityInfo(FakeModuleEntity.this.getClass, isStorable = false, properties)
  properties.foreach(_.entityInfo = $info)
}

/** Marker trait mostly used for (un)pickling. AdjustAST adds this as a base type of all @embeddable types */
//noinspection ScalaUnusedSymbol
trait Embeddable
