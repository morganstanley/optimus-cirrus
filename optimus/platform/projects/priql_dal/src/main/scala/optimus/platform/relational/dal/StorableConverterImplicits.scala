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
package optimus.platform.relational.dal

import optimus.entity.EntityInfoRegistry
import optimus.platform.annotations.noDalPriql
import optimus.platform.dal.DalAPI
import optimus.platform.internal.OptimusReporter
import optimus.platform.BusinessEvent
import optimus.platform.Query
import optimus.platform.QueryConverter
import optimus.platform.QueryProvider
import optimus.platform.RelationKey
import optimus.platform.relational.KeyPropagationPolicy
import optimus.platform.relational.PriqlConverter
import optimus.platform.relational.tree.MethodPosition
import optimus.platform.relational.tree.TypeInfo
import optimus.platform.storable.Entity
import optimus.platform.storable.EntityCompanionBase
import optimus.platform.storable.EventCompanionBase
import optimus.tools.scalacplugins.entity.reporter.RelationalAlarms

import scala.reflect.macros.blackbox.Context

trait StorableConverterImplicits { self: DalAPI =>
  import StorableConverterImplicits._
  import EntityPriqlConverterMacros._

  implicit def entityPriqlConverter[T, U <: Entity]: PriqlConverter[T, U] = macro entityPriqlConverterImpl[T, U]

  implicit def entityConverter[E <: Entity]: QueryConverter[E, EntityCompanionBase] = macro entityConvImpl[E]
  implicit def entityClassConverter[E <: Entity]: QueryConverter[E, Class] = macro entityClassConvImpl[E]
  implicit def eventConverter[E <: BusinessEvent]: QueryConverter[E, EventCompanionBase] = macro eventConvImpl[E]
  implicit def eventClassConverter[E <: BusinessEvent]: QueryConverter[E, Class] = macro eventClassConvImpl[E]

  def mkEntityConverter[E <: Entity]: QueryConverter[E, EntityCompanionBase] = new EntityConverter[E]
  private class EntityConverter[E <: Entity] extends QueryConverter[E, EntityCompanionBase] {
    def convert[T <: E](src: EntityCompanionBase[T], key: RelationKey[T], p: QueryProvider)(implicit
        itemType: TypeInfo[T],
        pos: MethodPosition): Query[T] = {
      val keyPolicy = p match {
        case x: KeyPropagationPolicy => x
        case _                       => KeyPropagationPolicy.NoKey
      }
      val e = DALProvider[T](src.info, key, pos, self, keyPolicy)
      p.createQuery(e)
    }
  }

  def mkEntityClassConverter[E <: Entity]: QueryConverter[E, Class] = new EntityClassConverter[E]
  private class EntityClassConverter[E <: Entity] extends QueryConverter[E, Class] {
    def convert[T <: E](src: Class[T], key: RelationKey[T], p: QueryProvider)(implicit
        itemType: TypeInfo[T],
        pos: MethodPosition): Query[T] = {
      val entity = EntityInfoRegistry.getCompanion(src).asInstanceOf[EntityCompanionBase[T]]
      val keyPolicy = p match {
        case x: KeyPropagationPolicy => x
        case _                       => KeyPropagationPolicy.NoKey
      }
      val e = DALProvider[T](entity.info, key, pos, self, keyPolicy)
      p.createQuery(e)
    }
  }

  def mkEventConverter[E <: BusinessEvent]: QueryConverter[E, EventCompanionBase] = new EventConverter[E]
  private class EventConverter[E <: BusinessEvent] extends QueryConverter[E, EventCompanionBase] {
    def convert[T <: E](src: EventCompanionBase[T], key: RelationKey[T], p: QueryProvider)(implicit
        itemType: TypeInfo[T],
        pos: MethodPosition): Query[T] = {
      val e = EventMultiRelation[T](src.info, key, pos)(itemType, self)
      p.createQuery(e)
    }
  }

  def mkEventClassConverter[E <: BusinessEvent]: QueryConverter[E, Class] = new EventClassConverter[E]
  private class EventClassConverter[E <: BusinessEvent] extends QueryConverter[E, Class] {
    def convert[T <: E](src: Class[T], key: RelationKey[T], p: QueryProvider)(implicit
        itemType: TypeInfo[T],
        pos: MethodPosition): Query[T] = {

      val event = EntityInfoRegistry.getCompanion(src).asInstanceOf[EventCompanionBase[T]]

      val e = EventMultiRelation[T](event.info, key, pos)(itemType, self)
      p.createQuery(e)
    }
  }

}

object StorableConverterImplicits {
  def entityConvImpl[T <: Entity: c.WeakTypeTag](
      c: Context { type PrefixType = StorableConverterImplicits }): c.Expr[QueryConverter[T, EntityCompanionBase]] = {
    import c.universe._
    val sym = symbolOf[T]
    if (sym.annotations.exists(an => an.tree.tpe =:= typeOf[noDalPriql]))
      OptimusReporter.abort(c, RelationalAlarms.NOPRIQL_ERROR)(c.enclosingPosition, sym, sym.name)
    reify { c.prefix.splice.mkEntityConverter[T] }
  }

  def entityClassConvImpl[T <: Entity: c.WeakTypeTag](
      c: Context { type PrefixType = StorableConverterImplicits }): c.Expr[QueryConverter[T, Class]] = {
    import c.universe._
    val sym = symbolOf[T]
    if (sym.annotations.exists(an => an.tree.tpe =:= typeOf[noDalPriql]))
      OptimusReporter.abort(c, RelationalAlarms.NOPRIQL_ERROR)(c.enclosingPosition, sym, sym.name)
    reify { c.prefix.splice.mkEntityClassConverter[T] }
  }

  def eventConvImpl[T <: BusinessEvent: c.WeakTypeTag](
      c: Context { type PrefixType = StorableConverterImplicits }): c.Expr[QueryConverter[T, EventCompanionBase]] = {
    import c.universe._
    val sym = symbolOf[T]
    if (sym.annotations.exists(an => an.tree.tpe =:= typeOf[noDalPriql]))
      OptimusReporter.abort(c, RelationalAlarms.NOPRIQL_ERROR)(c.enclosingPosition, sym, sym.name)
    reify { c.prefix.splice.mkEventConverter[T] }
  }

  def eventClassConvImpl[T <: BusinessEvent: c.WeakTypeTag](
      c: Context { type PrefixType = StorableConverterImplicits }): c.Expr[QueryConverter[T, Class]] = {
    import c.universe._
    val sym = symbolOf[T]
    if (sym.annotations.exists(an => an.tree.tpe =:= typeOf[noDalPriql]))
      OptimusReporter.abort(c, RelationalAlarms.NOPRIQL_ERROR)(c.enclosingPosition, sym, sym.name)
    reify { c.prefix.splice.mkEventClassConverter[T] }
  }

}
