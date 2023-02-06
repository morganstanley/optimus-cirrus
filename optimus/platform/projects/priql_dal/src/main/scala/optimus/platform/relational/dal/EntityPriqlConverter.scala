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

import optimus.entity.IndexInfo
import optimus.platform._
import optimus.platform.dal.DalAPI
import optimus.platform.Lambda1
import optimus.platform.relational._
import optimus.platform.relational.dal.serialization.DALFrom
import optimus.platform.relational.serialization.Filter
import optimus.platform.relational.tree.ElementFactory
import optimus.platform.relational.tree.MethodPosition
import optimus.platform.relational.tree.RuntimeFieldDescriptor
import optimus.platform.relational.tree.TypeInfo
import optimus.platform.storable.Entity
import optimus.platform.storable.EntityCompanionBase
import optimus.platform.storable.EntityReference
import optimus.platform.storable.EntityReferenceHolder

import scala.util.Try

class EntityPriqlConverter[T, K, U <: Entity](val conv: PriqlConverter[T, K], val companion: EntityCompanionBase[_])(
    implicit dalApi: DalAPI)
    extends PriqlConverter[T, U]
    with PriqlReferenceConverter[T]
    with DALQueryExtensions {
  @node override def convert(t: T): U = {
    companion.info.indexes.find(i => i.unique && !i.indexed) match {
      case Some(info) =>
        val keyInfo = info.asInstanceOf[IndexInfo[U, K]] // to satisfy the type system
        val keyImpl = keyInfo.makeKey(conv.convert(t))
        val result = EvaluationContext.env.entityResolver.findEntity(keyImpl, dalApi.loadContext)
        if (companion.info.runtimeClass.isAssignableFrom(result.getClass)) result
        else
          throw new RelationalException(s"Unable to cast ${result.getClass} to ${companion.info.runtimeClass}")
      case None =>
        throw new RelationalException(s"Unable to find key$$info for entity ${companion.info.runtimeClass}")
    }
  }

  @node override def toReference(t: T): EntityReference = {
    companion.info.indexes.find(i => i.unique && !i.indexed) match {
      case Some(info) =>
        // meta programming to achieve 'from(U).filter(_.[key] == conv.convert(t)).executeReference'
        val pos = MethodPosition.unknown
        val entityType = TypeInfo.javaTypeInfo(companion.info.runtimeClass)
        val from = DALFrom(companion.info.runtimeClass, KeyPropagationPolicy.NoKey, pos)
        val param = ElementFactory.parameter("_", entityType)
        val constant = ElementFactory.constant(conv.convert(t), TypeInfo.ANY)
        val member = new RuntimeFieldDescriptor(entityType, info.name, TypeInfo.ANY)
        val property = ElementFactory.makeMemberAccess(param, member)
        val body = ElementFactory.equal(property, constant)
        val lambda = ElementFactory.lambda(body, Seq(param))
        val predicate = Lambda1[Any, Boolean](Some(_ => ???) /*this won't run*/, None, Some(() => Try(lambda)))
        val query = Filter(from, predicate, pos).build
        query.executeReference.headOption.map(_.asInstanceOf[EntityReferenceHolder[_]].ref).getOrElse {
          throw new RelationalException(s"Unable to find ${companion.info.runtimeClass} by key ${constant.value}")
        }
      case None =>
        throw new RelationalException(s"Unable to find key$$info for entity ${companion.info.runtimeClass}")
    }
  }
}

class EntityReferencePriqlConverter[T, U <: Entity](
    val conv: PriqlConverter[T, EntityReference],
    val companion: EntityCompanionBase[_])(implicit dalApi: DalAPI)
    extends PriqlConverter[T, U]
    with PriqlReferenceConverter[T] {
  @node override def convert(t: T): U = {
    val eref = conv.convert(t)
    val result = EvaluationContext.env.entityResolver.findByReference(eref, dalApi.loadContext)
    if (companion.info.runtimeClass.isAssignableFrom(result.getClass)) result.asInstanceOf[U]
    else
      throw new RelationalException(s"Unable to cast ${result.getClass} to ${companion.info.runtimeClass}")
  }

  @node override def toReference(t: T): EntityReference = {
    conv.convert(t)
  }
}
