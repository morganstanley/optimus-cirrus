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
package optimus.platform.relational.data.mapping

import optimus.platform.Query
import optimus.platform.relational.data.Aggregator
import optimus.platform.relational.data.tree.ColumnElement
import optimus.platform.relational.data.tree.DALHeapEntityElement
import optimus.platform.relational.data.tree.DbEntityElement
import optimus.platform.relational.data.tree.DynamicObjectElement
import optimus.platform.relational.data.tree.EmbeddableCaseClassElement
import optimus.platform.relational.data.tree.OuterJoinedElement
import optimus.platform.relational.data.tree.ProjectionElement
import optimus.platform.relational.data.tree.TupleElement
import optimus.platform.relational.tree.ConditionalElement
import optimus.platform.relational.tree.ElementFactory
import optimus.platform.relational.tree.MemberElement
import optimus.platform.relational.tree.NewElement
import optimus.platform.relational.tree.RelationElement
import optimus.platform.relational.tree.RuntimeFieldDescriptor
import optimus.platform.relational.tree.TypeInfo
import optimus.platform.relational.tree.UnaryExpressionElement
import optimus.platform.relational.tree.UnaryExpressionType

import scala.annotation.tailrec

/**
 * provide bind/bindMember API
 */
trait QueryBinder {
  import QueryBinder._

  def bind(mapper: QueryMapper, e: RelationElement): RelationElement

  def bindMember(source: RelationElement, member: MemberInfo): RelationElement = {
    source match {
      case null => null

      case e: DbEntityElement =>
        bindMember(e.element, member) match {
          case MemberElement(inst, m) if (inst eq e.element) && m == member.name =>
            makeMemberAccess(source, member)
          case x => x
        }

      case UnaryExpressionElement(UnaryExpressionType.CONVERT, operand, _) =>
        bindMember(operand, member)

      case e: ProjectionElement if e.viaCollection.isEmpty =>
        // member access on a projection turns into a new projection w/ member access applied
        val newProj = bindMember(e.projector, member)
        new ProjectionElement(
          e.select,
          newProj,
          e.key,
          e.keyPolicy,
          Aggregator.getAggregator(member.memberType, TypeInfo(classOf[Query[_]], member.memberType)),
          entitledOnly = e.entitledOnly
        )

      case e: DALHeapEntityElement =>
        e.memberNames
          .zip(e.members)
          .collectFirst {
            case (name, e) if name == member.name && memberTypeEquals(e.rowTypeInfo, member.memberType) => e
          }
          .getOrElse(makeMemberAccess(source, member))

      case e: EmbeddableCaseClassElement =>
        e.memberNames
          .zip(e.members)
          .collectFirst {
            case (name, e) if name == member.name && memberTypeEquals(e.rowTypeInfo, member.memberType) => e
          }
          .getOrElse(makeMemberAccess(source, member))

      case ConditionalElement(test, ifTrue, ifFalse, _) =>
        ElementFactory.condition(test, bindMember(ifTrue, member), bindMember(ifFalse, member), member.memberType)

      case TupleElement(elements) =>
        elements.zipWithIndex.collectFirst {
          case (e, i) if s"_${i + 1}" == member.name => e
        } getOrElse (makeMemberAccess(source, member))

      case e: NewElement if e.members.isEmpty =>
        val idx = e.projectedType().primaryConstructorParams.map(_._1).indexOf(member.name)
        if (idx >= 0)
          e.arguments(idx)
        else
          makeMemberAccess(source, member)

      case e: NewElement if e.members.nonEmpty =>
        val idx = e.members.map(_.name).indexOf(member.name)
        e.arguments(idx)

      case e: OuterJoinedElement =>
        val ele = bindMember(e.element, member)
        (e.defaultValue, ele) match {
          case (null, c: ColumnElement) => c
          case _ =>
            val default = bindMember(e.defaultValue, member)
            new OuterJoinedElement(e.test, ele, default)
        }

      case _ => makeMemberAccess(source, member)
    }
  }

  def bindUntypedMember(
      source: RelationElement,
      memberName: String,
      lookup: MappingEntityLookup): Option[RelationElement] = {
    source match {
      case DynamicObjectElement(TupleElement(e1 :: e2 :: Nil)) =>
        val candidates = flattenTuple2(List(e2, e1))
        candidates.iterator.map(e => bindUntypedMember(e, memberName, lookup)).collectFirst { case Some(x) => x }

      case DynamicObjectElement(s) =>
        bindUntypedMember(s, memberName, lookup)

      case e: DbEntityElement =>
        bindUntypedMember(e.element, memberName, lookup).map(_ match {
          case m @ MemberElement(inst, name) if (inst eq e.element) && name == memberName =>
            makeMemberAccess(source, MemberInfo(e.rowTypeInfo, memberName, m.rowTypeInfo))
          case x => x
        })

      case e: DALHeapEntityElement =>
        e.memberNames.zip(e.members).collectFirst { case (name, m) if name == memberName => m }

      case UnaryExpressionElement(UnaryExpressionType.CONVERT, operand, _) =>
        bindUntypedMember(operand, memberName, lookup)

      case TupleElement(elements) =>
        elements.zipWithIndex.collectFirst { case (e, i) if s"_${i + 1}" == memberName => e }

      case e: NewElement if e.members.isEmpty =>
        val idx = e.projectedType().primaryConstructorParams.map(_._1).indexOf(memberName)
        if (idx >= 0) Some(e.arguments(idx)) else None

      case e: NewElement if e.members.nonEmpty =>
        val idx = e.members.map(_.name).indexOf(memberName)
        if (idx >= 0) Some(e.arguments(idx)) else None

      case _ => None
    }
  }

  protected def makeMemberAccess(inst: RelationElement, member: MemberInfo): MemberElement = {
    val desc = new RuntimeFieldDescriptor(inst.rowTypeInfo, member.name, member.memberType)
    ElementFactory.makeMemberAccess(inst, desc)
  }

  @tailrec
  final protected def flattenTuple2(
      candidates: List[RelationElement],
      tail: List[RelationElement] = Nil): List[RelationElement] = {
    candidates match {
      case Nil                                     => tail
      case TupleElement(e1 :: e2 :: Nil) :: others => flattenTuple2(e2 :: e1 :: others, tail)
      case h :: others                             => flattenTuple2(others, h :: tail)
    }
  }
}

object QueryBinder {
  def memberTypeEquals(t1: TypeInfo[_], t2: TypeInfo[_]): Boolean = {
    def classEquals(c1: Class[_], c2: Class[_]) = {
      (c1 eq c2) || c1.isAssignableFrom(c2) || c2.isAssignableFrom(c1)
    }
    if (t1 eq t2) true
    else {
      t1.classes.size == t2.classes.size && t1.classes
        .zip(t2.classes)
        .forall(t => classEquals(t._1, t._2)) && t1.typeParams.size == t2.typeParams.size && t1.typeParams
        .zip(t2.typeParams)
        .forall { t =>
          memberTypeEquals(t._1, t._2)
        }
    }
  }
}
