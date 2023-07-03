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
package optimus.platform

import java.time.ZonedDateTime

import optimus.core.needsPlugin
import optimus.graph.Node
import optimus.platform._
import optimus.platform.annotations._
import optimus.platform.relational.data.translation.ElementReplacer
import optimus.platform.relational.tree._
import optimus.platform.storable.Entity

import optimus.scalacompat.collection._
import scala.collection.generic.CanBuildFrom

package object relational extends QueryApi {

  implicit class RichZonedDateTime(zdt: ZonedDateTime) {
    // TODO (OPTIMUS-26009): Rename this to "equalInstant".
    def richEqualInstant(otherZdt: ZonedDateTime): Boolean = zdt.toInstant == otherZdt.toInstant
  }

  def legacyFromWithKey[T, F[_ <: T]](src: F[T], key: RelationKey[T])(implicit
      conv: QueryConverter[T, F],
      itemType: TypeInfo[T],
      pos: MethodPosition): Query[T] = {
    fromWithKey[T, F](src, key, KeyPropagationPolicy.Legacy)(conv, itemType, pos)
  }

  def legacyFrom[T, F[_ <: T]](
      src: F[T])(implicit conv: QueryConverter[T, F], itemType: TypeInfo[T], pos: MethodPosition): Query[T] = {
    from[T, F](src, KeyPropagationPolicy.Legacy)(conv, itemType, pos)
  }

  /**
   * this method is used to define custom aggregation rule to some fields in trait T using an anonymous class format.
   * this aggregation is T -> T mapping, so if there are fields which does not appear in the anonymous class definition,
   * priql will use default aggregation rule to these fields.
   *
   * trait Book { def name: String def price: Float } (q: Query[Book]) => new { def price = q.avg(_.price) }
   */
  def customAggregate[T](f: Query[T] => AnyRef): NodeFunction1[Query[T], T] = macro QueryMacros.customAggregate[T]

  /**
   * this method is used to define aggregation rules to all fields in U
   */
  @nodeSyncLift
  @nodeLiftByName
  def aggregate[T, U](@nodeLift @withNodeClassID f: Query[T] => U): NodeFunction1[Query[T], U] = needsPlugin
  def aggregate$withNode[T, U](vn: Query[T] => Node[U]): NodeFunction1[Query[T], U] = asNode.apply$withNode(vn)

  /**
   * Produce LambdaElement which represents the given scala lambda: f. It will always partial-evaluate the constant
   * sub-tree (e.g. 1+1 will become 2 in the result tree).
   *
   * If the lambda is too complex, this method returns None
   */
  def reifyLambda[T](f: T): Option[LambdaElement] = macro QueryMacros.reifyLambda[T]

  // It should not extend AnyVal, otherwise LambdaCompiler won't compile it correctly.
  // Scala will generate ofType$extension for implicit class derived from AnyVal.
  implicit class TraversableLikeOps[A <: AnyRef, Repr[T] <: TraversableLike[T, Repr[T]]](xs: Repr[A]) {
    def ofType[B <: A](implicit itemType: TypeInfo[B], cbf: CanBuildFrom[Repr[A], B, Repr[B]]) = {
      val klass = itemType.clazz
      val b = cbf.newBuilder(xs)
      for (x <- xs if klass.isInstance(x)) {
        b += x.asInstanceOf[B]
      }
      b.result()
    }
  }

  implicit class ExtendedEqualOps[U](val e: U) {
    @node def =~=[T](other: T)(implicit conv: PriqlConverter[T, U]): Boolean = {
      (e, conv) match {
        case (ent: Entity, erefConv: PriqlReferenceConverter[T] @unchecked) if !ent.dal$isTemporary =>
          ent.dal$entityRef == erefConv.toReference(other)
        case _ =>
          e == conv.convert(other)
      }
    }
  }

  implicit def tuple2Conveter[T1, T2, U1, U2](implicit
      conv1: PriqlConverter[T1, U1],
      conv2: PriqlConverter[T2, U2]): PriqlConverter[(T1, T2), (U1, U2)] = new PriqlConverter[(T1, T2), (U1, U2)] {
    @node def convert(t: (T1, T2)): (U1, U2) = (conv1.convert(t._1), conv2.convert(t._2))
  }

  implicit def tuple3Conveter[T1, T2, T3, U1, U2, U3](implicit
      conv1: PriqlConverter[T1, U1],
      conv2: PriqlConverter[T2, U2],
      conv3: PriqlConverter[T3, U3]): PriqlConverter[(T1, T2, T3), (U1, U2, U3)] =
    new PriqlConverter[(T1, T2, T3), (U1, U2, U3)] {
      @node def convert(t: (T1, T2, T3)): (U1, U2, U3) = (conv1.convert(t._1), conv2.convert(t._2), conv3.convert(t._3))
    }

  implicit class ScalaLambdaCalleeOps[T1, T2](val l: ScalaLambdaCallee[T1, T2]) extends AnyVal {
    def lambdaElementEvaluated: Option[LambdaElement] = l.lambdaElement.map(evaluate)
  }

  implicit class ScalaLambdaCallee2Ops[T1, T2, T3](val l: ScalaLambdaCallee2[T1, T2, T3]) extends AnyVal {
    def lambdaElementEvaluated: Option[LambdaElement] = l.lambdaElement.map(evaluate)
  }

  // this is to reserve the old behavior that we evaluate the async code block along with the RelationElement tree
  // construction
  @entersGraph private[optimus /*relational*/ ] def evaluate[T <: RelationElement](l: T): T = {
    import AsyncImplicits._

    val asyncConstants = ElementLocator.locate(
      l,
      _ match {
        case ConstValueElement(_: AsyncValueHolder[_], _) => true
        case _                                            => false
      })
    if (asyncConstants.isEmpty) l
    else {
      val evaluated = asyncConstants.apar.map { case ConstValueElement(asyncValue: AsyncValueHolder[_], typeInfo) =>
        ElementFactory.constant(asyncValue.evaluate, typeInfo)
      }
      ElementReplacer.replace(l, asyncConstants, evaluated).asInstanceOf[T]
    }
  }
}
