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
package optimus.platform.relational.inmemory

import optimus.platform.DynamicFromStaticKey
import optimus.platform.NoKey
import optimus.platform.RelationKey
import optimus.platform.TupleKey
import optimus.platform.relational.tree._
import optimus.platform.relational.tree.QueryMethod._

/**
 * This class is used to optimize distinct operation (with less distinctByKey invocations). Given a series of RT
 * projection fi, as long as the last sequence result S has a key (which is not NoKey), we can just perform one
 * ‘distinct’ against S. Suppose: we have sequence A, we can project it into sequence B via f1, and further project
 * sequence B into sequence C via f2; Both f1 and f2 are referential transparent; Both B and C has a RelationKey that is
 * not NoKey. Instead of invoke ‘distinct’ 2 times for both B and C, we could just do one ‘distinct’ for C.
 */
class MethodKeyOptimizer extends QueryTreeVisitor {
  import MethodKeyOptimizer._

  // 'optimizing' is used to mark if we find 'map_withkey' and start to optimize distinct.
  private var optimizing = false

  override protected def handleQuerySrc(element: ProviderRelation): RelationElement = {
    atInnerMapScope() {
      super.handleQuerySrc(element)
    }
  }

  override protected def handleMethod(method: MethodElement): RelationElement = {
    method.methodCode match {
      case OUTPUT | FLATMAP => optimizeProjection(method)
      case UNTYPE           => optimizeUntype(method)
      case SORT | WHERE | PERMIT_TABLE_SCAN | EXTEND_TYPED | _: Optimizable =>
        if (optimizing) optimizeAsNoKeyMethod(method)
        else super.handleMethod(method)
      case TAKE_DISTINCT => super.handleMethod(method)
      case _ =>
        atInnerMapScope() {
          super.handleMethod(method)
        }
    }
  }

  private def atInnerMapScope()(f: => RelationElement): RelationElement = {
    val savedOptimizing = optimizing
    optimizing = false
    val e = f
    optimizing = savedOptimizing
    e
  }

  private def optimizeUntype(method: MethodElement): RelationElement = {
    if (optimizing) optimizeAsNoKeyMethod(method)
    else if (!shouldDistinctUntype(method.key)) super.handleMethod(method)
    else {
      optimizing = true
      val newMethod = optimizeAsNoKeyMethod(method)
      optimizing = false
      new MethodElement(
        TAKE_DISTINCT_BYKEY,
        List(MethodArg("src", newMethod)),
        method.rowTypeInfo,
        method.key,
        method.pos)
    }
  }

  private def optimizeProjection(method: MethodElement): RelationElement = {
    (optimizing, method.key) match {
      case (_, NoKey) => // Map_with_nokey.
        super.handleMethod(method)

      case (
            false,
            _
          ) => // Reach outer 'map' with a key, start removing unnecessary keys within it in the recognized operators.
        optimizing = true
        val newMethod = optimizeAsNoKeyMethod(method)
        optimizing = false
        new MethodElement(
          TAKE_DISTINCT_BYKEY,
          List(MethodArg("src", newMethod)),
          method.rowTypeInfo,
          method.key,
          method.pos)

      case (true, _) => // Inner map_with_key. Change its key to NoKey
        optimizeAsNoKeyMethod(method)
    }
  }

  private def optimizeAsNoKeyMethod(method: MethodElement): RelationElement = {
    val (src :: others) = method.methodArgs
    val source = visitElement(src.arg)
    new MethodElement(method.methodCode, MethodArg("src", source) :: others, method.rowTypeInfo, NoKey, method.pos)
  }
}

object MethodKeyOptimizer {
  def optimize(e: RelationElement): RelationElement = {
    new MethodKeyOptimizer().visitElement(e)
  }

  private def shouldDistinctUntype(key: RelationKey[_]): Boolean = {
    key match {
      case dk: DynamicFromStaticKey =>
        dk.sk match {
          case tk: TupleKey[_, _, _, _] if tk.k1.fields.size + tk.k2.fields.size > tk.fields.size => true
          case _                                                                                  => false
        }
      case NoKey => false
      case _     => true
    }
  }

  // MethodElement with such MethodCode should only have one src MethodElement
  trait Optimizable extends QueryMethod
}
