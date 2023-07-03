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
package optimus.platform.relational.data.translation

import optimus.platform.NoKey
import optimus.platform.relational.inmemory.MethodKeyOptimizer
import optimus.platform.relational.tree._
import optimus.platform.relational.tree.QueryMethod._

/**
 * This class is used to optimize the last operator to add 'distinct_bykey'. For 'filter/permitTableScan' after
 * 'distinct_bykey', we could put 'distinct_bykey' after it. For 'distinct', we rewrite it to 'distinct_bykey' with
 * NoKey.
 *
 * We assume the tree has been transformed by MethodKeyOptimizer
 */
class DistinctByKeyOptimizer extends QueryTreeVisitor {
  override protected def handleMethod(method: MethodElement): RelationElement = {
    method.methodCode match {
      case WHERE | PERMIT_TABLE_SCAN | _: MethodKeyOptimizer.Optimizable => optimizeDistinctByKey(method)
      case UNTYPE if method.key != NoKey                                 => optimizeDistinctByKey(method)
      case _                                                             => super.handleMethod(method)
    }
  }

  private def optimizeDistinctByKey(method: MethodElement): RelationElement = {
    val (src :: others) = method.methodArgs
    val source = visitElement(src.arg)
    source match {
      case m: MethodElement if m.methodCode == TAKE_DISTINCT_BYKEY =>
        val methodWithNoKey = new MethodElement(
          method.methodCode,
          MethodArg("src", m.methodArgs.head.arg) :: others,
          method.rowTypeInfo,
          NoKey,
          method.pos)
        new MethodElement(
          TAKE_DISTINCT_BYKEY,
          List(MethodArg("src", methodWithNoKey)),
          method.rowTypeInfo,
          method.key,
          method.pos)

      case _ =>
        if (source eq src.arg) method
        else
          new MethodElement(
            method.methodCode,
            MethodArg("src", source) :: others,
            method.rowTypeInfo,
            method.key,
            method.pos)
    }
  }
}

object DistinctByKeyOptimizer {
  def optimize(e: RelationElement): RelationElement = {
    new DistinctByKeyOptimizer().visitElement(e)
  }
}
