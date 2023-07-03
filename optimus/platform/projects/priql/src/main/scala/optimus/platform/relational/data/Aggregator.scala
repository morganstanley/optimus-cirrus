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
package optimus.platform.relational.data

import optimus.platform.Query
import optimus.platform.relational.RelationalUnsupportedException
import optimus.platform.relational.tree.ElementFactory
import optimus.platform.relational.tree.LambdaElement
import optimus.platform.relational.tree.RuntimeMethodDescriptor
import optimus.platform.relational.tree.TypeInfo

object Aggregator {
  def getHeadAggregator(elemType: TypeInfo[_]): LambdaElement = {
    getHeadAggregatorWithName(elemType, "head")
  }

  def getHeadOptionAggregator(elemType: TypeInfo[_]): LambdaElement = {
    getHeadAggregatorWithName(elemType, "headOption")
  }

  private def getHeadAggregatorWithName(elemType: TypeInfo[_], methodName: String): LambdaElement = {
    val queryType = TypeInfo(classOf[Query[_]], elemType)
    val p = ElementFactory.parameter("p", queryType)
    val method = new RuntimeMethodDescriptor(queryType, methodName, elemType)
    val body = ElementFactory.call(p, method, Nil)
    ElementFactory.lambda(body, List(p))
  }

  def getAggregator(expectedType: TypeInfo[_], actualType: TypeInfo[_]): LambdaElement = {
    if (!expectedType.clazz.isAssignableFrom(actualType.clazz)) {
      val actualElemType = Query.findShapeType(actualType)
      if (expectedType.clazz.isAssignableFrom(actualElemType.clazz)) {
        getHeadAggregator(actualElemType)
      } else {
        throw new RelationalUnsupportedException(s"Unsupported auto-aggregator: $actualType => $expectedType")
      }
    } else null
  }
}
