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

import optimus.platform.relational.execution.NonRepetitiveMethodTreeVisitor
import optimus.platform.relational.tree._

/**
 * Optimize extendTypeValue and replaceValue, group adjacent method element into one group
 */
class ExtensionOptimizer extends NonRepetitiveMethodTreeVisitor {

  protected override def handleMethod(method: MethodElement): RelationElement = {
    method.methodCode match {
      case QueryMethod.EXTEND_TYPED | QueryMethod.REPLACE =>
        val methodList = collect(
          method,
          method.methodCode,
          if (method.methodCode == QueryMethod.EXTEND_TYPED) "extVal" else "repVal").reverse
        if (methodList.size <= 1) super.handleMethod(method)
        else {
          val s = visitElement(methodList.head.methodArgs.head.param)
          method.replaceArgs(MethodArg("src", s) :: methodList.map(m => m.methodArgs(1)))
        }
      case _ => super.handleMethod(method)
    }
  }

  protected def collect(source: RelationElement, methodCode: QueryMethod, paramName: String): List[MethodElement] = {
    source match {
      case m @ MethodElement(`methodCode`, (src :: lamOrVal :: Nil), _, _, _) if (lamOrVal.name == paramName) =>
        m :: collect(src.param, methodCode, paramName)
      case _ => Nil
    }
  }
}

object ExtensionOptimizer {
  def optimize(e: RelationElement): RelationElement = {
    new ExtensionOptimizer().visitElement(e)
  }
}
