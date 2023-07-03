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
package optimus.platform.relational.asm

import optimus.graph.Node
import optimus.platform.DynamicObject
import optimus.platform.relational.internal.ComputingCache
import optimus.platform.relational.tree.TypeInfo

private[platform] object ASMDynamicObjectFactory {
  type NodeOrFunc = Either[Any => Node[DynamicObject], Any => DynamicObject]

  private val cache = new ComputingCache[TypeInfo[_], NodeOrFunc]({ case s: TypeInfo[_] =>
    create(s)
  })

  def apply(shapeType: TypeInfo[_]): NodeOrFunc = {
    cache(shapeType)
  }

  private def create(shapeType: TypeInfo[_]): NodeOrFunc = {
    val gen = new ASMDynamicObjectGenerator(shapeType)
    val bytes = gen.generateClass()
    val clazz = ASMGeneratorUtils.loadClass(gen.className, bytes)
    val factory = clazz.getDeclaredMethod(ASMGeneratorUtils.FactoryName).invoke(null)
    if (gen.asyncFields.isEmpty) {
      val f = factory.asInstanceOf[java.util.function.Function[Any, DynamicObject]]
      Right(f.apply _)
    } else {
      val f = factory.asInstanceOf[java.util.function.Function[Any, Node[DynamicObject]]]
      Left(new FuncUtils.LF1(t => Continuation.wrap(() => f(t))))
    }
  }
}
