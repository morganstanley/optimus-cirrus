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
import optimus.platform.relational.internal.ComputingCache
import optimus.platform.relational.tree.TypeInfo

private[optimus /*platform*/ ] object ASMKeyFactory {
  import optimus.platform.CompiledKey.NodeOrFunc

  private val cache = new ComputingCache[(TypeInfo[_], Seq[String]), NodeOrFunc]({
    case (s: TypeInfo[_], fields: Seq[String]) => create(s, fields)
  })

  def apply(shapeType: TypeInfo[_], fields: Seq[String]): NodeOrFunc = {
    cache((shapeType, fields))
  }

  private def create(shapeType: TypeInfo[_], fields: Seq[String]): NodeOrFunc = {
    val gen = new ASMKeyGenerator(shapeType, fields)
    val bytes = gen.generateKeyClass()
    val keyClass = ASMGeneratorUtils.loadClass(gen.keyClassName, bytes)
    val factory = keyClass.getDeclaredMethod(ASMGeneratorUtils.FactoryName).invoke(null)
    if (gen.asyncFields.isEmpty) {
      val f = factory.asInstanceOf[java.util.function.Function[Any, AnyRef]]
      Right(f.apply _)
    } else {
      val f = factory.asInstanceOf[java.util.function.Function[Any, Node[AnyRef]]]
      Left(new FuncUtils.LF1(t => Continuation.wrap(() => f(t))))
    }
  }
}
