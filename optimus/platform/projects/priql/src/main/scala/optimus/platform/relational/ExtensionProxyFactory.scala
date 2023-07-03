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
package optimus.platform.relational

import java.util.function.BiFunction

import optimus.platform.relational.asm.ASMGeneratorUtils
import optimus.platform.relational.asm.ASMProxyGenerator
import optimus.platform.relational.internal.ComputingCache
import optimus.platform.relational.tree.TypeInfo

class ExtensionProxyFactory private (f: (AnyRef, AnyRef) => AnyRef, exchange: Boolean) {
  def proxy(item: AnyRef, ext: AnyRef): AnyRef = {
    if (exchange) f(ext, item) else f(item, ext)
  }
}

object ExtensionProxyFactory {
  def apply[L, R](main: TypeInfo[L], ext: TypeInfo[R], favorExt: Boolean = false): ExtensionProxyFactory = {
    if (favorExt)
      new ExtensionProxyFactory(cache(Key(ext, main)), true)
    else
      new ExtensionProxyFactory(cache(Key(main, ext)), false)
  }

  private final case class Key(main: TypeInfo[_], ext: TypeInfo[_])

  private val cache = new ComputingCache[Key, (AnyRef, AnyRef) => AnyRef]({ key =>
    if (key.main == key.ext) { (x, y) =>
      x
    } else {
      val factory =
        try {
          val gen = new ASMProxyGenerator(key.main, key.ext)
          val bytes = gen.generateProxyClass()
          val clazz = ASMGeneratorUtils.loadClass(gen.className, bytes)
          clazz
            .getDeclaredMethod(ASMGeneratorUtils.FactoryName)
            .invoke(null)
            .asInstanceOf[BiFunction[AnyRef, AnyRef, AnyRef]]
        } catch {
          case t: Throwable =>
            throw new RelationalException(s"Error generating clazz for $key", t)
        }
      (x, y) => factory.apply(x, y)
    }
  })
}
