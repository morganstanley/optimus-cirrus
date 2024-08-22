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
package optimus.platform.util

import optimus.core.utils.RuntimeMirror
import optimus.exceptions.RTException
import optimus.platform._

import scala.reflect.runtime.universe._

object ReflectionUtils {
  // see optimus.stargazer.utils.ReflectionHelper.reflectValue
  @node def reflectValue(
      instance: Any,
      symbol: Symbol
  ): Any = {
    val r = RuntimeMirror.forClass(instance.getClass).reflect(instance)

    val reflectedValue = NodeTry {
      symbol match {
        case m: MethodSymbol @unchecked => r.reflectMethod(m)()
        case f: TermSymbol @unchecked   =>
          // Will handle field reflection in a specific way due to
          // https://github.com/scala/bug/issues/12096
          // The scala compiler removes any unused fields so reflection
          // fails on them. This is fixed in 2.12 but we're not on that
          // version yet.
          NodeTry {
            r.reflectField(f).get
          } getOrRecover {
            case _: ScalaReflectionException => "[unused]"
            case e @ RTException             => s"Exception when accesing field $f on $instance: ${e.getMessage}"
          }
      }
    } getOrRecover { case e @ RTException =>
      s"Exception when invoking $symbol on $instance: ${e.getMessage}"
    }
    if (reflectedValue == null) "[null]" else reflectedValue
  }

  /**
   * a global lock which should be held during any scala reflect / toolbox operations to work around the global lack of
   * threadsafety in scala reflect API
   */
  private class ScalaReflectLock
  val scalaReflectLock: Object = new ScalaReflectLock
}
