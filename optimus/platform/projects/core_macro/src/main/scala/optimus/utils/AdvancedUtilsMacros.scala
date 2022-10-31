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
package optimus.utils

import optimus.utils.MacroUtils.splice
import optimus.utils.MacroUtils.typecheckAndValidate

import scala.reflect.macros.blackbox.Context

class AdvancedUtilsMacros(val c: Context) {
  import c.universe._
  final def timedImpl(f: c.Tree): c.Tree = {
    val startTime = c.freshName(TermName("startTime"))
    val result = c.freshName(TermName("result"))

    typecheckAndValidate(c)(q"""
      val $startTime = _root_.optimus.graph.OGTrace.nanoTime()
      val $result = ${splice(c)(f)}
      _root_.optimus.platform.AdvancedUtils.timedValueSink($startTime, $result)
    """)
  }
}
