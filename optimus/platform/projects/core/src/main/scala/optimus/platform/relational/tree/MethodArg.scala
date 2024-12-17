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
package optimus.platform.relational.tree

class MethodArg[T](val name: String, val arg: T) {
  def param: T = arg
}

object MethodArg {
  def unapply(m: MethodArg[_]) = Some(m.name, m.param)
  def apply[T](name: String, arg: T) = new MethodArg[T](name, arg)
}

object MethodArgConstants {
  val source = "src"
  val function = "f"
  val predicate = "p"
  val left = "left"
  val reduce = "reduce"
  val right = "right"
  val rightDefault = "rightDefault"
  val on = "on"
  val others = "others"
}
