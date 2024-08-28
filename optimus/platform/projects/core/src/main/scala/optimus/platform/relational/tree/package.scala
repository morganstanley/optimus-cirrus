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

import optimus.platform.relational.tree.TypeInfo.SummonMacro

package object tree {
  // must invoke the macro directly here rather than going via implicit resolution, because implicit resolution
  // will resolve "implicit val t: TypeInfo[T] = implicitly[TypeInfo[T]]" to t itself, whereas the macro is smart
  // enough not do do that. Note that the macro does first try an implicit search for TypeInfo[T], so we don't cause
  // any additional code expansion if there was some other TypeInfo[T] already available in implicit scope (but we do
  // avoid resolving back to t itself).
  def typeInfo[T]: TypeInfo[T] = macro SummonMacro.summonMacro[T]
}
