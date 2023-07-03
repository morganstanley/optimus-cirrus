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

final case class Closure(parent: Closure, values: Array[AnyRef]) {
  val size: Int = if (parent eq null) values.length else parent.size + values.length

  def resolve(index: Int): AnyRef = {
    require(index < size)

    var c = this
    while ((c.parent ne null) && index < c.parent.size) c = c.parent

    if (c.parent eq null) c.values(index)
    else c.values(index - c.parent.size)
  }
}
