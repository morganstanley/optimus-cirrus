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
package optimus.buildtool.runconf

sealed abstract class Launcher(val name: String) {
  override def toString: String = name
}

object Launcher {
  private val all = Seq(Standard, Grid)
  private val launchers = all.map(l => l.name -> l).toMap

  val names = launchers.keySet

  object Standard extends Launcher("standard")
  object Grid extends Launcher("grid")

  def fromString(s: String): Option[Launcher] = launchers.get(s)
}
