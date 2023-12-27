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
package optimus.buildtool.config

import scala.collection.mutable

sealed trait ArchiveType

object ArchiveType {
  private val parseMap = new mutable.HashMap[String, ArchiveType]
  private def add(a: BaseArchiveType): Unit = {
    parseMap += (a.name -> a)
  }
  def parse(name: String): Option[ArchiveType] = parseMap.get(name)
  lazy val known: Seq[ArchiveType] = parseMap.values.toSeq

  private[config] sealed abstract class BaseArchiveType(val name: String) extends ArchiveType

  case object War extends BaseArchiveType("war")
  add(War)
  case object IntellijPlugin extends BaseArchiveType("intellij-plugin")
  add(IntellijPlugin)
}
