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

import com.typesafe.config.ConfigUtil
import optimus.buildtool.config.ModuleId
import optimus.buildtool.config.ParentId
import optimus.buildtool.config.WorkspaceId

import scala.jdk.CollectionConverters._
import scala.collection.immutable.Seq
import scala.collection.compat._

trait ScopedName {
  def id: ParentId
  def name: String
  def elements: Seq[String] = id.elements :+ name
  def properPath: String = id match {
    case WorkspaceId => name
    case _           => s"${id.properPath}.$name"
  }

  override def hashCode(): Int = {
    val state = Seq(id, name)
    state.map(_.hashCode()).foldLeft(0)((a, b) => 31 * a + b)
  }
  override def equals(other: Any): Boolean = other match {
    case that: ScopedName => id == that.id && name == that.name
    case _                => false
  }
  override def toString: String = properPath
}
object ScopedName {
  private class ScopedNameImpl(val id: ParentId, val name: String) extends ScopedName

  def apply(id: ParentId, name: String): ScopedName = new ScopedNameImpl(id, name)

  def parse(str: String): ScopedName = {
    val elements = ConfigUtil.splitPath(str).asScala.to(Seq)
    val id = ParentId.parse(elements.init)
    val name = elements.last
    ScopedName(id, name)
  }
}

class ModuleScopedName private (val id: ModuleId, val name: String) extends ScopedName {
  def withSuffix(suffix: String): ModuleScopedName = ModuleScopedName(id, name + suffix)
}
object ModuleScopedName {
  def apply(id: ModuleId, name: String): ModuleScopedName = new ModuleScopedName(id, name)

  def parse(str: String): ModuleScopedName = {
    val elements = ConfigUtil.splitPath(str).asScala.to(Seq)
    // the first three elements of the path are the meta, bundle and module. The last path elements are the name
    // ("test" or "all_in_bla.x"). We do it this way so that we can support test names that themselves contain "."
    //
    // See OPTIMUS-52913 for details.
    val (moduleId, nameId) = elements.splitAt(3)
    val id = ModuleId.parse(moduleId)
    val name = nameId match {
      case Seq() => throw new RuntimeException(s"$str is not a valid scope id.")
      case seq   => seq.mkString(".")
    }
    ModuleScopedName(id, name)
  }
}

trait HasScopedName {
  def id: ParentId
  def name: String
  final def scopedName: ScopedName = ScopedName(id, name)
}
