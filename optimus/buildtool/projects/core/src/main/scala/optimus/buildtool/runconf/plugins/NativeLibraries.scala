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
package optimus.buildtool.runconf.plugins

import optimus.buildtool.runconf.FilePath
import optimus.buildtool.runconf.JavaPattern

import scala.collection.immutable.Seq
import scala.collection.compat._

final case class ReorderSpec(shouldBeBefore: FilePath, shouldBeAfter: FilePath)

final case class NativeLibraries(
    includes: Seq[FilePath],
    excludes: Seq[JavaPattern],
    reorder: Seq[ReorderSpec],
    defaults: Seq[FilePath] = Nil) {}

object NativeLibraries {
  def apply(includes: FilePath*): NativeLibraries = this(includes.to(Seq), Nil, Nil, Nil)

  val empty = NativeLibraries(Nil, Nil, Nil)
}
