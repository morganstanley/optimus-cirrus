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
package optimus.platform

// TODO (OPTIMUS-51110) Remove this aliasing (and instead update imports everywhere in the codebase) once package moves
// are complete
object PackageAliases {
  // during package migrations, you can set -Doptimus.packageAliases=old.package=new.package and then all symbols
  // in the new package will be aliased into the old package, so that downstream code may still use the old package name
  val aliases: List[String] = sys.props.get("optimus.packageAliases").map(_.split(",").toList).getOrElse(Nil)
  val aliasesOldToNew: List[(String, String)] = aliases.map { p =>
    val Array(oldName, newName) = p.split("=")
    (oldName, newName)
  }
  val aliasFlags: List[String] = aliases.map(a => s"-Yalias-package:$a")
}
