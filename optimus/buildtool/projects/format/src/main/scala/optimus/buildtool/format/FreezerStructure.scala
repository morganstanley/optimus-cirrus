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
package optimus.buildtool.format

import com.typesafe.config.Config
import optimus.buildtool.config.PartialScopeId
import optimus.buildtool.config.RelaxedScopeIdString
import optimus.buildtool.config.ScopeId
import optimus.buildtool.format.ConfigUtils._

final case class FreezerStructure(active: Boolean, save: Boolean, compile: Seq[PartialScopeId]) {
  import FreezerStructure._

  private[buildtool] def fpStrings: Seq[String] =
    compile.map(_.toString).sorted

  def state(s: ScopeId): CompilationState = {
    if (compile.exists(p => p.contains(s))) {
      Compile
    } else {
      Freeze
    }
  }

}

object FreezerStructure {
  sealed trait CompilationState
  case object Compile extends CompilationState
  case object Freeze extends CompilationState
  private val origin = MischiefConfig
  private val freezerPath = "freezer"
  val active = "active"
  val save = "save"
  val compile = "compile"

  /*
  Defaults:
  No freezer group given is equivalent to
    freezer {
      active = false // don't freeze anything
      save = true
      compile = []
    }

   Having a freezer group present (but empty) is equivalent to
     freezer {
       active = true // don't compile anything
       save = true
       compile = []
     }
   */
  val NoFreezerGroup: FreezerStructure = FreezerStructure(false, true, Seq.empty)
  val HasFreezerGroup: FreezerStructure = FreezerStructure(true, true, Seq.empty)

  def load(loader: ObtFile.Loader): Result[FreezerStructure] =
    loader(origin).flatMap(load)

  def load(config: Config): Result[FreezerStructure] = {
    Result
      .tryWith(origin, config) {
        if (config.hasPath(freezerPath)) {
          val freezerConf = config.getConfig(freezerPath)
          Success {
            FreezerStructure(
              freezerConf.booleanOrDefault(active, HasFreezerGroup.active),
              freezerConf.booleanOrDefault(save, HasFreezerGroup.save),
              compile = freezerConf
                .optionalStringList(compile)
                .map(_.filterNot(_.isBlank).map(RelaxedScopeIdString.asPartial))
                .getOrElse(HasFreezerGroup.compile)
            )
          }
            .withProblems(freezerConf.checkExtraProperties(origin, Keys.freezer))
        } else Success(NoFreezerGroup)
      }
  }
}
