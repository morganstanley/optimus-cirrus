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

final case class SubstitutionRule(category: String, names: Set[String]) {
  def matches(category: String, name: String): Boolean =
    this.category == category && this.names.contains(name)
}

final case class RunConfSubstitutionsValidator(allowList: Seq[SubstitutionRule], ignoreList: Seq[SubstitutionRule]) {
  def isAllowed(category: String, name: String): Boolean =
    allowList.exists(_.matches(category, name))

  def isIgnored(category: String, name: String): Boolean =
    ignoreList.exists(_.matches(category, name))

  def toHashableString: String = {
    // MUST be deterministic to be RT
    def toHashableString(substitution: SubstitutionRule): String =
      s"  ${substitution.category} = [${substitution.names.toSeq.sorted.mkString("\"", "\", \"", "\"")}]"

    s"""allowList = {
       |${allowList.map(toHashableString).mkString(",")}
       |}
       |ignoreList = {
       |${ignoreList.map(toHashableString).mkString(",")}
       |}""".stripMargin
  }
}

object RunConfSubstitutionsValidator {
  object names {
    val allowList = "allowList"
    val ignoreList = "ignoreList"
  }

  val Empty = RunConfSubstitutionsValidator(Seq.empty, Seq.empty)

  def load(loader: ObtFile.Loader): Result[RunConfSubstitutionsValidator] =
    loader(RunConfSubstitutions).flatMap(load)

  def load(conf: Config): Result[RunConfSubstitutionsValidator] = for {
    allowList <- loadSubstitutions(conf, names.allowList)
    ignoreList <- loadSubstitutions(conf, names.ignoreList)
  } yield RunConfSubstitutionsValidator(allowList, ignoreList)

  import ConfigUtils.ConfOps
  private def loadSubstitutions(conf: Config, path: String): Result[Seq[SubstitutionRule]] =
    conf.stringListMapOrEmpty(path, RunConfSubstitutions).map { maps =>
      maps.map { case (category, listOfNames) =>
        SubstitutionRule(category, listOfNames.toSet)
      }.toSeq
    }
}
