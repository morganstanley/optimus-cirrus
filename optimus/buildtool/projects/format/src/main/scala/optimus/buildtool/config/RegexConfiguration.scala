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

import optimus.buildtool.artifacts.Severity
import optimus.buildtool.utils.Hashing

import scala.collection.immutable.Seq
import scala.util.matching.Regex

private[buildtool] final case class RegexConfiguration(
    rules: Seq[CodeFlaggingRule]
)

private[buildtool] object RegexConfiguration {
  val Empty: RegexConfiguration = RegexConfiguration(rules = Nil)
}

private[buildtool] final case class Pattern(reStr: String, exclude: Boolean = false, message: Option[String] = None)
    extends Pattern.Fields

object Pattern {

  sealed abstract class Fields { this: Pattern =>
    // hack for spray-json which can't deal with extra non-ctor fields
    // doesn't participate in equality since it's not a ctor param
    val regex: Regex = reStr.r
  }
}

private[buildtool] final case class CodeFlaggingRule private (
    key: String,
    title: String,
    description: String,
    filePatterns: Seq[String],
    filter: Option[Filter],
    severityLevel: Severity,
    regexes: Seq[Pattern],
    isNew: Boolean,
    upToLine: Option[Int]
) extends CodeFlaggingRule.Fields {
  @transient val (excludes, includes) = regexes.partition(_.exclude)

}

object CodeFlaggingRule {

  def fingerprint(rule: CodeFlaggingRule): String = rule.key + "@" + Hashing.hashStrings {
    import rule._
    ((List(title, description) ++ filePatterns ++ filter :+ severityLevel) ++ regexes.flatMap(_.productIterator))
      .map(_.toString)
  }

  sealed abstract class Fields {
    this: CodeFlaggingRule => // hack for spray-json which can't deal with extra non-ctor fields
    val fileRegexen: Seq[Regex] = filePatterns.map(_.r)
    def matchesFile(fileName: String): Boolean = fileRegexen.exists(_.findFirstIn(fileName).isDefined)
    val fingerprint: String = CodeFlaggingRule.fingerprint(this)
  }
}
