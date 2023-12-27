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

object IncludeExcludeConverter {
  private val acceptAll = ".*"

  def obtToIntellij(includes: Seq[String], excludes: Seq[String]): String = {
    def acceptIncludes = acceptAllRegex(includes)

    def acceptExcludes = acceptAllRegex(excludes)

    if (includes.isEmpty && excludes.isEmpty) {
      acceptAll
    } else if (includes.nonEmpty && excludes.isEmpty) {
      acceptIncludes
    } else if (includes.isEmpty && excludes.nonEmpty) {
      s"(?!$acceptExcludes)$acceptAll"
    } else {
      s"(?!$acceptExcludes)($acceptIncludes)"
    }
  }

  def acceptAllRegex(gradlePatterns: Seq[String]): String = {
    gradlePatterns.map(toRegex).mkString("|")
  }

  def toRegex(gradlePattern: String): String = {
    val withoutExtension = dropExtension(gradlePattern)
    val classFQNRegex = filePathToFQNRegex(withoutExtension)
    gradleWildcardsToJavaRegexWildcards(classFQNRegex)
  }

  // dot means extension (.class in case of classes) that is not present in class name
  // that final pattern is meant to match. Removing everything after . also handles
  // cases like ClassName.java, ClassName.scala, ClassName.*, or ClassName.cl*
  private def dropExtension(str: String) = {
    str.takeWhile(_ != '.')
  }

  private def filePathToFQNRegex(str: String) = {
    str.replace("/", "\\.")
  }

  private def gradleWildcardsToJavaRegexWildcards(str: String) = {
    val starPlaceholder = "$star$"
    str
      .replace("*", starPlaceholder)
      .replace(starPlaceholder * 2, acceptAll)
      .replace(starPlaceholder, "[^\\.]*")
  }
}
