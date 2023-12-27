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
package optimus.exceptions.config

import optimus.scalacompat.collection.ArrayToVarArgsOps

import java.io.File
import java.nio.file.Files
import java.nio.file.Path
import java.nio.file.Paths
import java.util.Properties

import scala.util.Try
import msjava.slf4jutils.scalalog.getLogger

/**
 * RT List Config 1) provides an emergency mechanism for addition & removal of RT exceptions 2) matching based on
 * exception type (fqcn) and message patterns
 *
 * Format of the allow-list: 1) Semi-colon separated matchers (i.e. each matcher matches with one exception, by
 * asserting multiple criterion per matcher) 2) Comma separated criterion (i.e. all criterion in a matcher has an
 * implicit "AND" logic that forms the assertion)
 *
 * ```
 * optimus.additional.rt.exceptions=<operator>:<keyword>,<operator>:<keyword>;...
 * optimus.additional.rt.exception.classes=<exception fqcn>;...
 * optimus.removed.rt.exception.classes=<exception fqcn>;...
 * ```
 *
 * where `<operator>` currently supports: 1) `fqcn`: an exact match of a full-qualified class name 2) `contains`: a
 * sub-string match of the message of an exception 3) `equals`: an exact match of the message of an exception
 *
 * Example:
 *
 * 1) Treat an IllegalStateException with an exact message of "ThisIsRT!" as RT:
 * `optimus.additional.rt.exceptions=fqcn:java.lang.IllegalStateException,equals:ThisIsRT!;`
 *
 * 2) Treat a RuntimeException with a substring message of "is an RT operation" as RT:
 * `optimus.additional.rt.exceptions=fqcn:java.lang.RuntimeException,contains:is an RT operation;`
 */
private[optimus] trait RTListConfigTrait {
  val properties: Properties

  lazy val additionalRTExceptions: Option[String] = Option(properties.getProperty("optimus.additional.rt.exceptions"))
  lazy val additionalRTExceptionClasses: Option[String] = Option(
    properties.getProperty("optimus.additional.rt.exception.classes"))
  lazy val removedRTExceptionClasses: Option[String] = Option(
    properties.getProperty("optimus.removed.rt.exception.classes"))
}

object RTListConfig extends RTListConfigTrait {
  private val log = getLogger(this)

  private val propertyFile: Option[Path] = {
    def convertPathForCurrentOS(path: String): String = {
      val fixedPath = path.replace("/", File.separator).replace("\\", File.separator)

      val isWindows = System.getProperty("os.name").toLowerCase.indexOf("win") >= 0
      def convertUnixPathToWindows(path: String): String = {
        val fixedPath = path.replace("""/""", """\""").replace(File.separator, """\""")

        if (fixedPath.startsWith("""\\""")) // UNC Path, return as-is
          fixedPath
        else if (fixedPath.startsWith("""\""")) // converted from Unix path, prepend backslash to be UNC path
          """\""" + fixedPath
        else // no backslash prefix, return as-is
          fixedPath
      }
      if (isWindows) convertUnixPathToWindows(fixedPath) else fixedPath
    }

    sys.props.get("optimus.rt.allowlist.path") map { pathStr =>
      val path = Paths.get(convertPathForCurrentOS(pathStr))
      log.info(s"property file source: $path")
      path
    }
  }

  val properties: Properties = propertyFile.fold(new Properties) { propertyFile =>
    val props = new Properties
    Try(Files.newInputStream(propertyFile)).map { inStream =>
      Try(props.load(inStream))
      inStream.close()
    }
    log.info(s"loaded properties: ${props.toString}")
    props
  }

  lazy val hasNoAdditions: Boolean = additionalRTExceptions.isEmpty
}

import ExceptionMatcher.MatchingCriteria
private[optimus] final case class ExceptionMatcher(matchingCriterion: MatchingCriteria*) {
  override val toString: String = matchingCriterion.map(_.toString).mkString(",")
}
private[optimus] object ExceptionMatcher {
  sealed trait MatchingCriteria {
    def needle: String
    def matchWith(fqcn: String, msg: String): Boolean
  }

  object MatchingCriteria {
    final case class FQCNMatch(needle: String) extends MatchingCriteria {
      override def matchWith(fqcn: String, msg: String): Boolean = needle equals fqcn
      override val toString: String = s"fqcn:${needle}"
    }
    object MessageMatch {
      final case class EqualsMatch(needle: String) extends MatchingCriteria {
        override def matchWith(fqcn: String, msg: String): Boolean = needle equals msg
        override val toString: String = s"equals:${needle}"
      }
      final case class ContainsMatch(needle: String) extends MatchingCriteria {
        override def matchWith(fqcn: String, msg: String): Boolean = msg contains needle
        override val toString: String = s"contains:${needle}"
      }
    }

    object Parser {
      protected val pattern = """([^\s:]+):(.+)""".r
      def parse(criteria: String): MatchingCriteria = {
        criteria match {
          case pattern("fqcn", needle)     => FQCNMatch(needle.trim)
          case pattern("equals", needle)   => MessageMatch.EqualsMatch(needle.trim)
          case pattern("contains", needle) => MessageMatch.ContainsMatch(needle.trim)
        }
      }
    }
  }

  object Parser {
    def parse(matcher: String): ExceptionMatcher = {
      ExceptionMatcher(matcher.split(',').map(criteria => MatchingCriteria.Parser.parse(criteria)).toVarArgsSeq: _*)
    }
  }
}
