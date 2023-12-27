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

import scala.jdk.CollectionConverters._
import com.typesafe.config.Config
import com.typesafe.config.ConfigList
import com.typesafe.config.ConfigObject
import com.typesafe.config.ConfigValue

import scala.util.matching.Regex

// TODO (OPTIMUS-30165): Make ObtFileFormatter production ready since for now it it used in migration script
object ObtFileFormatter {
  val IdStringValue: Regex = """([\w._\-"]+)""".r
}

trait ObtFileFormatterSettings {

  private def renderKey(k: String) = if (k.forall(c => c.isLetter || c == '_' || c == '-')) k else s""""$k""""

  final def values(configObject: ConfigObject, path: Vector[String]): Seq[(String, ConfigValue)] = {
    if (keysMapping.isDefinedAt(path)) {
      val keys = keysMapping(path)
      val (explicitlyOrdered, rest) = configObject.asScala.partition { case (k, _) => keys.all(k) }
      (for { key <- keys.order; v <- explicitlyOrdered.get(key) } yield (key, v)) ++ rest.toSeq.sortBy(_._1)
    } else configObject.asScala.toSeq
  }.map { case (k, v) => renderKey(k) -> v }

  /**
   * Keys are order in following way: first keys present in KeySet with predefine order and then rest sorted by name. If
   * no KeySet is provided keys will be returned as defined in config
   */
  val keysMapping: PartialFunction[Vector[String], Keys.KeySet]

  def inline(path: Vector[String]): Boolean = false
  def blankLineAfter(path: Vector[String]): Boolean = false
}

object ModuleFormatterSettings extends ObtFileFormatterSettings {
  override def blankLineAfter(path: Vector[String]): Boolean = path.length == 1

  override val keysMapping: PartialFunction[Vector[String], Keys.KeySet] = {
    case Vector() =>
      Keys.projectFile
    case Vector(_) =>
      Keys.scopeDefinition
    case Vector(_, "compile" | "runtime") =>
      Keys.scopeDeps
    case Vector(_, "scalac") =>
      Keys.scalac
    case _ =>
      Keys.empty
  }
}

object BundlesFormatterSettings extends ObtFileFormatterSettings {

  override val keysMapping: PartialFunction[Vector[String], Keys.KeySet] = {
    case Vector(_, _)    => Keys.bundleDefinition
    case Vector(_, _, _) => Keys.moduleOwnership
    case _               => Keys.empty
  }
  override def inline(path: Vector[String]): Boolean = path.length == 3

  override def blankLineAfter(path: Vector[String]): Boolean = path match {
    case Vector(_, _)                => true
    case Vector(_, _, "modulesRoot") => true
    case _                           => false
  }
}

final case class ObtFileFormatter(identString: String, settings: ObtFileFormatterSettings) {

  def formatConfig(config: Config): String = new Formatter().format(config.root())
  private val CommentRegex = """\/\/(\d+) """.r

  class Formatter {
    var reportedComment = Vector[(Vector[String], String)]()
    def format(config: ConfigObject): String = {
      val lines = formatValue(config, "", 0, Vector()).linesIterator // empty line...
      lines
        .flatMap { line =>
          val commentsLink = CommentRegex.findAllIn(line)
          if (commentsLink.isEmpty) Seq(line)
          else {
            val commentsFromLines = commentsLink.matchData.map(m => reportedComment(m.group(1).toInt)._2)
            val actualLine = CommentRegex.replaceAllIn(line, "")
            val prefix = actualLine.takeWhile(_.isWhitespace)
            commentsFromLines.map(c => s"$prefix//$c").toVector :+ actualLine
          }
        }
        .mkString("\n")
    }

    def formatValue(configValue: ConfigValue, prefix: String, ident: Int, path: Vector[String]): String = {
      val fullIndent = identString * ident
      val linePrefix = if (prefix == "") "" else s"$fullIndent${prefix.stripPrefix(".")}"

      val base = configValue match {
        case list: ConfigList if list.size() == 1 =>
          s"$linePrefix += ${formatValue(list.get(0), prefix = ".", ident + 1, path)}"

        case list: ConfigList if settings.inline(path) || list.isEmpty =>
          list.asScala
            .map(v => formatValue(v, prefix = ".", ident = 0, path))
            .mkString(s"$linePrefix = [", ", ", s"]\n")
        case list: ConfigList =>
          list.asScala
            .map(v => fullIndent + identString + formatValue(v, prefix = ".", ident = 0, path))
            .mkString(s"$linePrefix = [\n", ",\n", s"\n$fullIndent]")

        case obj: ConfigObject if obj.size() == 1 =>
          val (key, value) = obj.asScala.head
          formatValue(value, s"$prefix.$key", ident, path :+ key)
        case obj: ConfigObject if settings.inline(path) || obj.isEmpty =>
          val values = settings
            .values(obj, path)
            .map { case (key, value) => formatValue(value, key, ident = 0, path :+ key) }
          values.mkString(s"$linePrefix {", ", ", "}")
        case obj: ConfigObject if path.isEmpty => // Top level object don't need to be wrapped in braces
          val values = settings
            .values(obj, path)
            .map { case (key, value) => formatValue(value, key, ident, path :+ key) }
          values.mkString("\n")
        case obj: ConfigObject =>
          val values = settings
            .values(obj, path)
            .map { case (key, value) => formatValue(value, key, ident + 1, path :+ key) }
          values.mkString(s"$linePrefix {\n", "\n", s"\n$fullIndent}")

        case _ =>
          val strValue = configValue.unwrapped().toString match {
            case ObtFileFormatter.IdStringValue(idString) =>
              idString
            case _ =>
              configValue.render()
          }
          if (prefix != ".") s"$linePrefix = $strValue"
          else strValue
      }

      val comments = for {
        origin <- Option(configValue.origin()).toList
        comment <- origin.comments().asScala
        if !reportedComment.exists { case (p, c) => c == comment && p.startsWith(path) }
      } yield {
        val index = reportedComment.size
        reportedComment :+= (path -> comment)
        s"//$index "
      }

      val endLine = if (settings.blankLineAfter(path)) "\n" else ""
      s"${comments.mkString("")}$base$endLine"
    }
  }
}
