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
package optimus.buildtool.utils

import scala.util.matching.Regex
import scala.util.parsing.combinator.JavaTokenParsers
import scala.util.parsing.combinator.PackratParsers
import scala.util.parsing.combinator.RegexParsers

trait TextTokenParsers extends RegexParsers {
  def createTextRegex(additionalCharsToExclude: String = ""): Regex =
    ("[^%$`" + additionalCharsToExclude + "]+").r

  // NOTE: must be valid for all of our parser traits as we combine them
  private[utils] lazy val text = createTextRegex("\\\\")
}

// Windows Command Shell
//  - Partial support for percent expansion rules
//  - No support for delayed expansion rules
//  - Extremely limited support for character escaping
//
// Windows parsing and expansion rules:
//   - https://stackoverflow.com/questions/4094699/how-does-the-windows-command-interpreter-cmd-exe-parse-scripts/4095133#4095133
//   - https://stackoverflow.com/questions/4094699/how-does-the-windows-command-interpreter-cmd-exe-parse-scripts/7970912#7970912
//
trait WindowsParameterExpansionTokensParser extends JavaTokenParsers {
  private[utils] lazy val windowsVarToken = """[^% ]+""".r // Too lenient, i.e. charset not restricted
  private[utils] lazy val windowsStandalonePercent = "%".r
  private[utils] val windowsAllArgs = "%*" // Unsupported, so keep as literal

  def windowsVar: Parser[Any] =
    ("%" ~> windowsVarToken <~ "%") ^^ SimpleLookup

  def windowsPercentEscape: Parser[Any] =
    "%%" ^^ { _.drop(1) } // Unescape

  def windowsArgRef: Parser[Any] =
    "%" ~ wholeNumber ^^ { case p ~ d =>
      p + d // Unsupported, so keep as literal
    }

  def windowsExpr: Parser[Any] =
    windowsPercentEscape | windowsArgRef | windowsAllArgs | windowsVar | windowsStandalonePercent
}

// Bash
//  - No support for indirection (e.g. ${!name[*]},  ${!name[@]}, ${!prefix*},
//  - No support for arrays
//  - No support for * and @ expansion (e.g. ${name[*]},  ${name[@]}, ${prefix*}, ${prefix@})
//  - No support for length measurement (e.g. ${#variable})
//  - No support for operators other than :=, :+, :? and :-
//
// Unix shell expansion rules:
//   - https://www.gnu.org/software/bash/manual/html_node/Shell-Parameter-Expansion.html
//
trait BashParameterExpansionTokensParser extends TextTokenParsers with PackratParsers {
  private[utils] lazy val bashIdentifier = """[a-zA-Z_][a-zA-Z_0-9]*""".r
  private[utils] lazy val bashPositional = """[0-9]""".r // Single digit
  private[utils] lazy val bashExpansionOp = """:[-=?+]""".r
  private[utils] val bashPid = "$$" // Unsupported, so keep as literal
  private[utils] val bashStandaloneDollar = "$" // Lone, so keep literal
  private[utils] val bashStandaloneBackslash = "\\" // Lone, so keep literal

  def bashSingleDigitPositionalParameter: Parser[Any] =
    "$" ~ bashPositional ^^ { case d ~ p =>
      d + p // Unsupported, so keep as literal
    }

  def bashMultiDigitPositionalParameter: Parser[Any] =
    ("${" ~ rep1(bashPositional) ~ "}") ^^ { case _ ~ t ~ _ =>
      s"$${${t.mkString("")}}" // Unsupported, so keep as literal
    }

  def bashEscapedNoBraceParameter: Parser[Any] =
    "\\" ~> "$" ~ bashIdentifier ^^ { case d ~ p =>
      d + p // Keep as literal for this escaped identifier
    }

  def bashNoBraceParameter: Parser[Any] =
    ("$" ~> bashIdentifier) ^^ SimpleLookup

  def bashBracedParameter: Parser[Any] =
    ("${" ~> bashIdentifier <~ "}") ^^ SimpleLookup

  // Keeping the closing curly bracket here as to help parser algorithm to disambiguate
  private[utils] lazy val textWithinBraces = createTextRegex("}")
  lazy val bashParameterExpression: PackratParser[Any] =
    (bashIdentifier ~ bashExpansionOp <~ "}") ^^ {
      case t ~ o if o == ":-" => LookupOrElse(t, List.empty)
      case t ~ o if o == ":=" => LookupOrElseAssign(t, List.empty)
      case t ~ o if o == ":?" => LookupOrElseComplain(t, List.empty)
      case t ~ o if o == ":+" => LookupMessageIfSet(t, List.empty)
      // this shouldn't happen as we are matching from a closed set of characters
      case other => throw new MatchError(s"$other did not match")
    } | (bashIdentifier ~ bashExpansionOp ~ rep(bashExpr | textWithinBraces) <~ "}") ^^ {
      case t ~ o ~ w if o == ":-" => LookupOrElse(t, w)
      case t ~ o ~ w if o == ":=" => LookupOrElseAssign(t, w)
      case t ~ o ~ w if o == ":?" => LookupOrElseComplain(t, w)
      case t ~ o ~ w if o == ":+" => LookupMessageIfSet(t, w)
      // see comment above
      case other => throw new MatchError(s"$other did not match")
    }

  lazy val bashBracedParameterExpression: PackratParser[Any] =
    "${" ~> bashParameterExpression

  def whoAmI: Parser[Any] =
    "`whoami`" ^^ { _ =>
      WhoAmI()
    }

  lazy val bashExpr: PackratParser[Any] =
    bashPid | bashSingleDigitPositionalParameter | bashEscapedNoBraceParameter | bashNoBraceParameter | bashMultiDigitPositionalParameter | bashBracedParameter | bashBracedParameterExpression | whoAmI | bashStandaloneDollar | bashStandaloneBackslash
}

object ParameterExpansionParser
    extends PackratParsers
    with TextTokenParsers
    with WindowsParameterExpansionTokensParser
    with BashParameterExpansionTokensParser {
  override def skipWhitespace: Boolean = false

  def all: PackratParser[List[Any]] = rep(windowsExpr | bashExpr | text)

  def parse(s: String): ParseResult[List[Any]] =
    parseAll(all, s)

  override def phrase[T](p: ParameterExpansionParser.Parser[T]): PackratParser[T] = super[PackratParsers].phrase(p)
  def eval(s: String, envVars: Map[String, String] = sys.env): String =
    parseAll(all, s) match {
      case Failure(msg, _) => msg
      case Error(msg, _)   => msg
      case Success(parts, _) =>
        ParameterEvaluator.evalParts(envVars, parts)
    }
}

trait ParameterEvaluator {
  def eval(envVars: Map[String, String]): String
}

object ParameterEvaluator {
  final def evalParts(envVars: Map[String, String], parts: List[Any]): String =
    parts
      .map {
        case e: ParameterEvaluator =>
          e.eval(envVars)
        case other =>
          other.toString
      }
      .mkString("")
}

import optimus.buildtool.utils.ParameterEvaluator._

// Common evaluators
final case class SimpleLookup(name: String) extends ParameterEvaluator {
  def eval(envVars: Map[String, String]): String = envVars.getOrElse(name, "")
}

// BASH-style evaluators
sealed trait BashStyleEvaluator extends ParameterEvaluator
final case class WhoAmI() extends BashStyleEvaluator {
  def eval(envVars: Map[String, String]): String = sys.props.get("user.name").getOrElse("")
}
final case class LookupOrElse(name: String, expr: List[Any]) extends BashStyleEvaluator {
  def eval(envVars: Map[String, String]): String =
    envVars.getOrElse(name, evalParts(envVars, expr))
}
final case class LookupOrElseAssign(name: String, expr: List[Any]) extends BashStyleEvaluator {
  def eval(envVars: Map[String, String]): String =
    envVars.getOrElse(name, evalParts(envVars, expr)) // We do not support assigning right now
}
final case class LookupOrElseComplain(name: String, expr: List[Any]) extends BashStyleEvaluator {
  def eval(envVars: Map[String, String]): String =
    envVars.getOrElse(
      name, { // Mimic zsh
        // No logging available in this project because it integrates with IntelliJ
        println(s"Runconf sh: $name: ${if (expr.nonEmpty) evalParts(envVars, expr) else "parameter not set"}")
        ""
      }
    )
}
final case class LookupMessageIfSet(name: String, expr: List[Any]) extends BashStyleEvaluator {
  def eval(envVars: Map[String, String]): String =
    envVars.get(name) match {
      case Some(_) => evalParts(envVars, expr)
      case _       => ""
    }
}
