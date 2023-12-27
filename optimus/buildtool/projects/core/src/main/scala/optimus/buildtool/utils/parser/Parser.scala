/*
 * Scala (https://www.scala-lang.org)
 *
 * Copyright EPFL and Lightbend, Inc.
 *
 * Licensed under Apache License 2.0
 * (http://www.apache.org/licenses/LICENSE-2.0).
 *
 * Modifications: Copyright Morgan Stanley
 *
 * This file is vendored from the scala standard library. We use it to tokenize shell arguments, to go from
 *
 * "-A 'b c' d   -f"
 *
 * to
 *
 * Seq("-A", "b c", "d", "-f")
 *
 * in a way that mimics the shell way to do it. We do it this way because the scala standard library doesn't have a
 * standard way of attacking that problem, and the code that we vendored changed significantly between 2.12 and 2.14, so
 * we don't want to use reflection.
 *
 * The main modifications (from the 2.13 version we vendored) are a bug fix for https://github.com/scala/bug/issues/12611
 * and the addition of a keepQuotes flag that keeps the quotes part of the tokens.
 *
 * For those modifications that we made:
 *
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

package optimus.buildtool.utils.parser

import scala.annotation.tailrec

/**
 * A simple enough command line parser.
 */
object Parser {
  private final val DQ = '"'
  private final val SQ = '\''

  /**
   * Split the line into tokens separated by whitespace or quotes.
   *
   * @return
   *   either an error message or reverse list of tokens
   */
  private def tokens(in: String, keepQuotes: Boolean) = {
    import Character.isWhitespace
    import java.lang.{StringBuilder => Builder}

    import collection.mutable.ArrayBuffer

    var accum: List[String] = Nil
    var pos = 0
    var start = 0
    val qpos = new ArrayBuffer[Int](16) // positions of paired quotes

    def cur: Int = if (done) -1 else in.charAt(pos)

    def bump(): Unit = pos += 1
    def done = pos >= in.length

    def skipToQuote(q: Int) = {
      var escaped = false
      def terminal = in.charAt(pos) match {
        case _ if escaped => escaped = false; false
        case '\\'         => escaped = true; false
        case `q`          => true
        case _            => false
      }
      while (!done && !terminal) pos += 1
      !done
    }

    var escapedCur = false
    @tailrec
    def skipToDelim(): Boolean = {
      cur match {
        case _ if escapedCur      => escapedCur = false; bump(); skipToDelim()
        case '\\'                 => escapedCur = true; bump(); skipToDelim()
        case q @ (DQ | SQ)        => { qpos += pos; bump(); skipToQuote(q) } && { qpos += pos; bump(); skipToDelim() }
        case -1                   => true
        case c if isWhitespace(c) => true
        case _                    => bump(); skipToDelim()
      }
    }
    def skipWhitespace(): Unit = while (isWhitespace(cur)) pos += 1
    def copyText() = {
      val buf = new Builder
      var p = start
      var i = 0
      while (p < pos) {
        if (i >= qpos.size) {
          buf.append(in, p, pos)
          p = pos
        } else if (p == qpos(i)) {
          // start and end of delimited substring, which might include the delimiters
          val (start, end) = if (keepQuotes) (qpos(i), qpos(i + 1) + 1) else (qpos(i) + 1, qpos(i + 1))
          buf.append(in, start, end)
          p = qpos(i + 1) + 1
          i += 2
        } else {
          buf.append(in, p, qpos(i))
          p = qpos(i)
        }
      }
      buf.toString
    }
    def text() = {
      val res =
        if (qpos.isEmpty) in.substring(start, pos.min(in.length))
        else if (qpos(0) == start && qpos(1) == pos) in.substring(start + 1, pos - 1)
        else copyText()
      qpos.clear()
      res
    }
    def badquote = Left("Unmatched quote")

    @tailrec def loop(): Either[String, List[String]] = {
      skipWhitespace()
      start = pos
      if (done) Right(accum)
      else if (!skipToDelim()) badquote
      else {
        accum = text() :: accum
        loop()
      }
    }
    loop()
  }

  class ParseException(msg: String) extends RuntimeException(msg)

  def tokenize(line: String, keepQuotes: Boolean = false): List[String] =
    tokens(line, keepQuotes) match {
      case Right(args) => args.reverse
      case Left(msg)   => throw new ParseException(msg)
    }

}
