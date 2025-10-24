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
package optimus.platform.util

import msjava.slf4jutils.scalalog.Logger
import msjava.slf4jutils.scalalog.getLogger
import optimus.platform.AdvancedUtils

object CmdLineTokenizer {

  protected val log: Logger = getLogger(this.getClass)

  import scala.collection.mutable.ArrayBuffer

  def tokenize(cmdStr: String): Array[String] = {
    val tokens: ArrayBuffer[String] = ArrayBuffer()
    val (time1, events) = AdvancedUtils.timed(escapeDoubleQuote(toTokenEvents(cmdStr)))
    log.debug(s"Got ${events.size} events in ${time1 / 1e6} ms")
    val initState = Token("", "", TokenState.Idle, TokenState.Idle)
    val (time2, unit) = AdvancedUtils.timed(events.foldLeft(initState)((b, a) => transition(b, a, tokens)))
    log.debug(s"Parse tokens in ${time2 / 1e6} ms")

    tokens.toArray
  }

  private def transition(d: Token, e: TokenEvent, tokens: ArrayBuffer[String]): Token = {
    d.tokenState match {
      case TokenState.Idle =>
        e match {
          case Space => {
            if (d.term.nonEmpty)
              tokens += d.term
            cloneNewState(d, TokenState.Idle, d.tokenState, "", "")
          }
          case DoubleQuote => cloneNewState(d, TokenState.DoubleQuoteStarted, d.tokenState, d.term, "")
          case EOL => {
            if (d.term.nonEmpty)
              tokens += d.term
            cloneNewState(d, TokenState.End, d.tokenState, "", "")
          }
          case event => cloneNewState(d, TokenState.TermStarted, d.tokenState, d.term + event.value, "")
        }

      case TokenState.DoubleQuoteStarted =>
        e match {
          case DoubleQuote => cloneNewState(d, d.preTokenState, d.tokenState, d.term + d.quoted, "")
          case EOL => {
            tokens += (d.term + d.quoted)
            cloneNewState(d, TokenState.End, d.tokenState, "", "")
          }
          case event => cloneNewState(d, d.tokenState, d.preTokenState, d.term, d.quoted + event.value)
        }
      case TokenState.TermStarted =>
        e match {
          case DoubleQuote => cloneNewState(d, TokenState.DoubleQuoteStarted, d.tokenState, d.term, "")
          case Space => {
            tokens += d.term
            cloneNewState(d, TokenState.Idle, d.tokenState, "", "")
          }
          case EOL => {
            tokens += d.term
            cloneNewState(d, TokenState.End, d.tokenState, "", "")
          }
          case event => cloneNewState(d, d.tokenState, d.preTokenState, d.term + event.value, "")
        }
      case _ => d
    }
  }

  private def toTokenEvents(str: String): IndexedSeq[TokenEvent] = {
    val (tokens, sb) = str.foldLeft((Vector[TokenEvent](), new StringBuilder))((sbt, c) =>
      sbt match {
        case (tokens, sb) => {
          c match {
            // Handle special characters to generate key tokens
            case '"' | ' ' | '\\' => {
              val (tokens_, sb_) =
                if (sb.nonEmpty)
                  (tokens :+ Normal(sb.toString), new StringBuilder())
                else (tokens, sb)

              (
                tokens_ :+ (c match {
                  case '"'  => DoubleQuote
                  case ' '  => Space
                  case '\\' => Backslash
                }),
                sb_)
            }
            case ch => (tokens, sb += ch)
          }
        }
      })

    {
      if (sb.nonEmpty)
        tokens :+ Normal(sb.toString)
      else
        tokens
    } :+ EOL
  }

  private def escapeDoubleQuote(events: IndexedSeq[TokenEvent]): Seq[TokenEvent] = {
    var escape: Boolean = false
    val newEvents = for (i <- 0 until (events.size - 1)) yield {
      if (escape) {
        escape = false
        None
      } else if (events(i) == Backslash && events(i + 1) == DoubleQuote) {
        escape = true
        // Return as regular text as this has been escaped, so transition folding handles it as such
        Some(Normal("\""))
      } else Some(events(i))
    }

    if (!escape) newEvents.flatten :+ events.last
    else newEvents.flatten
  }

  private object TokenState extends Enumeration {
    type State = Value
    val Idle, DoubleQuoteStarted, TermStarted, End = Value
  }
  private final case class Token(
      term: String,
      quoted: String,
      tokenState: TokenState.State,
      preTokenState: TokenState.State)

  private trait TokenEvent {
    def value: String
  }

  private case object Space extends TokenEvent {
    val value: String = " "
  }
  private case object DoubleQuote extends TokenEvent {
    val value: String = "\""
  }

  private case object Backslash extends TokenEvent {
    val value = "\\"
  }

  private case object EOL extends TokenEvent {
    val value: String = "\n"
  }

  private final case class Normal(value: String) extends TokenEvent

  private def cloneNewState(d: Token, s: TokenState.State, p: TokenState.State, t: String, q: String) =
    d.copy(term = t, tokenState = s, preTokenState = p, quoted = q)
}
