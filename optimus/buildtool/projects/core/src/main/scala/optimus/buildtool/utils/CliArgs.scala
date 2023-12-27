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

import optimus.buildtool.utils.parser.Parser

import scala.collection.immutable.Seq

object CliArgs {

  /**
   * Normalize and tokenize command-line arguments.
   *
   * This function respects quoting and escaping of spaces, unlike a simple .splitWhitespace(). It will attempt to
   * maintain the given token separation. See normalizeWithRetry for a version that will collapse the args together
   * before tokenizing.
   */
  def normalize(args: Seq[String], keepQuotes: Boolean = true): Seq[String] =
    args
      .flatMap { arg =>
        try { Parser.tokenize(arg, keepQuotes) }
        catch {
          case e: Parser.ParseException =>
            throw new RuntimeException(s"Error while tokenizing $arg: ${e.getMessage}")
        }
      }
}
