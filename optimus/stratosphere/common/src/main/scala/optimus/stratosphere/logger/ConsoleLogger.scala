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
package optimus.stratosphere.logger

import optimus.stratosphere.config.ConsoleColors

import scala.io.StdIn

final case class ConsoleLogger(colors: ConsoleColors) extends Logger(colors) {
  override def readLine(text: String, args: Any*): Option[String] = Option(StdIn.readLine(text, args: _*))
  override def handleAnswer(answer: String): Unit = ()
  override def info(toLog: String): Unit = System.out.println(toLog)
  override def debug(toLog: String): Unit = {} // Never on console, verbose will upgrade to info for us
}
