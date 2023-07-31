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

import java.util.concurrent.CopyOnWriteArrayList
import scala.jdk.CollectionConverters._
import scala.sys.process.ProcessLogger

final case class LineDescriptor(isError: Boolean, line: String)

final case class CustomProcessLogger(logger: Logger, printOutputToScreen: Boolean) extends ProcessLogger {
  override def out(s: => String): Unit = append(LineDescriptor(isError = false, s))
  override def err(s: => String): Unit = append(LineDescriptor(isError = true, s))
  override def buffer[T](f: => T): T = f

  def standardOutputOnly: String = getLines(!_.isError)
  def errorOutputOnly: String = getLines(_.isError)
  def output: String = getLines()

  private def getLines(condition: LineDescriptor => Boolean = _ => true): String =
    lines.iterator().asScala.filter(condition).map(_.line).mkString(System.lineSeparator())

  private def append(lineDesc: LineDescriptor): Unit = {
    lines.add(lineDesc)
    if (printOutputToScreen) logger.info(lineDesc.line) else logger.debug(lineDesc.line)
  }

  private val lines = new CopyOnWriteArrayList[LineDescriptor]()
}
