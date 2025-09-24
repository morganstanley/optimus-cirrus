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
package optimus.stratosphere.utils

import scala.annotation.tailrec
import scala.collection.compat._

object CollectionUtils {
  def splitByLength(elements: Seq[String], lengthLimit: Int): Seq[Seq[String]] = {
    @tailrec def groupByLengthLimit(
        elements: Seq[String],
        currLength: Int,
        curr: Seq[String],
        acc: Seq[Seq[String]]): Seq[Seq[String]] = {
      elements match {
        case Seq() if curr.isEmpty => acc
        case Seq()                 => acc :+ curr
        case Seq(cmd, _*) if cmd.length > lengthLimit =>
          throw new RuntimeException(
            s"cannot group by length smaller than long command: $cmd (length ${cmd.length}) > $lengthLimit")
        case Seq(cmd, rest @ _*) if cmd.length + currLength <= lengthLimit =>
          groupByLengthLimit(rest.to(Seq), cmd.length + currLength, curr :+ cmd, acc)
        case cmdTooLong =>
          groupByLengthLimit(cmdTooLong, 0, Seq(), acc :+ curr)
      }
    }

    groupByLengthLimit(elements, 0, Seq(), Seq())

  }
}
