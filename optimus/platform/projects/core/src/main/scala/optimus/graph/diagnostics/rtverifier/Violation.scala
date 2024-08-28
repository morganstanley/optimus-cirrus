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
package optimus.graph.diagnostics.rtverifier

import org.apache.commons.text.StringEscapeUtils

final case class Violation(category: String, owner: String, key: String, details: String)

private[rtverifier] final case class FrameViolation(
    count: Int,
    owner: String,
    frame: String,
    stackTraces: Seq[StackTraceViolation])

private[rtverifier] object FrameViolation {

  def convertAndSort(violations: Seq[Violation]): Seq[FrameViolation] =
    violations
      .groupBy(v => v.owner -> StringEscapeUtils.escapeHtml4(v.key))
      .map { case ((owner, frame), vs) =>
        val stackTraceViolations = StackTraceViolation.convertAndSort(owner = owner, frame = frame, vs)
        FrameViolation(count = vs.size, owner = owner, frame = frame, stackTraces = stackTraceViolations)
      }
      .toSeq
      .sortBy(v => -v.count -> v.owner -> v.frame)
}

private final case class StackTraceViolation(count: Int, owner: String, frame: String, stackTrace: String)
private object StackTraceViolation {

  def convertAndSort(owner: String, frame: String, violations: Seq[Violation]): Seq[StackTraceViolation] = {
    violations
      .groupBy(v => StringEscapeUtils.escapeHtml4(v.details))
      .map { case (stackTrace, violationsPerStackTrace) =>
        StackTraceViolation(count = violationsPerStackTrace.size, owner = owner, frame = frame, stackTrace = stackTrace)
      }
      .toSeq
      .sortBy(v => -v.count -> v.owner -> v.frame)
  }

}
