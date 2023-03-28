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
package optimus.utils

object StringDiffExplainer {
  private val ellipsis = "..."
  private val diffBraces = "[]"

  /**
   * The format is to show where the change is, with indication of the range that is different, with some context
   * around.
   */
  private[utils] def explainStringDiff(lhs: String, rhs: String): ExplanationState = {
    val longestSize = Math.max(lhs.length, rhs.length)
    val explanation = lhs
      .padTo(longestSize, ' ')
      .zip(rhs.padTo(longestSize, ' '))
      .zipWithIndex
      .foldLeft(ExplanationState(inDiff = false, -1, -1, -1, -1, "", "")) {
        case (state, ((lhsChar, rhsChar), index)) if lhsChar != rhsChar && !state.inDiff =>
          val newFirstIndex = if (state.firstDiffIndex >= 0) state.firstDiffIndex else state.left.length
          val fromSourceIndex = if (state.fromSourceIndex >= 0) state.fromSourceIndex else index
          state.copy(
            inDiff = true,
            firstDiffIndex = newFirstIndex,
            lastDiffIndex = state.left.length + 1,
            fromSourceIndex = fromSourceIndex,
            toSourceIndex = index,
            left = state.left + '[' + lhsChar,
            right = state.right + '[' + rhsChar
          )

        case (state, ((lhsChar, rhsChar), index)) if lhsChar != rhsChar && state.inDiff =>
          state.copy(
            lastDiffIndex = state.left.length,
            toSourceIndex = index,
            left = state.left + lhsChar,
            right = state.right + rhsChar)

        case (state, ((lhsChar, rhsChar), _)) if lhsChar == rhsChar && state.inDiff =>
          state.copy(
            inDiff = false,
            lastDiffIndex = state.left.length,
            left = state.left + ']' + lhsChar,
            right = state.right + ']' + rhsChar)

        case (state, ((lhsChar, rhsChar), _)) =>
          state.copy(inDiff = false, left = state.left + lhsChar, right = state.right + rhsChar)
      }
    if (explanation.inDiff)
      explanation.copy(
        inDiff = false,
        lastDiffIndex = explanation.left.length,
        left = explanation.left + ']',
        right = explanation.right + ']')
    else
      explanation
  }

  private[utils] def windowOnDiff(
      explanation: ExplanationState,
      maxLen: Int = 100,
      charsBefore: Int = 5,
      charsAfter: Int = 5): Explanation = {
    require(
      (maxLen - charsBefore - charsAfter - ellipsis.length - diffBraces.length) >= 0,
      "Max explanation length must allow for context, ellipsis and diff braces")
    if (explanation.fromSourceIndex < 0 || explanation.toSourceIndex < 0) {
      Explanation(-1, -1, explanation.left, explanation.right)
    } else {
      require(explanation.firstDiffIndex != -1 && explanation.lastDiffIndex != -1)
      val fromAnnotatedIndex = Math.max(0, explanation.firstDiffIndex - charsBefore)
      val toAnnotatedIndex = Math.min(
        explanation.lastDiffIndex + charsAfter + 1 /* lastDiffIndex is inclusive */,
        Math.max(explanation.left.length, explanation.right.length))
      Explanation(
        explanation.fromSourceIndex,
        explanation.toSourceIndex,
        windowOnString(explanation.left, fromAnnotatedIndex, toAnnotatedIndex, maxLen),
        windowOnString(explanation.right, fromAnnotatedIndex, toAnnotatedIndex, maxLen)
      )
    }
  }

  def explain(lhs: String, rhs: String, maxLen: Int = 100, charsBefore: Int = 5, charsAfter: Int = 5): Explanation = {
    windowOnDiff(explainStringDiff(lhs, rhs), maxLen, charsBefore, charsAfter)
  }

  private def windowOnString(content: String, fromIndex: Int, toIndex: Int, maxLen: Int): String = {
    val chunk = content.slice(fromIndex, toIndex)
    if (chunk.length > maxLen) {
      val ellipsisHalfLength: Int = ellipsis.length >> 1
      val ellipsisResidualLength: Int = ellipsis.length - ellipsisHalfLength
      val firstCutPoint: Int = (maxLen >> 1) - ellipsisHalfLength
      val secondCutPoint: Int = chunk.length - Math.max(0, maxLen - firstCutPoint - ellipsisResidualLength)
      chunk.slice(0, firstCutPoint) + ellipsis + chunk.slice(secondCutPoint, chunk.length)
    } else chunk
  }
}

final case class ExplanationState(
    inDiff: Boolean,
    // Effective indices of the brackets
    firstDiffIndex: Int,
    lastDiffIndex: Int, // Inclusive
    // Source indices of the difference boundaries
    fromSourceIndex: Int,
    toSourceIndex: Int, // Inclusive
    // The two sides, with annotations
    left: String,
    right: String)

final case class Explanation(fromSourceIndex: Int, toSourceIndex: Int, left: String, right: String) {
  def explainsDifferences: Boolean = fromSourceIndex != -1 && toSourceIndex != -1
}
