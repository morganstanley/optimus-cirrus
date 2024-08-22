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
package optimus.platform.installcommonpath

final case class InstallCommonPath(path: String, nfsPrefix: String, afsPrefix: String) {
  private val normalizedNfsPrefix: String = normalizeString(nfsPrefix)
  private val normalizedAfsPrefix: String = normalizeString(afsPrefix)
  private val normalizedPath: String = normalizeString(path)

  private def normalizeString(str: String): String = {
    val charPath = str.toCharArray.toSeq
    charPath.indices
      .flatMap { i =>
        val currentChar: Char = charPath(i)
        val isDuplicate = sameCharSequence(currentChar, i, charPath)
        if (currentChar == '/')
          if (!isDuplicate)
            Some(currentChar)
          else
            None
        else if (currentChar == '\\')
          if (!isDuplicate)
            Some('/')
          else
            None
        else Some(currentChar)
      }
      .mkString("")
  }

  private def sameCharSequence(currChar: Char, currentCharIndex: Int, charSeq: Seq[Char]): Boolean =
    if (currentCharIndex != 0)
      currChar == charSeq(currentCharIndex - 1)
    else
      false

  val isNfs: Boolean = normalizedPath.startsWith(normalizedNfsPrefix)
  val isAfs: Boolean = normalizedPath.startsWith(normalizedAfsPrefix)

  val metaProjectRelease: Option[MetaProjectRelease] = {
    val pathTokens = normalizedPath.split('/')
    val count = pathTokens.length
    val afsPrefixSize = normalizedAfsPrefix.split('/').count(_.nonEmpty)
    if (isAfs && count >= afsPrefixSize + 4) {
      val meta = pathTokens(afsPrefixSize + 1)
      val project = pathTokens(afsPrefixSize + 3)
      val release = pathTokens(afsPrefixSize + 4)
      Some(MetaProjectRelease(meta, project, Some(release)))
    } else if (isNfs && count >= 7) {
      val meta = pathTokens(count - 7)
      val project = pathTokens(count - 6)
      val release = pathTokens(count - 5)
      Some(MetaProjectRelease(meta, project, Some(release)))
    } else None
  }
}

final case class MetaProjectRelease(meta: String, proj: String, release: Option[String])
