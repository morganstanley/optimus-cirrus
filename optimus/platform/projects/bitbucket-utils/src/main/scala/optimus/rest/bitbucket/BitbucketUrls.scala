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
package optimus.rest.bitbucket

/**
 * For the non-API related URLs.
 *
 * For the API-related, see [[BitBucketApiUrls]].
 */
final case class BitbucketUrls(host: String) {

  private def basePath(project: String, repo: String): String =
    s"http://$host/atlassian-stash/projects/$project/repos/$repo"

  def browseFile(
      project: String,
      repo: String,
      filePath: Option[String] = None,
      atRef: Option[String] = None,
      lineRange: Option[String] = None): String = {
    val filePart = if (filePath.nonEmpty) s"/$filePath" else ""
    val commitPart = atRef.map(commit => s"?at=$commit").getOrElse("")
    val lineRangePart = lineRange.map(range => s"#$range").getOrElse("")
    s"${basePath(project, repo)}/browse$filePart$commitPart$lineRangePart"
  }

  def commit(project: String, repo: String, commit: String): String =
    s"${basePath(project, repo)}/commits/$commit"

  def pullRequest(project: String, repo: String, prId: Long): String =
    s"${basePath(project, repo)}/pull-requests/$prId/"

}
