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

import optimus.platform.platform.config.StaticConfig
import java.io.FileInputStream
import java.util.Properties
import org.apache.commons.text.CaseUtils

import scala.jdk.CollectionConverters._
import optimus.scalacompat.collection._

object BranchConfiguration {
  private lazy val branchConfig: Map[String, String] = {
    val environmentPath = System.getenv().getOrDefault("ENVIRONMENT_PATH", StaticConfig.string("environmentPath"))
    val fis = new FileInputStream(environmentPath + "/BranchConfig.py")

    try {
      val prop = new Properties
      prop.load(fis)
      prop.asScala.toMap.mapValuesNow(_.stripPrefix("\"").stripSuffix("\""))
    } finally {
      fis.close()
    }
  }

  lazy val Main: String = branchConfig("MAIN_BRANCH")
  lazy val Staging: String = branchConfig("STAGING_BRANCH")
  lazy val StagingArtifactNamingSuffix: String = branchConfig("STAGING_ARTIFACT_NAMING_SUFFIX")
  lazy val StagingArtifactLatestSymlink: String = branchConfig("STAGING_ARTIFACT_LATEST_SYMLINK")
  lazy val StagingJenkinsJobNamePart: String = branchConfig("STAGING_JENKINS_JOB_NAME_PART")

  lazy val StagingCamelCase: String = CaseUtils.toCamelCase(Staging, true, '-', '_')
}
