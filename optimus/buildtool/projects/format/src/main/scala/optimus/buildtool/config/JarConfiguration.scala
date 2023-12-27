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
package optimus.buildtool.config

import java.{util => ju}

import optimus.buildtool.files.Directory

import scala.jdk.CollectionConverters._

final case class TestplanConfiguration(
    cell: String,
    cpu: String,
    disk: String,
    mem: String,
    ignoredPaths: Seq[String],
    priority: Int,
    useTestCaching: Boolean,
    useDynamicTests: Boolean
)

final case class GenericRunnerConfiguration(genericRunnerAppDirOverride: Option[Directory])

final case class VersionConfiguration(
    installVersion: String,
    obtVersion: String,
    stratosphereVersion: String,
    scalaVersion: String
)

final case class JarConfiguration(
    manifest: Map[String, String]
) {
  def javaManifest: ju.Map[String, String] = manifest.asJava
}
