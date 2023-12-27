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

import optimus.buildtool.dependencies.PythonAfsDependencyDefinition
import optimus.buildtool.dependencies.PythonDefinition
import optimus.buildtool.dependencies.PythonDependency
import optimus.buildtool.dependencies.PythonDependencyDefinition

sealed trait ModuleType {
  def label: String = this match {
    case ModuleType.Afs  => "afs"
    case ModuleType.PyPi => "pypi"
  }
}

object ModuleType {
  object Afs extends ModuleType
  object PyPi extends ModuleType

  def default: ModuleType = PyPi
  def resolve(label: String): Option[ModuleType] = Seq(Afs, PyPi).find(_.label == label)
}

final case class PythonConfiguration(
    python: PythonDefinition,
    libs: Set[PythonDependency],
    moduleType: ModuleType
) {
  def afsDependencies: Set[PythonAfsDependencyDefinition] = libs.collect {
    case dep @ PythonAfsDependencyDefinition(_, _, _, _, _, _) => dep
  }
  def artifactoryDependencies: Set[PythonDependencyDefinition] = libs.collect {
    case dep @ PythonDependencyDefinition(_, _, _, _, _) => dep
  }

  def isAfs: Boolean = moduleType == ModuleType.Afs
  def isArtifactory: Boolean = moduleType == ModuleType.PyPi
}
