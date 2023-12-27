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
package optimus.buildtool.dependencies
import optimus.buildtool.config.ModuleType
import optimus.buildtool.config.NamingConventions

final case class PythonDependencies(
    pythonDefinitions: Set[PythonDefinition],
    dependencies: Set[PythonDependency]
) {
  val afsDependencies: Set[PythonAfsDependencyDefinition] = dependencies.collect {
    case dep @ PythonAfsDependencyDefinition(_, _, _, _, _, _) => dep
  }

  val artifactoryDependencies: Set[PythonDependencyDefinition] = dependencies.collect {
    case dep @ PythonDependencyDefinition(_, _, _, _, _) => dep
  }

  def pythonVersionForConfig(specifiedVersion: Option[String]): Option[PythonDefinition] = {
    specifiedVersion match {
      case Some(version) => variantPythonDefinition(version)
      case None          => defaultPythonDefinition
    }
  }

  def findArtifactoryDependency(libName: String): Option[PythonDependencyDefinition] =
    artifactoryDependencies.find(_.label == libName)
  def findAfsDependency(libName: String): Option[PythonAfsDependencyDefinition] =
    afsDependencies.find(_.label == libName)
  def findDependency(libName: String, moduleType: ModuleType): Option[PythonDependency] = moduleType match {
    case ModuleType.Afs  => findAfsDependency(libName)
    case ModuleType.PyPi => findArtifactoryDependency(libName)
  }

  private val defaultPythonDefinition: Option[PythonDefinition] = pythonDefinitions.find(_.variant.isEmpty)
  private def variantPythonDefinition(variantName: String): Option[PythonDefinition] =
    pythonDefinitions.find {
      case PythonDefinition(_, _, _, Some(variant)) => variant.name == variantName
      case _                                        => false
    }
}

final case class PythonDefinition(
    version: String,
    path: String,
    venvPack: String,
    variant: Option[PythonDependencies.Variant])

sealed trait PythonDependency {
  def name: String
  def version: String
  def sourceType: String // This is for json deserialization in StructureBuilder
  def variant: Option[PythonDependencies.Variant]
  def label: String = variant
    .map(v => s"$name.variant.${v.name}")
    .getOrElse(name)
}

final case class PythonDependencyDefinition(
    name: String,
    pypiName: String,
    version: String,
    variant: Option[PythonDependencies.Variant],
    sourceType: String = ModuleType.PyPi.label)
    extends PythonDependency {}
final case class PythonAfsDependencyDefinition(
    name: String,
    meta: String,
    project: String,
    version: String,
    variant: Option[PythonDependencies.Variant],
    sourceType: String = ModuleType.Afs.label)
    extends PythonDependency {
  def path: String = s"${NamingConventions.AfsRootStr}dist/$meta/PROJ/$project/$version/lib"
}

object PythonDependencies {
  final case class Variant(name: String, reason: String)

  def empty(): PythonDependencies = PythonDependencies(Set.empty, Set.empty)
}
