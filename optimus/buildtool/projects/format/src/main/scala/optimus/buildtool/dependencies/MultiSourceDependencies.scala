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

import com.typesafe.config._
import optimus.buildtool.config._
import optimus.buildtool.format.ObtFile
import optimus.buildtool.format.Result
import optimus.buildtool.format.Failure
import optimus.buildtool.format.Success

import scala.collection.immutable.Seq

final case class MultiSourceDependencies(loaded: Seq[MultiSourceDependency]) {
  val (noVersionMavenDeps, versionDeps) = loaded.partition(_.noVersionMaven)
  val (mavenOnlyDeps, afsDefinedDeps) = versionDeps.partition(_.mavenOnly)
  val (afsOnlyDeps, multiSourceDeps) = afsDefinedDeps.partition(_.afsOnly)
  val mavenDefinedDeps = versionDeps.filter(_.maven.nonEmpty)

  def ++(to: MultiSourceDependencies): MultiSourceDependencies = MultiSourceDependencies(
    (this.loaded ++ to.loaded).distinct)
}

final case class MultiSourceDependency(
    name: String,
    afs: Option[DependencyDefinition],
    maven: Seq[DependencyDefinition],
    line: Int,
    ivyConfig: Boolean = false)
    extends OrderedElement {
  private def mavenDefinition: DependencyDefinition = maven match {
    case Seq(mavenAlone) => mavenAlone // for maven-only mixed mode definitions
    case multipleMaven
        if maven.count(_.variant.isEmpty) > 1 => // prevent user define multiple maven deps when afs{} is empty
      val linesStr = maven.map { d => s"'${d.key}' at line ${d.line}" }.mkString(", ")
      throw new IllegalArgumentException(
        s"[${MultiSourceDependenciesLoader.jvmPathStr}] '$name' without afs definition is not allowed to define multiple maven definitions: $linesStr")
    case _ => // both afs{} & maven{} are empty, we don't allow empty definition
      throw new IllegalArgumentException(
        s"[${MultiSourceDependenciesLoader.jvmPathStr}] '$name' at line $line can't be empty!")
  }

  def mavenOnly: Boolean = afs.isEmpty && maven.size == 1

  def afsOnly: Boolean = maven.isEmpty && afs.size == 1

  def noVersionMaven: Boolean = maven.nonEmpty && maven.forall(_.noVersion)

  def definition: DependencyDefinition = afs match {
    case Some(d) => d
    case None    => mavenDefinition
  }

  def asExternalDependency: ExternalDependency = this match {
    case both if both.afs.isDefined && both.maven.nonEmpty => ExternalDependency(this.definition, maven)
    case _                                                 => ExternalDependency(this.definition, Nil)
  }

  // jvm-dependencies.obt id is single string
  override def id: DependencyId = DependencyId(group = "obt.jvm.loaded", name = name)
}

object MultiSourceDependency {
  private[buildtool] val MultipleAfsError = "only one afs dependency should be defined!"
  private[buildtool] def NoMavenVariantError(name: String) = s"'$name' no maven equivalent variant found!"
  private[buildtool] val NoVersionError = "maven version must be defined in order to map afs dependency!"
  private[buildtool] def emptyDepError(name: String) = s"'$name' is empty!"

  def apply(
      obtFile: ObtFile,
      confValue: ConfigValue,
      name: String,
      afsDep: Option[DependencyDefinition],
      mavenDeps: Seq[DependencyDefinition],
      ivyCfgsMap: Map[DependencyDefinition, Seq[DependencyDefinition]],
      line: Int): Result[Seq[MultiSourceDependency]] = {
    val emptyError = Failure(Seq(obtFile.errorAt(confValue, emptyDepError(name))))
    val loadedIvyCfgs = ivyCfgsMap.map { case (ivy, equivalents) =>
      MultiSourceDependency(s"$name.${ivy.configuration}", Some(ivy), equivalents, ivy.line, ivyConfig = true)
    }

    def getMavenVariants(mavenVariants: Seq[DependencyDefinition]) =
      mavenVariants.map { d =>
        val nameWithVariant = d.variant match {
          case Some(mavenVar) => s"$name.variant.${mavenVar.name}" // for example: foo.variant.bar
          case None           => name
        }
        MultiSourceDependency(nameWithVariant, None, Seq(d), line)
      }

    def getMavenDependencies =
      mavenDeps match { // maven only
        case empty if mavenDeps.isEmpty => emptyError
        case mavenCfgs
            if mavenDeps.map(d => d.group + d.name + d.version).distinct.size == 1 &&
              mavenDeps.map(_.configuration).toSet.size != 1 => // multiple configurations defined
          Success(mavenCfgs.map { d =>
            val nameWithConfig =
              if (d.configuration.nonEmpty) // not default config
                s"$name.${d.configuration}" // for example: scala.libraries
              else name
            MultiSourceDependency(nameWithConfig, None, Seq(d), line)
          })
        case mavenLibsWithVariants
            if mavenDeps.map(d => d.group + d.name).distinct.size == 1 && mavenDeps.count(
              _.variant.isEmpty) == 1 && mavenDeps.exists(_.variant.isDefined) =>
          Success(getMavenVariants(mavenLibsWithVariants))
        case _ => Success(Seq(MultiSourceDependency(name, None, mavenDeps, line)))
      }

    afsDep match {
      case Some(afs) =>
        if (mavenDeps.isEmpty) emptyError
        else if (mavenDeps.exists(_.version.isEmpty))
          Failure(Seq(obtFile.errorAt(confValue, NoVersionError)))
        else {
          val (mavenVariants, mavenLibs) = mavenDeps.partition(_.variant.isDefined)
          val mapped = MultiSourceDependency(name, Some(afs), mavenLibs, line)
          val notMapped = getMavenVariants(mavenVariants) // variants only be used for forced version
          Success(notMapped ++ loadedIvyCfgs :+ mapped)
        }
      case None => getMavenDependencies
    }

  }
}
