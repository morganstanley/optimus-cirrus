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

import scala.collection.compat._
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
    extends OrderedElement[DependencyId] {
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
  private[buildtool] val NoVersionError = "maven version must be defined in order to map afs dependency!"
  private[buildtool] def emptyDepError(name: String) =
    s"'$name' has no maven definition or explicit runtime ivy configuration!"

  def expandDependencyDefinitions(
      obtFile: ObtFile,
      confValue: ConfigValue,
      name: String,
      afsDep: Option[DependencyDefinition],
      afsVariants: Seq[DependencyDefinition],
      mavenDeps: Seq[DependencyDefinition],
      maybeOverridenRuntime: Option[MultiSourceDependency],
      line: Int): Result[Seq[MultiSourceDependency]] = {
    val emptyError = Failure(Seq(obtFile.errorAt(confValue, emptyDepError(name))))

    def getMavenVariants(mavenVariants: Seq[DependencyDefinition]) =
      mavenVariants.map { d =>
        val nameWithVariant = d.variant match {
          case Some(mavenVar) => s"$name.variant.${mavenVar.name}" // for example: foo.variant.bar
          case None           => name
        }
        MultiSourceDependency(nameWithVariant, None, Seq(d), d.line)
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
        // Remove the comments after next obt release.
        if (maybeOverridenRuntime.isDefined /* && mavenDeps.nonEmpty */ ) Success(Nil)
        /* Failure(Seq(obtFile.errorAt(confValue, "There is a runtime override present in `ivyConfigurations` at line ${maybeOverridenRuntime.get.line}")), please remove this line and add this dependency inside ivyConfigurations along with including it in `runtime.extends` list */
        else if (mavenDeps.isEmpty) emptyError
        else if (mavenDeps.exists(_.version.isEmpty))
          Failure(Seq(obtFile.errorAt(confValue, NoVersionError)))
        else {
          val afsVariantMap = toMap(afsVariants)
          val (mavenVariants, mavenLibs) = mavenDeps.partition(_.variant.isDefined)
          val mavenVariantsMap = mavenVariants.groupBy(_.variant.map(_.name))
          val mappedVariants = mavenVariantsMap.to(Seq).map { case (variantName, mavenDeps) =>
            afsVariantMap.get(variantName) -> mavenDeps
          }
          val mapped = MultiSourceDependency(name, Some(afs), mavenLibs, line) +: mappedVariants
            .collect { case (Some(afs), mavens) =>
              MultiSourceDependency(name, Some(afs), mavens, line)
            }
            .to(Seq)
          val notMapped = getMavenVariants(
            mappedVariants.collect { case (None, mavens) => mavens }.flatten.to(Seq)
          ) // variants only be used for forced version
          Success(notMapped ++ mapped)
        }
      case None => getMavenDependencies
    }

  }

  private def toMap(libs: Seq[DependencyDefinition]): Map[Option[String], DependencyDefinition] =
    libs.groupBy(_.variant.map(_.name)).map {
      case (k, Seq(v)) => k -> v
      case (k, vs) =>
        throw new IllegalArgumentException(s"Multiple definitions for variant $k: ${vs.map(_.key).mkString(", ")}")
    }
}
