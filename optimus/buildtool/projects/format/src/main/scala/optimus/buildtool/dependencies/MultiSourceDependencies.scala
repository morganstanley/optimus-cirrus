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
import optimus.buildtool.format.DependencyConfig
import optimus.buildtool.format.Result
import optimus.buildtool.format.Failure
import optimus.buildtool.format.Success

import scala.collection.compat._

final case class MultiSourceDependencies(all: Seq[MultiSourceDependency], substitutions: Seq[Substitution]) {
  val preferred: Seq[DependencyDefinition] = all.flatMap(_.definitions.preferred)
  val afsOnly: Seq[DependencyDefinition] = all.map(_.definitions).collect { case AfsOnly(a) => a }
  val unmapped: Seq[DependencyDefinition] = all.map(_.definitions).collect { case Unmapped(m) => m }
  val mavenOnly: Seq[DependencyDefinition] = all.map(_.definitions).collect { case MavenOnly(m) => m }
  val mapped: Seq[Mapped] = all.map(_.definitions).collect { case m: Mapped => m }

  def ++(to: MultiSourceDependencies): MultiSourceDependencies =
    MultiSourceDependencies(
      all = (this.all ++ to.all).distinct,
      substitutions = (this.substitutions ++ to.substitutions).distinct)

  def +++(to: Iterable[MultiSourceDependencies]): MultiSourceDependencies = MultiSourceDependencies(
    all = (this.all ++ to.flatMap(_.all)).distinct,
    substitutions = (this.substitutions ++ to.flatMap(_.substitutions)).distinct)
}

sealed trait Definitions {
  def all: Seq[DependencyDefinition] = this match {
    case AfsOnly(a)   => Seq(a)
    case MavenOnly(m) => Seq(m)
    case Unmapped(m)  => Seq(m)
    case Mapped(a, m) => a +: m
  }
  def preferred: Seq[DependencyDefinition] = this match {
    case AfsOnly(a)   => Seq(a)
    case MavenOnly(m) => Seq(m)
    case Unmapped(m)  => Seq(m)
    case Mapped(_, m) => m
  }
}
final case class AfsOnly(afs: DependencyDefinition) extends Definitions
final case class MavenOnly(maven: DependencyDefinition) extends Definitions
final case class Unmapped(maven: DependencyDefinition) extends Definitions
final case class Mapped(afs: DependencyDefinition, maven: Seq[DependencyDefinition]) extends Definitions

final case class MultiSourceDependency(
    name: Option[String],
    definitions: Definitions,
    file: DependencyConfig,
    line: Int,
    ivyConfig: Boolean = false
) extends OrderedElement[DependencyId] {

  def afs: Option[DependencyDefinition] = definitions match {
    case AfsOnly(a)   => Some(a)
    case Mapped(a, _) => Some(a)
    case _            => None
  }

  def maven: Seq[DependencyDefinition] = definitions match {
    case MavenOnly(m) => Seq(m)
    case Unmapped(m)  => Seq(m)
    case Mapped(_, m) => m
    case _            => Nil
  }

  private def nameStr: String = name.getOrElse {
    definitions match {
      case AfsOnly(a)   => a.key
      case MavenOnly(m) => m.key
      case Unmapped(m)  => m.key
      case Mapped(a, _) => a.key
    }
  }

  override def id: DependencyId = DependencyId(group = "obt.jvm.loaded", name = nameStr)
}

object MultiSourceDependency {
  private[buildtool] val MultipleAfsError = "only one afs dependency should be defined!"
  private[buildtool] val NoAfsVersionError = "afs version must be defined if no maven mapping exists!"
  private[buildtool] def emptyDepError(name: String) =
    s"'$name' has no maven definition or explicit runtime ivy configuration!"

  def expandDependencyDefinitions(
      obtFile: DependencyConfig,
      confValue: ConfigValue,
      name: String,
      afsDep: Option[DependencyDefinition],
      afsVariants: Seq[DependencyDefinition],
      mavenDeps: Seq[DependencyDefinition],
      maybeOverriddenRuntime: Option[MultiSourceDependency],
      line: Int): Result[Seq[MultiSourceDependency]] = {
    def emptyError = Failure(Seq(obtFile.errorAt(confValue, emptyDepError(name))))

    val isIvyConfig = afsDep.exists(_.variant.exists(_.configurationOnly))

    def expandedName(d: DependencyDefinition, nameSuffix: String = ""): String = {
      val suffix = if (nameSuffix.nonEmpty) s".$nameSuffix" else ""
      d.variant match {
        case Some(variant) => s"$name.variant.${variant.name}$suffix"
        case None          => s"$name$suffix"
      }
    }

    def getMavenVariants(mavenVariants: Seq[(DependencyDefinition, String, Int)]) =
      mavenVariants.map { case (d, nameSuffix, line) =>
        MultiSourceDependency(Some(expandedName(d, nameSuffix)), MavenOnly(d), obtFile, line, isIvyConfig)
      }

    def getMavenDependencies =
      mavenDeps match { // maven only
        case _ if mavenDeps.isEmpty => emptyError
        case mavenCfgs
            if mavenDeps.map(d => d.group + d.name + d.version).distinct.size == 1 &&
              mavenDeps.map(_.configuration).toSet.size != 1 => // multiple configurations defined
          Success(mavenCfgs.map { d =>
            val nameWithConfig =
              if (d.configuration.nonEmpty) // not default config
                s"$name.${d.configuration}" // for example: scala.libraries
              else name
            MultiSourceDependency(Some(nameWithConfig), MavenOnly(d), obtFile, line, isIvyConfig)
          })
        case mavenLibsWithVariants
            if mavenDeps.map(d => d.group + d.name).distinct.size == 1 && mavenDeps.count(
              _.variant.isEmpty) == 1 && mavenDeps.exists(_.variant.isDefined) =>
          Success(getMavenVariants(mavenLibsWithVariants.map(m => (m, "", m.line))))
        case Seq(m) => Success(Seq(MultiSourceDependency(Some(name), MavenOnly(m), obtFile, line, isIvyConfig)))
        case _ => // multiple maven deps defined, without AFS mapping
          Failure(
            Seq(obtFile
              .errorAt(confValue, s"Only single maven dependencies are permitted when AFS is not defined for '$name'")))

      }

    afsDep match {
      case Some(afs) =>
        // Remove the comments after next obt release.
        if (maybeOverriddenRuntime.isDefined /* && mavenDeps.nonEmpty */ ) Success(Nil)
        /* Failure(Seq(obtFile.errorAt(confValue, "There is a runtime override present in `ivyConfigurations` at line ${maybeOverridenRuntime.get.line}")), please remove this line and add this dependency inside ivyConfigurations along with including it in `runtime.extends` list */
        else if (mavenDeps.isEmpty && afs.noVersion)
          Failure(Seq(obtFile.errorAt(confValue, NoAfsVersionError)))
        else if (mavenDeps.isEmpty) {
          val afsDeps = (afs +: afsVariants).map { dd =>
            MultiSourceDependency(Some(expandedName(dd)), AfsOnly(dd), obtFile, line, isIvyConfig)
          }
          Success(afsDeps)
        } else {
          val afsVariantMap = toMap(afsVariants)
          val (mavenVariants, mavenLibs) = mavenDeps.partition(_.variant.isDefined)
          val mavenVariantsMap = mavenVariants.groupBy(_.variant.map(_.name))
          val mappedVariants = mavenVariantsMap.to(Seq).map { case (variantName, mavenDeps) =>
            afsVariantMap.get(variantName) -> mavenDeps
          }
          val mapped =
            MultiSourceDependency(Some(name), Mapped(afs, mavenLibs), obtFile, line, isIvyConfig) +: mappedVariants
              .collect { case (Some(afs), mavens) =>
                MultiSourceDependency(Some(expandedName(afs)), Mapped(afs, mavens), obtFile, line, isIvyConfig)
              }
              .to(Seq)
          val notMapped = getMavenVariants(
            mappedVariants
              .collect {
                // for variants in configurations they need a name suffix to disambiguate them and to be ordered by the configuration, not the variant
                case (None, mavens) if afs.variant.exists(_.configurationOnly) => mavens.map(m => (m, m.key, afs.line))
                case (None, mavens)                                            => mavens.map(m => (m, "", m.line))
              }
              .flatten
              .to(Seq)
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
