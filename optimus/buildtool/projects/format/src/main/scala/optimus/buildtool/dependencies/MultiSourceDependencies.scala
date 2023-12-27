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
  val (mavenOnlyDeps, afsDefinedDeps) = loaded.partition(_.mavenOnly)
  val (afsOnlyDeps, multiSourceDeps) = afsDefinedDeps.partition(_.afsOnly)
  val mavenDefinedDeps = loaded.filter(_.maven.nonEmpty)

  def ++(to: MultiSourceDependencies): MultiSourceDependencies = MultiSourceDependencies(
    (this.loaded ++ to.loaded).distinct)
}

final case class MultiSourceDependency(
    name: String,
    afs: Option[DependencyDefinition],
    maven: Seq[DependencyDefinition],
    line: Int)
    extends OrderedElement {

  private def mavenDefinition: DependencyDefinition = maven match {
    case Seq(mavenAlone) => mavenAlone // for maven-only mixed mode definitions
    case multipleMaven if maven.size > 1 => // prevent user define multiple maven deps when afs{} is empty
      val linesStr = maven.map { d => s"'${d.key}' at line ${d.line}" }.mkString(", ")
      throw new IllegalArgumentException(
        s"[${MultiSourceDependenciesLoader.jvmPathStr}] '$name' without afs definition is not allowed to define multiple maven definitions: $linesStr")
    case _ => // both afs{} & maven{} are empty, we don't allow empty definition
      throw new IllegalArgumentException(
        s"[${MultiSourceDependenciesLoader.jvmPathStr}] '$name' at line $line can't be empty!")
  }

  def mavenOnly: Boolean = afs.isEmpty && maven.size == 1

  def afsOnly: Boolean = maven.isEmpty && afs.size == 1

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
  private[buildtool] def emptyDepError(name: String) = s"'$name' is empty!"

  private def getMavenVariant(afs: DependencyDefinition, maven: Seq[DependencyDefinition]): Seq[DependencyDefinition] =
    maven
      .filter { d => d.variant.map(_.name).getOrElse("") == afs.variant.map(_.name).getOrElse("afs") }

  def apply(
      obtFile: ObtFile,
      confValue: ConfigValue,
      name: String,
      afsDeps: Seq[DependencyDefinition],
      maven: Seq[DependencyDefinition],
      line: Int): Result[Seq[MultiSourceDependency]] = {
    val emptyError = Failure(Seq(obtFile.errorAt(confValue, emptyDepError(name))))

    if (afsDeps.isEmpty)
      maven match { // maven only
        case empty if maven.isEmpty => emptyError
        case _                      => Success(Seq(MultiSourceDependency(name, None, maven, line)))
      }
    else
      Result
        .sequence(afsDeps.map { afs =>
          afs match {
            case afsOnly if maven.isEmpty && afs.isDisabled => emptyError
            case afsVariant if afs.variant.isDefined =>
              val mavenVariant = getMavenVariant(afs, maven)
              if (mavenVariant.nonEmpty)
                Success(MultiSourceDependency(name, Some(afs), mavenVariant, line))
              else Failure(Seq(obtFile.errorAt(confValue, NoMavenVariantError(afs.key))))
            case _ =>
              if (afsDeps.filterNot(_.variant.isDefined).size == 1)
                Success(MultiSourceDependency(name, Some(afs), maven.filterNot(_.variant.isDefined), line))
              else Failure(Seq(obtFile.errorAt(confValue, MultipleAfsError)))
          }
        })
  }
}
