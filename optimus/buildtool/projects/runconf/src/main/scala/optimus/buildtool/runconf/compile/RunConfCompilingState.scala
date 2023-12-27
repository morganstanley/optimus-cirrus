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
package optimus.buildtool.runconf.compile

import java.nio.file.Path

import com.typesafe.config.Config
import optimus.buildtool.config.ParentId
import optimus.buildtool.runconf.HasScopedName
import optimus.buildtool.runconf.RunConf
import optimus.buildtool.runconf.ScopedName
import optimus.buildtool.runconf.Template

import scala.collection.immutable.Seq
import scala.collection.mutable

private[compile] object RunConfCompilingState {

  def typechecked(
      block: Block,
      scopedName: ScopedName,
      untypedProperties: Map[String, Any],
      config: Config,
      runConf: UnresolvedRunConf,
      file: Path,
      sourceRoot: Option[Path]
  ): RunConfCompilingState =
    new RunConfCompilingState(
      block,
      scopedName.id,
      scopedName.name,
      untypedProperties,
      Some(runConf),
      initialProblems = Seq.empty,
      config,
      file,
      sourceRoot)

  def incorrect(
      block: Block,
      scopedName: ScopedName,
      problems: Seq[Problem],
      config: Config,
      file: Path,
      sourceRoot: Option[Path],
      untypedProperties: Map[String, Any] = Map.empty
  ): RunConfCompilingState = {
    val state = new RunConfCompilingState(
      block,
      scopedName.id,
      scopedName.name,
      untypedProperties,
      initialRunConf = None,
      problems,
      config,
      file,
      sourceRoot)
    state.resolvedAsIncorrect()
    state
  }

}

trait ImmutableRunConfCompilingState extends HasScopedName {
  def block: Block
  def id: ParentId
  def name: String
  def untypedProperties: Map[String, Any]
  def runConf: UnresolvedRunConf
  def runConfWithoutParents: UnresolvedRunConf
  def hasErrors: Boolean
  def allProblems: Seq[Problem]
  def resolvedRunConf: Option[RunConf]
  def resolvedTemplate: Option[Template]
  def isResolved: Boolean
  def isTyped: Boolean
  def isUsed: Boolean
  def parents: Seq[RunConfCompilingState]
}

trait ResolvedRunConfCompilingState extends ImmutableRunConfCompilingState {
  def transformRunConf(transform: PartialFunction[RunConf, RunConf]): Unit
  def transformTemplate(transform: Template => Template): Unit
  def reportWarning: Reporter
}

trait UnresolvedRunConfCompilingState extends ImmutableRunConfCompilingState {
  def reportError: Reporter
  def reportWarning: Reporter
}

private[compile] class RunConfCompilingState(
    val block: Block,
    val id: ParentId,
    val name: String,
    val untypedProperties: Map[String, Any],
    initialRunConf: Option[UnresolvedRunConf],
    initialProblems: Seq[Problem],
    config: Config,
    file: Path,
    sourceRoot: Option[Path]
) extends ResolvedRunConfCompilingState
    with UnresolvedRunConfCompilingState {

  private val reporter: Reporter = Reporter(block, ScopedName(id, name), config, file, sourceRoot, initialProblems)

  private val warnReporter: Reporter = reporter.withLevel(Level.Warn)

  def reportError: Reporter = reporter

  def reportWarning: Reporter = warnReporter

  def hasErrors: Boolean = reporter.hasErrors

  def allProblems: Seq[Problem] = reporter.problems

  def runConfWithoutParents: UnresolvedRunConf = initialRunConf.get

  private var _runConf: Option[UnresolvedRunConf] = initialRunConf

  def runConf: UnresolvedRunConf = _runConf.get

  private var _resolvedRunConf = Option.empty[RunConf]

  private var _resolvedTemplate = Option.empty[Template]

  private var _isResolved = false

  def resolvedRunConf: Option[RunConf] = _resolvedRunConf

  def resolvedTemplate: Option[Template] = _resolvedTemplate

  def resolvedAsIncorrect(): Unit = resolvedAs(None)

  def resolvedAsTemplate(template: Template): Unit = {
    resolvedAs(None)
    _resolvedTemplate = Some(template)
  }

  def resolvedAsRunConf(runConf: RunConf): Unit = resolvedAs(Some(runConf))

  private def resolvedAs(resolved: Option[RunConf]): Unit = {
    _resolvedRunConf = resolved
    _isResolved = true
  }

  private var _used = false

  def markAsUsed(): Unit = _used = true

  def isUsed: Boolean = _used

  def transformRunConf(transform: PartialFunction[RunConf, RunConf]): Unit = {
    resolvedRunConf.foreach { conf =>
      resolvedAsRunConf(transform.applyOrElse(conf, identity[RunConf]))
    }
  }

  def transformTemplate(transform: Template => Template): Unit = {
    resolvedTemplate.foreach { template =>
      resolvedAsTemplate(transform(template))
    }
  }

  def isResolved: Boolean = _isResolved

  def isTyped: Boolean = _runConf.isDefined

  private val _parents = mutable.Buffer[RunConfCompilingState]()

  def parents: Seq[RunConfCompilingState] = _parents.toList

  def appendParent(parent: RunConfCompilingState): Unit = _parents.append(parent)

  def prependParents(parents: Seq[RunConfCompilingState]): Unit = _parents.prependAll(parents)

  def update(updated: UnresolvedRunConf): Unit = {
    _runConf = Some(updated)
  }

  def validateStrings(errorMsg: String)(isInvalid: String => Boolean): Unit = {
    val hasInvalidEnv = runConf.env.values.exists {
      case Left(str)   => isInvalid(str)
      case Right(strs) => strs.exists(isInvalid)
    }

    if (hasInvalidEnv) {
      reportError.atValue(RunConfSupport.names.env, errorMsg)
    }
    if (runConf.javaOpts.exists(isInvalid)) {
      reportError.atValue(RunConfSupport.names.javaOpts, errorMsg)
    }
  }

  override def equals(obj: Any): Boolean = {
    obj match {
      case s: RunConfCompilingState => s.id == this.id && s.name == this.name
      case _                        => false
    }
  }

  override def hashCode(): Int = (id, name).hashCode
}
