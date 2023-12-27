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
package optimus.buildtool.app

import msjava.slf4jutils.scalalog.Logger
import msjava.slf4jutils.scalalog.getLogger
import optimus.buildtool.config.ScopeId
import optimus.buildtool.files.Directory
import optimus.platform._
import optimus.buildtool.format.FreezerStructure
import optimus.buildtool.format.MischiefArgs
import optimus.buildtool.format.MischiefStructure
import optimus.buildtool.format.ObtFile
import optimus.buildtool.format.Result
import optimus.buildtool.trace.ObtTrace
import optimus.buildtool.utils.Hashing

// Mischief is an OBT special mode that modify compilation in potentially non-RT ways. For example, we can deliberately
// under- and over-compile, or add new scalac arguments not reflected in the final produced jars.
//
// It is called "mischief" because we are using it to play tricks on the build tool.

class MischiefLoader(workspaceSrc: Directory) {
  private val loader = ObtFile.FilesystemLoader(workspaceSrc)
  private def loadConfig[A](load: ObtFile.Loader => Result[A]): A = {
    val result = load(loader)
    result.problems.foreach { msg =>
      ObtTrace.warn(msg.toString)
    }
    result.get
  }

  /**
   * Check if mischief is active by reading mischief.obt.
   */
  def on(): Option[Boolean] = loadConfig(MischiefStructure.checkIfActive)
  def loadFreezer(): FreezerStructure = loadConfig(FreezerStructure.load)
  def loadMischief(): MischiefStructure = loadConfig(MischiefStructure.load)
}

object MischiefLoader {
  protected val log: Logger = getLogger(this)

  private def warn(msg: String): Unit = {
    // display intellij bsp msg
    ObtTrace.warn(msg)
    // display msg in terminal
    log.warn(msg)
  }

  def logNoMischief(hasFile: Boolean): Unit = {
    if (hasFile) MischiefLoader.warn("mischief.obt file exists, but active = false. Compilation will proceed normally.")
  }

  def logYesMischief(freezer: FreezerStructure, mischief: MischiefStructure): Unit = {
    MischiefLoader.warn("mischief.obt file exists!")
    log(freezer)
    log(mischief)
  }

  private def log(conf: FreezerStructure): Unit = {
    val notFrozen =
      if (conf.compile.isEmpty) "   Everything is frozen."
      else {
        s"   Not frozen:\n     ${conf.fpStrings.mkString("\n     ")}"
      }
    val begin = if (conf.active) {
      s"""|Freezer cache ACTIVE!
          |   OBT will proceed ABNORMALLY
          |     - installed jars could be INCORRECT
          |     - compilation might NO LONGER BE RT
          |${notFrozen}
          |""".stripMargin
    } else {
      """|Freezer cache INACTIVE!
         |   OBT will proceed normally
         |""".stripMargin
    }
    val end = s"""   Results will ${if (conf.save) "" else "NOT "}be saved for frozen compilations."""
    warn(begin ++ end)
  }

  private def log(conf: MischiefStructure): Unit = {
    if (conf.tricks.isEmpty) {
      warn("Mischief is INACTIVE.")
    } else {
      warn(s"Mischief is ACTIVE for scopes: ${conf.tricks.keys.mkString(", ")}")
    }
  }
}

@entity class MischiefOptions(
    val loader: MischiefLoader,
    private val initiallyActive: Boolean,
    private val mischiefStructure: MischiefStructure,
    private val freezerStructure: FreezerStructure) {

  @node(tweak = false) def mischief(scope: ScopeId): Option[MischiefArgs] =
    if (globallyActive) mischiefTweaks(scope) else None
  @node(tweak = false) def freezer: Option[FreezerStructure] =
    if (globallyActive) Some(freezerTweak) else None

  // Note that we don't address MischiefStructure directly. This is done so that we minimize the amount of invalidations
  // due to to calls that don't actually need the whole MischiefStructure but only one of its scope-resolved components.
  @node(tweak = true) private def mischiefTweaks(scope: ScopeId): Option[MischiefArgs] =
    mischiefStructure.tricks.get(scope)
  @node(tweak = true) private def mischiefScopes = mischiefStructure.tricks.keySet
  @node(tweak = true) private def freezerTweak: FreezerStructure = freezerStructure
  @node(tweak = true) private def globallyActive: Boolean = initiallyActive

  /**
   * Get content of mischief.obt file as tweaks. NOT RT.
   */
  @async def configAsTweaks(): Seq[Tweak] = {
    val (hasFile, active) = loader.on() match {
      case None    => (false, false) // no mischief file
      case Some(t) => (true, t) // has a mischief file
    }

    val activationTweaks = if (active != globallyActive) {
      Tweaks.byValue(this.globallyActive := active)
    } else Seq.empty

    activationTweaks ++ {
      if (active) {
        val newFreezer = loader.loadFreezer()
        val newMischief = loader.loadMischief()
        MischiefLoader.logYesMischief(newFreezer, newMischief)

        // mischief scopes that we have to clear
        val clearList = mischiefScopes
          .filter(scope => !newMischief.tricks.contains(scope))
          .flatMap(scope => Tweaks.byValue(mischiefTweaks(scope) := None))

        // mischief scopes that we apply
        val updateList = newMischief.tricks.flatMap { case (scope, args) =>
          Tweaks.byValue(mischiefTweaks(scope) := Some(args))
        }

        // new scopes
        val newMischiefScopes = Tweaks.byValue(mischiefScopes := newMischief.tricks.keySet)

        Tweaks.byValue(this.freezerTweak := newFreezer) ++ clearList ++ updateList ++ newMischiefScopes
      } else {
        MischiefLoader.logNoMischief(hasFile)
        Seq.empty
      }
    }
  }

  @node def freezerHash: Option[String] =
    if (globallyActive && freezerTweak.active) Some(Hashing.hashStrings(freezerTweak.fpStrings)) else None
}

object MischiefOptions {
  def load(workspaceSrcRoot: Directory): MischiefOptions = {
    val loader = new MischiefLoader(workspaceSrcRoot)
    val (hasFile, active) = loader.on() match {
      case None    => (false, false)
      case Some(t) => (true, t)
    }

    if (active) {
      val mischief = loader.loadMischief()
      val freezer = loader.loadFreezer()
      MischiefLoader.logYesMischief(freezer, mischief)
      MischiefOptions(loader, true, mischief, freezer)
    } else {
      MischiefLoader.logNoMischief(hasFile)
      MischiefOptions(loader, false, MischiefStructure.Empty, FreezerStructure.NoFreezerGroup)
    }
  }
}
