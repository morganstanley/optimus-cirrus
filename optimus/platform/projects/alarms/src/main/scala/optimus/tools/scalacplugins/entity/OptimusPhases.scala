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
package optimus.tools.scalacplugins.entity

import scala.reflect.internal.util.SourceFile
import scala.tools.nsc.Properties
import scala.tools.nsc.SubComponent

object OptimusPhases {

  // we don't have whole phase list here
  // use "-Xshow-phases" to display all the compile phases

  // Nb. Adding a phase that traverses the AST, doing negligible work at each node, does not
  // measurably affect our compilation times.
  object names {
    val parser = "parser" // scalac phase: parse source into ASTs, perform simple desugaring

    //    val optimus_staging = "optimus_staging"
    val optimus_adjustast = "optimus_adjustast"

    val namer = "namer" // scalac phase: resolve names, attach symbols to named trees

    val typer = "typer" // scalac phase: the meat and potatoes: type the trees

    val optimus_forwarding = StagingPhase.names.optimus_forwarding
    val optimus_classifier = "optimus_classifier"
    val optimus_apicheck = "optimus_apicheck"
    val optimus_entity_relationship = "optimus_entity_relationship"
    val optimus_refchecks = "optimus_refchecks"
    val optimus_puritycheck = "optimus_puritycheck"
    val optimus_valaccessors = "optimus_valaccessors"
    val optimus_embeddable = "optimus_embeddable"
    val optimus_entityinfo = "optimus_entityinfo"
    val optimus_cleanup = "optimus_cleanup"
    val optimus_nodelift = "optimus_nodelift"
    val optimus_generatenodemethods = "optimus_generatenodemethods"
    val optimus_asyncgraph = "optimus_asyncgraph"
    val optimus_autoasync = "optimus_autoasync"

    val patmat = "patmat" // scalac phase: translate match expressions
    val superaccessors = "superaccessors" // scalac phase: add super accessors in traits and nested classes
    val pickler = "pickler" // scalac phase: serialize symbol tables
    val refchecks = "refchecks" // scalac phase: reference/override checking, translate nested objects

    val extmethods = "extmethods"
    val uncurry = "uncurry" // uncurry, translate function values to anonymous classes

    val explicitouter = "explicitouter" // scalac phase: this refs to outer pointers",

    val mixin = "mixin" // scalac phase: mixin composition
    val fields = "fields" // scalac phase: synthesize accessors, fields (and bitmaps) for (lazy) vals and modules

    val optimus_constructors = "optimus_constructors"
    val optimus_addSIcheck = "optimus_addSIcheck"
    val optimus_safeInteropExportCheck = "optimus_safe_interop_export_check"
    val optimus_position = "optimus_position"
    val optimus_propertyinfo = "optimus_propertyinfo"

    val cleanup = "cleanup" // scalac phase: platform-specific cleanups, generate reflective calls
    val delambdafy = "delambdafy" // last phase jvm
    val lastPhaseBeforeJVM = delambdafy

    val optimus_export = "optimus_export"
    val optimus_capturebyvalue = "optimus_capturebyvalue"

    val jvm = "jvm" // scalac phase: write the class files
  }

  import names._
  import StagingPhase.names.optimus_staging

  def phaseBlock(names: String*): List[OptimusPhaseInfo] = {
    names
      .sliding(3)
      .map {
        case Seq(prev, curr, next) => OptimusPhaseInfo(curr, prev, next)
      }
      .toList
  }

  private val List(_, _ADJUST_AST) = phaseBlock(
    parser,
    optimus_staging,
    optimus_adjustast,
    namer
  )
  private val List(
    _FORWARDING,
    _CLASSIFIER,
    _ENTITY_RELATIONSHIP,
    _PROPERTY_INFO,
    _SAFE_EXPORT_CHECK,
    _APICHECK,
    _PURITY_CHECK,
    _VAL_ACCESSORS,
    _REF_CHECKS,
  ) = phaseBlock(
    typer,
    optimus_forwarding,
    optimus_classifier,
    optimus_entity_relationship,
    optimus_propertyinfo,
    optimus_safeInteropExportCheck,
    optimus_apicheck,
    optimus_puritycheck,
    optimus_valaccessors,
    optimus_refchecks,
    superaccessors
  )
  private val List(_EMBEDDABLE, _ENTITY_INFO, _AUTOASYNC) =
    phaseBlock(pickler, optimus_embeddable, optimus_entityinfo, optimus_autoasync, patmat)
  private val List(_GENERATE_NODE_METHODS, _NODE_LIFT) =
    phaseBlock(patmat, optimus_generatenodemethods, optimus_nodelift, refchecks)
  private val _ASYNC_GRAPH = OptimusPhaseInfo(optimus_asyncgraph, refchecks, explicitouter)
  private val List(_OPTIMUS_CONSTRUCTORS, _ADD_SICHECK, _POSITION) =
    phaseBlock(mixin, optimus_constructors, optimus_addSIcheck, optimus_position, cleanup)
  private val _EXPORTINFO = OptimusPhaseInfo(optimus_export, delambdafy, jvm)
  private val _CAPTURE_BY_VALUE = OptimusPhaseInfo(optimus_capturebyvalue, uncurry, fields)

  val ADJUST_AST = _ADJUST_AST
  val FORWARDING = _FORWARDING
  val CLASSIFIER = _CLASSIFIER
  val ENTITY_RELATIONSHIP = _ENTITY_RELATIONSHIP
  val PROPERTY_INFO = _PROPERTY_INFO
  val SAFE_EXPORT_CHECK = _SAFE_EXPORT_CHECK
  val APICHECK = _APICHECK
  val PURITY_CHECK = _PURITY_CHECK
  val REF_CHECKS = _REF_CHECKS
  val VAL_ACCESSORS = _VAL_ACCESSORS
  val EMBEDDABLE = _EMBEDDABLE
  val ENTITY_INFO = _ENTITY_INFO
  val AUTOASYNC = _AUTOASYNC
  val GENERATE_NODE_METHODS = _GENERATE_NODE_METHODS
  val NODE_LIFT = _NODE_LIFT
  val ASYNC_GRAPH = _ASYNC_GRAPH
  val OPTIMUS_CONSTRUCTORS = _OPTIMUS_CONSTRUCTORS
  val ADD_SICHECK = _ADD_SICHECK
  val POSITION = _POSITION
  val EXPORTINFO = _EXPORTINFO
  val CAPTURE_BY_VALUE = _CAPTURE_BY_VALUE
}

object PluginDataAccessFromReflection {
  // we have to access this reflectively as it is typically loaded in a different classloader
  // it is also essential that all of the passed types are simple types in the parent classloader
  // e.g. String, Int etc. Keep in step with trait PluginDataAccess below

  type PluginDataAccess = {
    def getConfiguredLevelRaw(
        alarmId: Int,
        alarmString: String,
        alarmLevel: String,
        positionSource: String,
        positionLine: Int,
        positionCol: Int,
        template: String): String
  }
}
trait PluginDataAccess {
  type Sourced = { def source: SourceFile }
  val pluginData: PluginData
  // we have to access this reflectively as it is typically loaded in a different classloader
  // it is also essential that all of the passed types are simple types in the parent classloader
  // e.g. String, Int etc. Keep in step with trait {{PluginDataAccessFromReflection.PluginDataAccess}} above
  def getConfiguredLevelRaw(
      alarmId: Int,
      alarmString: String,
      alarmLevel: String,
      positionSource: String,
      positionLine: Int,
      positionCol: Int,
      template: String): String =
    pluginData.alarmConfig.getConfiguredLevelRaw(
      alarmId,
      alarmString,
      alarmLevel,
      positionSource,
      positionLine,
      positionCol,
      template)
}

object ScalaVersionData {
  val scalaVersion = Properties.versionNumberString
}

trait WithOptimusPhase extends SubComponent with PluginDataAccess {
  val phaseInfo: OptimusPhaseInfo
  val phaseName: String = phaseInfo.phaseName
  val runsAfter = phaseInfo.runsAfter
  override val runsBefore = phaseInfo.runsBefore
}
