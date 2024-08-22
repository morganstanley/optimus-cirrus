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

import java.io.PrintWriter
import java.io.StringWriter
import optimus.tools.scalacplugins.entity.provider.PluginProvider
import optimus.tools.scalacplugins.entity.provider.PropertyInfoProvider
import optimus.tools.scalacplugins.entity.staged.scalaVersionRange

import scala.tools.nsc.plugins.Plugin
import scala.collection.mutable
import scala.reflect.internal.util.SourceFile
import scala.tools.nsc.Global
import scala.tools.nsc.plugins.OutputFileWriter
import scala.tools.nsc.plugins.PluginComponent
import scala.tools.nsc.reporters.FilteringReporter

class EntityPluginData(global: Global, val settings: EntitySettings) extends PluginData(global) {
  def configure(): Unit = configureBasic()
}

class EntityPlugin(val global: Global) extends Plugin {
  import global._
  val name = "entity"
  val description = "Optimus platform entity and calcgraph plugin"

  def reporter: FilteringReporter = global.reporter

  def parseOption(option: String, name: String, default: Boolean): Boolean = option.substring(name.length) match {
    case "false" | "disable" | "0" | "off" => false // Would be hard not to guess this one
    case "true" | "enable" | "1" | "on"    => true
    case _                                 => default
  }

  def parseInt(option: String, name: String, default: Int): RunId =
    try {
      option.substring(name.length).toInt
    } catch {
      case t: Throwable => default
    }

  val settings = new EntitySettings

  override def init(options: List[String], error: String => Unit): Boolean = {
    try {
      import EntitySettings.OptionNames._

      val es = settings

      for (option <- options) {
        if (option.startsWith(wartsName))
          es.warts = StagingPlugin.parseStringList(option, wartsName)
        else if (option.startsWith(enableStagingName))
          es.enableStaging = parseOption(option, enableStagingName, es.enableStaging)
        else if (option.startsWith(disableExportInfoName))
          es.disableExportInfo = parseOption(option, disableExportInfoName, es.disableExportInfo)
        else if (option.startsWith(posValidateName))
          es.posValidate = parseOption(option, posValidateName, es.posValidate)
        else if (option.startsWith(enableEntityRelationName))
          es.entityRelation = parseOption(option, enableEntityRelationName, es.entityRelation)
        else if (option.startsWith(autoparName))
          moduleLangOptions :+= LanguageOption.autopar
        else if (option.startsWith(loomName))
          es.loom = parseOption(option, loomName, es.loom)
        else StagingPlugin.processCommonOption(option, pluginData, error)
      }

      pluginData.configure()
      pluginProviders.foreach(_.macroPlugins.foreach(analyzer.addMacroPlugin))
      pluginProviders.foreach(_.analyzerPlugins.foreach(analyzer.addAnalyzerPlugin))
      global.settings.async.value = true
      true
    } catch {
      case e: Exception =>
        val sw = new StringWriter()
        val pw = new PrintWriter(sw)
        e.printStackTrace(pw)
        println(sw)
        global.reporter.echo(NoPosition, sw.toString)
        throw e
    }
  }

  override val optionsHelp: Option[String] = Some(
    """|  -P:entity:opt:LEVEL              set optimization level (default = 0)
      "|  -P:entity:enableStaging:false    disable/enable staging phase in entity plugin
      "|  -P:entity:entityRelation:false   disable/enable writing entity relationship metadata to a file
      """.stripMargin
  )

  val pluginData = new EntityPluginData(global, settings)

  lazy val pluginProviders: List[PluginProvider[global.type]] = List(
    new PropertyInfoProvider[global.type](pluginData, global),
    new AsyncDefaultGetterComponent[global.type](pluginData, global)
  )

  // Anything that runs prior to the typer and thus produces APIs that should be processed by e.g. scaladoc
  lazy val frontendComponents: List[WithOptimusPhase] = // Phases to run before typer
    new StagingComponent(
      pluginData,
      global,
      StagingPhase.STAGING,
      settings.enableStaging) :: // Before optimus_adjustast
      new CodingStandardsComponent(pluginData, global, StagingPhase.STANDARDS) ::
      new AnnotatingComponent(pluginData, global, StagingPhase.ANNOTATING) ::
      new AdjustASTComponent(this, OptimusPhases.ADJUST_AST) :: // Before namer
      Nil

  lazy val backendComponents: List[PluginComponent] = frontendComponents ::: pluginProviders.flatMap(_.components) ::: (
    // ... typer ...
    new PostTyperCodingStandardsComponent(pluginData, global, StagingPhase.POST_TYPER_STANDARDS) ::
      new rewrite.RewriteComponent(pluginData, global, StagingPhase.REWRITE) ::
      new ClassifierComponent(this, OptimusPhases.CLASSIFIER) ::
      new GeneralAPICheckComponent(pluginData, global, StagingPhase.GENERAL_API_CHECK) ::
      new OptimusAPICheckComponent(this, OptimusPhases.APICHECK) :: // typer .. _refchecks, _puritycheck
      new LocationTagComponent(this, OptimusPhases.GENERATE_LOCATION_TAG) ::
      new AutoAsyncComponent(this, OptimusPhases.AUTOASYNC) ::
      new SafeInteropExportCheckComponent(this, OptimusPhases.SAFE_EXPORT_CHECK) ::
      new OptimusRefchecksComponent(this, OptimusPhases.REF_CHECKS) ::
      new OptimusDALRefChecks(this, OptimusPhases.DAL_REF_CHECKS) ::
      new RedirectAccessorsComponent(this, OptimusPhases.VAL_ACCESSORS) ::
      new EmbeddableComponent(this, OptimusPhases.EMBEDDABLE) :: // After optimus_valaccessors
      new EntityInfoComponent(this, OptimusPhases.ENTITY_INFO) ::
      new GenerateNodeMethodsComponent(this, OptimusPhases.GENERATE_NODE_METHODS) ::
      new NodeLiftComponent(this, OptimusPhases.NODE_LIFT) :: // Before optimus_async
      // ... patmat ...
      // TODO (OPTIMUS-0000): XASYNC Should we defer this until later in the pipeline? The hard constraint will be that
      //             it runs prior to async (which follows posterasure). If it moves after uncurry, it could
      //             be simplified with the assumption that Apply isn't nested. If it runs after erasure
      //             it needn't concern itself with TypeApply. etc.
      new AsyncGraphComponent(this, OptimusPhases.ASYNC_GRAPH) :: // After optimus_nodelift Before refcheck/selectiveanf
      // ... superaccessors
      // ... refcheck ...

      // ... uncurry ...
      new CaptureByValueComponent(this, OptimusPhases.CAPTURE_BY_VALUE) ::
      new OptimusConstructorsComponent(
        this,
        OptimusPhases.OPTIMUS_CONSTRUCTORS
      ) :: // Before cleanup, after mixin, constructors
      new PositionValidateComponent(this, OptimusPhases.POSITION) :: // The latest phase: check all trees have positions
      new OptimusExportInfoComponent(this, OptimusPhases.EXPORTINFO) ::
      Nil
  )

  // We reflectively update the compiler phase order to move the pickling phase earlier
  // This is only until we upgrade to a version of scalac that contains the actual change in the compiler
  // TODO (OPTIMUS-24229): Remove reflective re-ordering after upgrade to Scala 2.13.x is fully complete
  if (scalaVersionRange("2.13:"): @staged) {
    // don't mess with the built in phase ordering
  } else {
    def setRunsAfter(phase: AnyRef, runsAfter: String): Unit = {
      val runsAfterField = phase.getClass.getDeclaredField("runsAfter")
      runsAfterField.setAccessible(true)
      runsAfterField.set(phase, List(runsAfter))
    }
    setRunsAfter(global.superAccessors, "typer")
    setRunsAfter(global.patmat, "pickler")
    setRunsAfter(global.uncurry, "refchecks")
  }

  // The Global.forScaladoc is deprecated in 2.11, we check the global type in 2.11
  private lazy val isForScalaDoc = global.isInstanceOf[scala.tools.nsc.doc.ScaladocGlobal]

  override lazy val components: List[PluginComponent] = if (isForScalaDoc) frontendComponents else backendComponents

  def applyPreNamerTranforms(tree: Tree): global.Tree = {
    import scala.tools.nsc.transform.Transform
    val preNamerPhases = components.filter(_.runsBefore.contains("namer")).collect {
      // This is a protected method.  Can we fake a CompilationUnit?
      case t: Transform => t.asInstanceOf[Transform { def newTransformer(unit: CompilationUnit): Transformer }]
    }

    val compiler = global.asInstanceOf[{
      def verify(tree: Tree): Unit
    }]

    import language.reflectiveCalls
    val x = preNamerPhases.foldLeft(tree) { (transformedTree, phase) =>
      val newTree = phase.newTransformer(NoCompilationUnit).transform(transformedTree)
      compiler.verify(newTree)
      newTree
    }

    x
  }

  private val additionalOutputs: mutable.Map[String, Array[Byte]] = mutable.HashMap()

  def addAdditionalOutput(relativeName: String, content: Array[Byte]): Unit = {
    require(additionalOutputs.put(relativeName, content).isEmpty, s"additional output $relativeName was overwritten!")
  }

  override def writeAdditionalOutputs(writer: OutputFileWriter): Unit = {
    val outDirOrJar = global.settings.outputDirs.getSingleOutput.get
    additionalOutputs.foreach { case (name, content) =>
      writer.writeFile(name, content, outDirOrJar)
    }
    additionalOutputs.clear()
  }

  // Record optimus language options for a given source file
  var moduleLangOptions = Seq.empty[LanguageOption]
  var fileLangOptions = Map.empty[SourceFile, Seq[LanguageOption]]
  def addLanguageOption(s: SourceFile)(f: => LanguageOption): Unit =
    fileLangOptions += s -> (f +: fileLangOptions.getOrElse(s, Nil))
  def hasLanguageOption(s: SourceFile, option: LanguageOption): Boolean =
    moduleLangOptions.contains(option) || fileLangOptions.get(s).exists(_.contains(option))
}

sealed trait LanguageOption
object LanguageOption {
  case object autopar extends LanguageOption
}
