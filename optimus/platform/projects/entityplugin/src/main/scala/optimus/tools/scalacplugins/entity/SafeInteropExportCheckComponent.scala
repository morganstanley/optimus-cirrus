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

import optimus.tools.scalacplugins.entity.reporter.OptimusErrors

import scala.tools.nsc.Phase

class SafeInteropExportCheckComponent(val plugin: EntityPlugin, val phaseInfo: OptimusPhaseInfo)
    extends EntityPluginComponent
    with TypedUtils
    with WithOptimusPhase {
  import global._

  def newPhase(_prev: Phase) = new SafeExportCheck(_prev)

  class SafeExportCheck(prev: Phase) extends EntityPluginStdPhase(prev) {
    override def name = phaseInfo.phaseName

    def apply0(unit: CompilationUnit) = new ExportTraverser(unit).traverse(unit.body)
  }

  private def isExported(defn: Tree) = defn.symbol.hasAnnotation(ExportedAnnotation)

  /**
   * Safe exports are @node/@async defs in module i.e. scala object (package objects too), which have serialisable
   * parameters and return types The def must also not be ambiguous for the reflection to work i.e. there should be no
   * overloaded method of the same name and the exports can only be defined on top level scala modules, as inner classes
   * names are mangled and virtually impossible to lookup using standard java reflection. Finally we allow at most one
   * parameter to be a ProgressReporter instead of a serialisable type
   */
  private def validateExport(
      module: ModuleDef,
      defn: ValOrDefDef,
      tpt: Tree,
      tparams: List[TypeDef],
      vparamss: List[List[ValDef]],
      unit: CompilationUnit): Unit = {
    if (!defn.symbol.hasAnnotation(NodeAnnotation) && !defn.symbol.hasAnnotation(AsyncAnnotation))
      alarm(OptimusErrors.EXPORT_NON_NODE_NON_ASYNC, defn.pos, defn.name)

    if (!(defn.mods.isPublic))
      alarm(OptimusErrors.EXPORT_PRIVATE, defn.pos, defn.name)

    var seenProgressReporter = false
    mforeach(vparamss) { param =>
      val isSerialisable = this.isSerialisable(param.tpt.tpe)
      val isProgressReporter = param.tpt.tpe.typeSymbol.isSubClass(ProgressReporter)

      if (isProgressReporter && seenProgressReporter)
        alarm(OptimusErrors.EXPORT_DUPLICATE_PARAM, defn.pos, defn.name, param.name.toString.trim)

      if (!(isSerialisable || isProgressReporter))
        alarm(OptimusErrors.EXPORT_WRONG_PARAM, defn.pos, defn.name, param.name.toString.trim, param.tpt.tpe)

      if (param.mods.hasDefault)
        alarm(OptimusErrors.EXPORT_DEFAULT_ARG, param.pos, defn.name, param.name.toString.trim)

      seenProgressReporter ||= isProgressReporter
    }

    if (!isSerialisable(tpt.tpe))
      alarm(OptimusErrors.EXPORT_WRONG_RETRUN_TYPE, defn.pos, defn.name, tpt)
  }

  private def isSerialisable(t: Type): Boolean = {
    t.typeSymbol.isSerializable || t.typeSymbol.isPrimitiveValueClass
  }

  /*
   * This traverser delegates to two sub traversers as a means to track state; one looks for valid exports inside a package
   * the other reports bad exports in any other location
   */
  private class ExportTraverser(val unit: CompilationUnit) extends Traverser {

    def reportBadlyLocated(defn: NameTree) =
      alarm(OptimusErrors.EXPORT_WRONG_LOCATION, defn.pos, defn.name)

    def reportCantExportClasses(defn: ClassDef) =
      alarm(OptimusErrors.EXPORT_CLASSES, defn.pos, defn.name)

    def reportCantExportVal(defn: ValDef) =
      alarm(OptimusErrors.EXPORT_VAL_VAR_PARAM, defn.pos, defn.name.toString.trim)

    def checkModuleDefAndTraverse(modDef: ModuleDef) = {
      if (isExported(modDef)) {
        alarm(OptimusErrors.EXPORT_CLASSES, modDef.pos, modDef.name)
      }
      new InModuleTraverser(modDef).traverseTrees(modDef.impl.body)
    }

    override def traverse(tree: Tree): Unit = {
      tree match {
        case PackageDef(_, list) =>
          list.foreach {
            case pack @ PackageDef(_, _) =>
              traverse(pack)

            case mod @ ModuleDef(_, _, _) =>
              checkModuleDefAndTraverse(mod)

            case t @ _ =>
              OutsideModuleTraverser.traverse(t)
          }

        case _ =>
          OutsideModuleTraverser.traverse(tree)
      }
    }

    private class InModuleTraverser(module: ModuleDef) extends Traverser {
      override def traverse(tree: Tree): Unit =
        tree match {
          case mod @ ModuleDef(_, _, impl) =>
            OutsideModuleTraverser.traverseTrees(impl.body)

          case defn @ ValDef(_, _, _, _) if isExported(defn) =>
            reportCantExportVal(defn)
            OutsideModuleTraverser.traverse(defn.rhs)

          case defn @ DefDef(_, _, tparams, vparamss, tpt, body) if isExported(defn) =>
            validateExport(module, defn, tpt, tparams, vparamss, unit)
            OutsideModuleTraverser.traverse(body)

          case _ =>
            OutsideModuleTraverser.traverse(tree)
        }
    }

    // All exports will be invalid
    object OutsideModuleTraverser extends Traverser {
      override def traverse(tree: Tree): Unit = {
        tree match {
          case valDef @ ValDef(_, _, _, _) if isExported(valDef) =>
            reportCantExportVal(valDef)

          case defdef @ DefDef(_, _, _, _, _, _) if isExported(defdef) =>
            reportBadlyLocated(defdef)

          case classDef @ ClassDef(_, _, _, _) if isExported(classDef) =>
            reportCantExportClasses(classDef)

          case _ =>
        }

        super.traverse(tree)
      }
    }
  }
}
