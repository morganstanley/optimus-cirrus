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
package optimus.entityplugin.pretyper

import optimus.tools.scalacplugins.entity.AdjustASTComponent

import scala.tools.nsc.symtab.Flags

trait ColumnarPriql { this: AdjustASTComponent =>
  import global._

  def createNamedColumnResult(entityName: TypeName, entityFieldToType: Map[TermName, String]): ClassDef = {
    val className = newTypeName("Column" + entityName.toString())
    // #1 new ColumnResult[ShapeType]{
    //    def fieldName = view.get("fieldName", classOf[String])
    //   }
    val clsTempBodyValDef =
      ValDef(Modifiers(Flags.PARAMACCESSOR), newTermName("view"), Ident(newTypeName("RelationColumnView")), EmptyTree)
    val clsTempBodyDefs: List[Tree] = entityFieldToType.iterator.map { f =>
      val classType = TypeApply(Ident(nme.classOf), List(Ident(newTypeName(f._2))))
      val applyArgs = List(Literal(Constant(f._1.toString)), classType)
      val view = Apply(Select(Ident(newTermName("view")), nme.get), applyArgs)

      DefDef(NoMods, f._1, Nil, List(Nil), TypeTree(), view)
    }.toList
    val clsTempParentsAppliedTypeTree = List(
      AppliedTypeTree(Ident(newTypeName("ColumnResult")), List(Ident(entityName))))
    val clsTemp = createTemplate(
      clsTempParentsAppliedTypeTree,
      noSelfType,
      NoMods,
      List(List(clsTempBodyValDef)),
      List(Nil),
      clsTempBodyDefs,
      NoPosition)
    ClassDef(Modifiers(Flags.FINAL), className /*tpnme.ANON_CLASS_NAME*/, Nil, clsTemp)
  }

  def createConversionModule(entityName: TypeName, entityFieldToType: Map[TermName, String]): ModuleDef = {
    val className = newTypeName("Column" + entityName)

    // body of ModuleDef_Template

    // #2 new anon$()
    val rhs =
      Apply(Select(New(Ident(className /*tpnme.ANON_CLASS_NAME*/ )), nme.CONSTRUCTOR), List(Ident(newTermName("view"))))
    //      val rhs = Block(List(clsDef), exprBlockNew)

    // #3 override def convert(view: RelationColumnView) = new ColumnResult[EmployeeImpl] {
    //       def id = view.get("id")
    //   }
    // here convert method comes from trait ViewToAnonConversion[EmployeeImpl, ColumnResult[EmployeeImpl]]
    val moduledefBody = DefDef(
      Modifiers(Flags.OVERRIDE),
      newTermName("convert"),
      Nil,
      List(
        List(ValDef(Modifiers(Flags.PARAM), newTermName("view"), Ident(newTypeName("RelationColumnView")), EmptyTree))),
      TypeTree(),
      rhs
    )

    // parents of ModuleDef_Template

    // #1 generic type for ViewToAnnonConversion: ShapeType and ColumnResult[ShapeType] { def fieldName: Array[_] }
    val defs = entityFieldToType.toList.map(f => {
      val fieldType = AppliedTypeTree(Ident(tpnme.Array), List(Ident(newTypeName(f._2))))
      DefDef(Modifiers(Flags.DEFERRED), f._1, Nil, List(Nil), fieldType, EmptyTree)
    })
    val tempCompound =
      Template(List(AppliedTypeTree(Ident(newTypeName("ColumnResult")), List(Ident(entityName)))), noSelfType, defs)
    val appliedTypeTreeArgs_2 = CompoundTypeTree(tempCompound)
    //      val appliedTypeTreeArgs = List(Ident(entityName), appliedTypeTreeArgs_2)
    val appliedTypeTreeArgs = List(Ident(entityName), Ident(className))
    val appliedTypeTree_tpt = Ident(newTypeName("ColumnViewToColumnResult"))
    val moduledefParent = AppliedTypeTree(appliedTypeTree_tpt, appliedTypeTreeArgs.toList)

    // Module def definition

    // #1 implicit object entityName_Conversion extends ViewToAnonConversion[EmployeeImpl, ColumnResult[EmployeeImpl] { def id: Array[_] }] {
    //           override def convert(view: RelationColumnView) = new ColumnResult[EmployeeImpl] {
    //               def id = view.get("id")
    //           }
    // }
    ModuleDef(
      Modifiers(Flags.IMPLICIT),
      newTermName("" + entityName + "Conversion"),
      createTemplate(List(moduledefParent), noSelfType, NoMods, Nil, List(Nil), List(moduledefBody), NoPosition)
    )
  }

}
