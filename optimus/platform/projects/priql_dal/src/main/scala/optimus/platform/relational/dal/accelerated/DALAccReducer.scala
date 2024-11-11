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
package optimus.platform.relational.dal.accelerated

import java.time._
import optimus.platform.dal.DSIStorageInfo
import optimus.platform.dal.DalAPI
import optimus.platform.dal.EntityResolver
import optimus.platform.dal.EntitySerializer
import optimus.platform.pickling.PickledInputStream
import optimus.platform.pickling.Unpickler
import optimus.platform.NoKey
import optimus.platform.RelationKey
import optimus.platform.TemporalContext
import optimus.platform.pickling.OptionUnpickler
import optimus.platform.relational.dal.DALProvider
import optimus.platform.relational.data.DbQueryTreeReducerBase
import optimus.platform.relational.data.DbQueryTreeReducerBase.TypeInfos
import optimus.platform.relational.data.FieldReader
import optimus.platform.relational.data.QueryCommand
import optimus.platform.relational.data.language.FormattedQuery
import optimus.platform.relational.data.mapping.MappingEntityLookup
import optimus.platform.relational.data.translation.ComparisonRewriter
import optimus.platform.relational.data.translation.ConditionalFlattener
import optimus.platform.relational.data.tree._
import optimus.platform.relational.tree._
import optimus.platform.storable.Entity
import optimus.platform.storable.EntityReference
import optimus.platform.storable.SerializedEntity

class DALAccReducer(val provider: DALProvider) extends DbQueryTreeReducerBase {
  import DALAccReducer._

  def createMapping() = new DALMapping

  def createLanguage(lookup: MappingEntityLookup) = new DALLanguage(lookup)

  protected override def buildInner(e: RelationElement): RelationElement = {
    val r = new DALAccReducer(provider)
    r.scope = scope
    r.nReaders = nReaders
    r.translator = translator
    r.executeOptions = executeOptions
    r.visitElement(e)
  }

  protected override def compileAndExecute(
      command: QueryCommand,
      projector: LambdaElement,
      proj: ProjectionElement): RelationElement = {
    def executeDALHeapEntityElement(d: DALHeapEntityElement, key: RelationKey[_]): RelationElement = {
      val fn = DALProvider.knownProjectorToLambda(d).get
      provider.execute(command, fn, key, d.rowTypeInfo, executeOptions.asEntitledOnlyIf(proj.entitledOnly))
    }

    proj.projector match {
      case DynamicObjectElement(d: DALHeapEntityElement) if proj.select.columns.size == 1 =>
        new MethodElement(
          QueryMethod.UNTYPE,
          MethodArg("src", executeDALHeapEntityElement(d, NoKey)) :: Nil,
          proj.rowTypeInfo,
          proj.key)
      case pj if proj.select.columns.size == 1 =>
        DALProvider.knownProjectorToLambda(pj) map { fn =>
          provider.execute(command, fn, proj.key, pj.rowTypeInfo, executeOptions.asEntitledOnlyIf(proj.entitledOnly))
        } getOrElse {
          super.compileAndExecute(command, projector, proj)
        }
      case _ =>
        super.compileAndExecute(command, projector, proj)
    }
  }

  protected override def handleBinaryExpression(binary: BinaryExpressionElement): RelationElement = {
    import BinaryExpressionType._
    binary match {
      case BinaryExpressionElement(EQ | NE, de: DALHeapEntityElement, c @ ConstValueElement(null, _), _) =>
        val column = de.members.head.asInstanceOf[ColumnElement]
        getCheckNoneFunction(binary.op == NE, column).getOrElse { updateBinary(binary, column, c) }
      case _ => super.handleBinaryExpression(binary)
    }
  }

  protected override def getReaderFunction(column: ColumnElement, returnType: TypeInfo[_]): Option[RelationElement] = {
    if (scope eq null) None
    else {
      scope.getValue(column).map { case (reader, iOrdinal) =>
        if (column.rowTypeInfo eq DALProvider.PersistentEntityType) {
          // generate code "val value = reader.readValue[Entity](ordinal)"
          val isOption = returnType.clazz == classOf[Option[_]]
          val fieldType = if (isOption) EntityOptionType else DALProvider.EntityType
          val readValue = FieldReader.getReaderFunction(fieldType, iOrdinal, reader)
          ElementFactory.convert(readValue, returnType)
        } else getFieldReaderFunction(reader, returnType, iOrdinal, column)
      }
    }
  }

  protected override def handleOptionElement(option: OptionElement): RelationElement = {
    val readOpt = option.element match {
      case de: DALHeapEntityElement =>
        de.members.head match {
          case e: ColumnElement => getReaderFunction(e, option.rowTypeInfo)
        }
      case _ => None
    }
    readOpt.getOrElse(super.handleOptionElement(option))
  }

  protected override def handleDALHeapEntity(de: DALHeapEntityElement): RelationElement = {
    de.members.head match {
      case e: ColumnElement => getReaderFunction(e, de.rowTypeInfo).getOrElse(e)
    }
  }

  protected override def handleEmbeddableCaseClass(caseClass: EmbeddableCaseClassElement): RelationElement = {
    caseClass.members.head match {
      case e: ColumnElement => getReaderFunction(e, caseClass.rowTypeInfo).getOrElse(e)
    }
  }

  protected override def getFormattedQuery(proj: ProjectionElement): FormattedQuery = {
    var select = SerializedKeyBasedIndexOptimizer.optimize(proj.select, translator.mapping)
    select = ComparisonRewriter.rewrite(select)
    select = ConditionalFlattener.flatten(select)
    translator.dialect.format(select)
  }
}

object DALAccReducer {
  val EntityType = TypeInfo(classOf[Entity])
  val EntityResolver = TypeInfo(classOf[EntityResolver])
  val DalAPI = TypeInfo(classOf[DalAPI])
  val TemporalContext = TypeInfo(classOf[TemporalContext])
  val UnpicklerAny = TypeInfo(classOf[Unpickler[_]], TypeInfo.ANY)
  val PickledInputStream = TypeInfo(classOf[PickledInputStream])
  val OptionCompanion = TypeInfo(Option.getClass)
  val OptionAny = TypeInfo(classOf[Option[_]], TypeInfo.ANY)
  val SerializedEntityType = TypeInfo(classOf[SerializedEntity])
  val EntitySerializerCompanion = TypeInfo(EntitySerializer.getClass)
  val StorageInfo = TypeInfo(classOf[DSIStorageInfo])
  val StorageInfoCompanion = TypeInfo(DSIStorageInfo.getClass)
  val EntityOptionType = typeInfo[Option[Entity]]

  def getFieldReaderFunction(reader: RelationElement, returnType: TypeInfo[_], iOrdinal: Int, column: ColumnElement) = {
    val (needUnpickle, needConvert) = shouldUnpickleOrConvert(returnType)

    (needUnpickle, needConvert) match {
      case (true, _) =>
        val isOption = returnType.clazz == classOf[Option[_]]
        val isEntity =
          if (isOption) returnType.typeParams.head <:< classOf[Entity] else returnType <:< classOf[Entity]

        val fieldType = if (isOption) OptionAny else TypeInfo.ANY
        val readValue = FieldReader.getReaderFunction(fieldType, iOrdinal, reader)
        // generate code "unpickler.unpickle(value, reader.pickledInputStream)"
        val columnUnpickler =
          if (isEntity) PicklerSelector.getUnpickler(returnType)
          else
            column.columnInfo.unpickler
              .map(p => if (isOption) new OptionUnpickler(p) else p)
              .getOrElse(PicklerSelector.getUnpickler(returnType))

        getUnpickledValue(columnUnpickler, readValue, reader, returnType)

      case (false, true) =>
        // generate code "val value = reader.readXXX(ordinal)"
        val readValue = FieldReader.getReaderFunction(returnType, iOrdinal, reader)
        ElementFactory.convert(readValue, returnType)

      case (false, false) => FieldReader.getReaderFunction(returnType, iOrdinal, reader)
    }
  }

  /**
   * Check if it need Unpicker and Convert. For Primitive type, we don't need Unpickler and Convert. For EntityReference
   * and Time type, we don't need Unpickler. But need Convert. For Option, we need check the inner type. For other case,
   * we need Unpickler and Convert.
   */
  private def shouldUnpickleOrConvert(rowTypeInfo: TypeInfo[_]): (Boolean, Boolean) = {
    val target = rowTypeInfo.clazz
    target match {
      case _ if target.isPrimitive || target == classOf[String] => (false, false)
      case _
          if target == classOf[ZonedDateTime] || target == classOf[LocalDate] || target == classOf[
            LocalTime] || target == classOf[OffsetTime] ||
            target == classOf[ZoneId] || target == classOf[Period] || target == classOf[EntityReference] =>
        (false, true)
      case _ if target == classOf[Option[_]] => shouldUnpickleOrConvert(rowTypeInfo.typeParams.head)
      case _                                 => (true, true)
    }
  }

  private def getUnpickledValue(
      unpickler: Unpickler[_],
      pickledValue: RelationElement,
      reader: RelationElement,
      returnType: TypeInfo[_]): RelationElement = {
    val unpicklerEle = new ConstValueElement(unpickler, UnpicklerAny)
    val method = new RuntimeMethodDescriptor(UnpicklerAny, "unpickle", TypeInfo.ANY)
    val inputStream = ElementFactory.call(
      reader,
      new RuntimeMethodDescriptor(TypeInfos.FieldReader, "pickledInputStream", TypeInfos.PickledInputStream),
      Nil)

    val value = ElementFactory.call(unpicklerEle, method, pickledValue :: inputStream :: Nil)
    ElementFactory.convert(value, returnType)
  }
}
