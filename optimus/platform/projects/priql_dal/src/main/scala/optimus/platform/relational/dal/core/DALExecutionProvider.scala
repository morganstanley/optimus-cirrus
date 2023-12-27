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
package optimus.platform.relational.dal.core

import optimus.entity.EntityInfoRegistry
import optimus.graph.Node
import optimus.platform._
import optimus.platform.dal.DalAPI
import optimus.platform.dal.EntityResolverReadImpl
import optimus.platform.dal.QueryTemporalityFinder
import optimus.platform.pickling.PickledInputStream
import optimus.platform.AsyncImplicits._
import optimus.platform.dsi.bitemporal.EntityClassQuery
import optimus.platform.dsi.bitemporal.ExpressionQueryCommand
import optimus.platform.dsi.bitemporal.QueryPlan
import optimus.platform.dsi.bitemporal.QueryResultMetaData
import optimus.platform.dsi.bitemporal.QueryResultSet
import optimus.platform.dsi.bitemporal.QueryResultSetReader
import optimus.platform.dsi.bitemporal.SerializedKeyQuery
import optimus.platform.dsi.expressions._
import optimus.platform.relational.PriqlSettings
import optimus.platform.relational.RelationalException
import optimus.platform.relational.RelationalUnsupportedException
import optimus.platform.relational.dal.DALProvider
import optimus.platform.relational.data.FieldReader
import optimus.platform.relational.data.tree.DbPickledInputStream
import optimus.platform.relational.inmemory.IterableSource
import optimus.platform.relational.tree.ExecuteOptions
import optimus.platform.relational.tree.MethodPosition
import optimus.platform.relational.tree.ProviderRelation
import optimus.platform.relational.tree.QueryExplainItem
import optimus.platform.relational.tree.TypeInfo
import optimus.platform.storable.{Entity => OptimusEntity}
import optimus.platform.storable.NonUniqueKey
import optimus.platform.storable.SerializedKey
import optimus.platform.storable.UniqueKey

import scala.collection.immutable.VectorBuilder
import scala.collection.mutable.ListBuffer

class DALExecutionProvider[T](
    private[optimus /*platform*/ ] val expression: Expression,
    private val plan: QueryPlan,
    val nodeOrFunc: Either[FieldReader => Node[T], FieldReader => T],
    shape: TypeInfo[_],
    key: RelationKey[_],
    val dalApi: DalAPI,
    val executeOptions: ExecuteOptions,
    outerPos: MethodPosition = MethodPosition.unknown
) extends ProviderRelation(shape, key)
    with IterableSource[T] {
  import DALExecutionProvider._
  private def somePosition = pos.orElse(executeOptions.pos).orElse(outerPos).posInfo.split('/').last
  override def getProviderName: String = "DALExecutionProvider"
  override def shouldExecuteDistinct: Boolean = false
  override def isSyncSafe: Boolean = false

  override def getSync(): Iterable[T] = {
    throw new RelationalUnsupportedException(s"$getProviderName cannot execute query sync")
  }

  override def makeKey(newKey: RelationKey[_]): DALExecutionProvider[T] = {
    new DALExecutionProvider(expression, plan, nodeOrFunc, rowTypeInfo, newKey, dalApi, executeOptions, pos)
  }

  override def fillQueryExplainItem(level_id: Integer, table: ListBuffer[QueryExplainItem]): Unit = {
    val displayExpr = AsyncValueEvaluator.prepareForDisplay(expression)
    val sel @ Select(e: Entity, _, _, _, Nil, _, None, _, _, _) = ConstFormatter.format(displayExpr, null)
    val detail = ExpressionPriQLFormatter.format(sel).substring(6 + e.name.length) // remove the from(...)
    table += QueryExplainItem(level_id, "DALAccess", rowTypeInfo.name, if (detail.isEmpty) "TableScan" else detail, 0)
  }

  @async
  final protected def getViaExpression(resolver: EntityResolverReadImpl): Iterable[T] = {
    val command = buildServerSideCommand()
    val resultSet = Query.Tracer.trace(
      executeOptions.pos,
      asNode { () =>
        resolver.findByExpressionCommandWithTemporalContext(command, dalApi.loadContext)
      }
    )
    val intermediate = resultSet.result

    val inputStream = DbPickledInputStream(Map.empty, dalApi.loadContext)
    val ret = nodeOrFunc match {
      case Right(f) =>
        val builder = new VectorBuilder[T]()
        val rsReader = resultSet.reader
        val reader = new DALFieldReader(rsReader, inputStream)
        while (rsReader.next()) {
          builder += f(reader)
        }
        val ret = builder.result()
        ret

      case Left(nf) =>
        val nodeFunc = asNode.apply$withNode(nf)
        intermediate.apar(PriqlSettings.concurrencyLevel).map(r => nodeFunc(new RowReader(r, inputStream)))
    }
    ret
  }

  @async override def get(): Iterable[T] = {
    import BinaryOperator.Equal
    import PropertyType.Special
    val resolver = EvaluationContext.env.entityResolver.asInstanceOf[EntityResolverReadImpl]
    val Select(e: Entity, properties, where, sortBy, Nil, take, None, _, _, _) = expression
    val entity = EntityInfoRegistry.getClassInfo(e.name).runtimeClass.asInstanceOf[Class[OptimusEntity]]
    val loadCtx = dalApi.loadContext
    properties match {
      case Nil | List(
            PropertyDef(
              _,
              Function(ConvertOps.ToEntity.Name, Property(Special, List(DALProvider.EntityRef), _) :: Nil))) =>
        // no projection (for Query.execute)
        where match {
          case None if sortBy.isEmpty && take.isEmpty =>
            // class query, we should trace it since there is no DAL api for such query
            val ret = Query.Tracer.trace(
              executeOptions.pos,
              asNode { () =>
                resolver
                  .findEntitiesByClassWithContext(entity, loadCtx, executeOptions.entitledOnly)
                  .asInstanceOf[Iterable[T]]
              }
            )
            ret

          case Some(Binary(Equal, _: Property, r @ RichConstant(_, _, Some(index))))
              if (index.unique || index.storableClass == entity) && sortBy.isEmpty && take.isEmpty =>
            val RichConstant(v, _, _) = AsyncValueEvaluator.evaluate(r)
            val sk = index.toSerializedKey(checkLoadContext(v, loadCtx))
            val k = if (index.unique) {
              new UniqueKey[OptimusEntity] {
                def toSerializedKey: SerializedKey = sk
                def subjectClass: Class[OptimusEntity] = index.storableClass.asInstanceOf[Class[OptimusEntity]]
              }
            } else {
              new NonUniqueKey[OptimusEntity] {
                def toSerializedKey: SerializedKey = sk
                def subjectClass: Class[OptimusEntity] = index.storableClass.asInstanceOf[Class[OptimusEntity]]
              }
            }
            // query equivalent to Entity.index.find(...) or Entity.get(...)
            val result = Query.Tracer.trace(
              executeOptions.pos,
              asNode { () =>
                resolver.findByIndex(k, loadCtx, Some(entity), executeOptions.entitledOnly).asInstanceOf[Iterable[T]]
              }
            )
            val ret =
              if (k.subjectClass == entity) result
              else result.filter(e => entity.isAssignableFrom(e.getClass))
            ret
          case _ =>
            getViaExpression(resolver)
        }

      case List(PropertyDef(_, Aggregate(AggregateType.Count, Nil, false))) =>
        // .count
        if (executeOptions.entitledOnly)
          throw new RelationalUnsupportedException(entitledOnlyUnsupportedForCount)
        val newWhere = where.map(w => ConstFormatter.format(AsyncValueEvaluator.evaluate(w), loadCtx))
        val dsiAt = dalTemporalContext(dalApi.loadContext, entity, newWhere)
        newWhere map {
          case Binary(Equal, _: Property, Constant(sk: SerializedKey, _)) =>
            Seq(resolver.count(SerializedKeyQuery(sk), dsiAt)).asInstanceOf[Iterable[T]]
          case x =>
            throw new RelationalUnsupportedException(s"Unexpected where expression: $x")
        } getOrElse {
          Seq(resolver.count(EntityClassQuery(e.name), dsiAt)).asInstanceOf[Iterable[T]]
        }

      case _ =>
        // server side projection (for Query.executeReference currently)
        getViaExpression(resolver)

    }
  }

  @async def buildServerSideCommand(): ExpressionQueryCommand = {
    val s @ Select(e: Entity, _, where, _, _, _, _, _, _, _) =
      ConstFormatter.format(AsyncValueEvaluator.evaluate(expression), dalApi.loadContext)
    val entity = EntityInfoRegistry.getClassInfo(e.name).runtimeClass
    val tsQuery = toTemporalSurfaceQuery(entity, where)
    val isEref = QueryResultMetaData.isEntityReferenceQuery(s)
    val dsiAt =
      if (isEref) QueryTemporalityFinder.findIndexQueryTemporality(dalApi.loadContext, tsQuery)
      else QueryTemporalityFinder.findQueryTemporality(dalApi.loadContext, tsQuery)
    val newEx = s.copy(from = e.copy(when = dsiAt))
    ExpressionQueryCommand(newEx, plan, executeOptions.entitledOnly)
  }
}

object DALExecutionProvider extends DALQueryUtil {

  val entitledOnlyUnsupportedForCount = "entitledOnly is not supported for count query"

  @node @scenarioIndependent
  def unsafeFindByExpressionDoNotUse(command: ExpressionQueryCommand, loadContext: TemporalContext): QueryResultSet = {
    EvaluationContext.entityResolver
      .asInstanceOf[EntityResolverReadImpl]
      .findByExpressionCommandWithTemporalContext(command, loadContext)
  }

  class DALFieldReader(val rs: QueryResultSetReader, override val pickledInputStream: PickledInputStream)
      extends FieldReader {

    def readValue[S: TypeInfo](ordinal: Int): S = {
      if (isNone(ordinal)) throw new RelationalException("Can't read empty value from database. Use Option instead.")
      else rs.get[S](ordinal)
    }

    def readOptionValue[S: TypeInfo](ordinal: Int): Option[S] = {
      if (isNone(ordinal)) None
      else {
        val x = readValue[S](ordinal)
        Some(x)
      }
    }

    def readBoolean(ordinal: Int): Boolean = {
      if (isNone(ordinal))
        throw new RelationalException("Can't read empty boolean from database. Use Option[Boolean] instead.")
      else rs.get[Boolean](ordinal)
    }

    def readOptionBoolean(ordinal: Int): Option[Boolean] = {
      if (isNone(ordinal)) None
      else {
        val x = readBoolean(ordinal)
        Some(x)
      }
    }

    def readByte(ordinal: Int): Byte = {
      if (isNone(ordinal))
        throw new RelationalException("Can't read empty byte from database. Use Option[Byte] instead.")
      else {
        rs.get[Any](ordinal) match {
          case v: Number => v.byteValue
          case v: Byte   => v
          case v         => throw new RelationalException(s"Expected byte value but actual get ${v.getClass}.")
        }
      }
    }

    def readOptionByte(ordinal: Int): Option[Byte] = {
      if (isNone(ordinal)) None
      else {
        val x = readByte(ordinal)
        Some(x)
      }
    }

    def readChar(ordinal: Int): Char = {
      if (isNone(ordinal))
        throw new RelationalException("Can't read empty char from database. Use Option[Char] instead.")
      else {
        rs.get[Any](ordinal) match {
          case v: Int  => v.toChar
          case v: Char => v
          case v       => throw new RelationalException(s"Expected char value but actual get ${v.getClass}.")
        }
      }
    }

    def readOptionChar(ordinal: Int): Option[Char] = {
      if (isNone(ordinal)) None
      else {
        val x = readChar(ordinal)
        Some(x)
      }
    }

    def readShort(ordinal: Int): Short = {
      if (isNone(ordinal))
        throw new RelationalException("Can't read empty short from database. Use Option[Short] instead.")
      else {
        rs.get[Any](ordinal) match {
          case v: Number => v.shortValue
          case v         => throw new RelationalException(s"Expected short value but actual get ${v.getClass}.")
        }
      }
    }

    def readOptionShort(ordinal: Int): Option[Short] = {
      if (isNone(ordinal)) None
      else {
        val x = readShort(ordinal)
        Some(x)
      }
    }

    def readInt(ordinal: Int): Int = {
      if (isNone(ordinal)) throw new RelationalException("Can't read empty int from database. Use Option[Int] instead.")
      else {
        rs.get[Any](ordinal) match {
          case v: Number => v.intValue
          case v         => throw new RelationalException(s"Expected int value but actual get ${v.getClass}.")
        }
      }
    }

    def readOptionInt(ordinal: Int): Option[Int] = {
      if (isNone(ordinal)) None
      else {
        val x = readInt(ordinal)
        Some(x)
      }
    }

    def readLong(ordinal: Int): Long = {
      if (isNone(ordinal))
        throw new RelationalException("Can't read empty long from database. Use Option[Long] instead.")
      else {
        rs.get[Any](ordinal) match {
          case v: Number => v.longValue
          case s: String => s.toLong
          case v         => throw new RelationalException(s"Expected long value but actual get ${v.getClass}.")
        }
      }
    }

    def readOptionLong(ordinal: Int): Option[Long] = {
      if (isNone(ordinal)) None
      else {
        val x = readLong(ordinal)
        Some(x)
      }
    }

    def readFloat(ordinal: Int): Float = {
      if (isNone(ordinal))
        throw new RelationalException("Can't read empty float from database. Use Option[Float] instead.")
      else {
        rs.get[Any](ordinal) match {
          case v: Number => v.floatValue
          case v         => throw new RelationalException(s"Expected float value but actual get ${v.getClass}.")
        }
      }
    }

    def readOptionFloat(ordinal: Int): Option[Float] = {
      if (isNone(ordinal)) None
      else {
        val x = readFloat(ordinal)
        Some(x)
      }
    }

    def readDouble(ordinal: Int): Double = {
      if (isNone(ordinal))
        throw new RelationalException("Can't read empty double from database. Use Option[Double] instead.")
      else {
        rs.get[Any](ordinal) match {
          case v: Number => v.doubleValue
          case s: String => s.toDouble
          case v         => throw new RelationalException(s"Expected double value but actual get ${v.getClass}.")
        }
      }
    }

    def readOptionDouble(ordinal: Int): Option[Double] = {
      if (isNone(ordinal)) None
      else {
        val x = readDouble(ordinal)
        Some(x)
      }
    }

    def readString(ordinal: Int): String = {
      if (isNone(ordinal))
        throw new RelationalException("Can't read empty string from database. Use Option[String] instead.")
      else rs.get[String](ordinal)
    }

    def readOptionString(ordinal: Int): Option[String] = {
      if (isNone(ordinal)) None
      else {
        val x = readString(ordinal)
        Some(x)
      }
    }

    def isNone(ordinal: Int): Boolean = {
      rs.isNull(ordinal)
    }
  }

  class RowReader(val row: Array[Any], override val pickledInputStream: PickledInputStream) extends FieldReader {

    def readValue[S: TypeInfo](ordinal: Int): S = {
      if (isNone(ordinal)) throw new RelationalException("Can't read empty value from database. Use Option instead.")
      else row(ordinal).asInstanceOf[S]
    }

    def readOptionValue[S: TypeInfo](ordinal: Int): Option[S] = {
      if (isNone(ordinal)) None
      else {
        val x = readValue[S](ordinal)
        Some(x)
      }
    }

    def readBoolean(ordinal: Int): Boolean = {
      if (isNone(ordinal))
        throw new RelationalException("Can't read empty boolean from database. Use Option[Boolean] instead.")
      else row(ordinal).asInstanceOf[Boolean]
    }

    def readOptionBoolean(ordinal: Int): Option[Boolean] = {
      if (isNone(ordinal)) None
      else {
        val x = readBoolean(ordinal)
        Some(x)
      }
    }

    def readByte(ordinal: Int): Byte = {
      if (isNone(ordinal))
        throw new RelationalException("Can't read empty byte from database. Use Option[Byte] instead.")
      else {
        row(ordinal) match {
          case v: Number => v.byteValue
          case v: Byte   => v
          case v         => throw new RelationalException(s"Expected byte value but actual get ${v.getClass}.")
        }
      }
    }

    def readOptionByte(ordinal: Int): Option[Byte] = {
      if (isNone(ordinal)) None
      else {
        val x = readByte(ordinal)
        Some(x)
      }
    }

    def readChar(ordinal: Int): Char = {
      if (isNone(ordinal))
        throw new RelationalException("Can't read empty char from database. Use Option[Char] instead.")
      else {
        row(ordinal) match {
          case v: Int  => v.toChar
          case v: Char => v
          case v       => throw new RelationalException(s"Expected char value but actual get ${v.getClass}.")
        }
      }
    }

    def readOptionChar(ordinal: Int): Option[Char] = {
      if (isNone(ordinal)) None
      else {
        val x = readChar(ordinal)
        Some(x)
      }
    }

    def readShort(ordinal: Int): Short = {
      if (isNone(ordinal))
        throw new RelationalException("Can't read empty short from database. Use Option[Short] instead.")
      else {
        row(ordinal) match {
          case v: Number => v.shortValue
          case v         => throw new RelationalException(s"Expected short value but actual get ${v.getClass}.")
        }
      }
    }

    def readOptionShort(ordinal: Int): Option[Short] = {
      if (isNone(ordinal)) None
      else {
        val x = readShort(ordinal)
        Some(x)
      }
    }

    def readInt(ordinal: Int): Int = {
      if (isNone(ordinal)) throw new RelationalException("Can't read empty int from database. Use Option[Int] instead.")
      else {
        row(ordinal) match {
          case v: Number => v.intValue
          case v         => throw new RelationalException(s"Expected int value but actual get ${v.getClass}.")
        }
      }
    }

    def readOptionInt(ordinal: Int): Option[Int] = {
      if (isNone(ordinal)) None
      else {
        val x = readInt(ordinal)
        Some(x)
      }
    }

    def readLong(ordinal: Int): Long = {
      if (isNone(ordinal))
        throw new RelationalException("Can't read empty long from database. Use Option[Long] instead.")
      else {
        row(ordinal) match {
          case v: Number => v.longValue
          case s: String => s.toLong
          case v         => throw new RelationalException(s"Expected long value but actual get ${v.getClass}.")
        }
      }
    }

    def readOptionLong(ordinal: Int): Option[Long] = {
      if (isNone(ordinal)) None
      else {
        val x = readLong(ordinal)
        Some(x)
      }
    }

    def readFloat(ordinal: Int): Float = {
      if (isNone(ordinal))
        throw new RelationalException("Can't read empty float from database. Use Option[Float] instead.")
      else {
        row(ordinal) match {
          case v: Number => v.floatValue
          case v         => throw new RelationalException(s"Expected float value but actual get ${v.getClass}.")
        }
      }
    }

    def readOptionFloat(ordinal: Int): Option[Float] = {
      if (isNone(ordinal)) None
      else {
        val x = readFloat(ordinal)
        Some(x)
      }
    }

    def readDouble(ordinal: Int): Double = {
      if (isNone(ordinal))
        throw new RelationalException("Can't read empty double from database. Use Option[Double] instead.")
      else {
        row(ordinal) match {
          case v: Number => v.doubleValue
          case s: String => s.toDouble
          case v         => throw new RelationalException(s"Expected double value but actual get ${v.getClass}.")
        }
      }
    }

    def readOptionDouble(ordinal: Int): Option[Double] = {
      if (isNone(ordinal)) None
      else {
        val x = readDouble(ordinal)
        Some(x)
      }
    }

    def readString(ordinal: Int): String = {
      if (isNone(ordinal))
        throw new RelationalException("Can't read empty string from database. Use Option[String] instead.")
      else row(ordinal).asInstanceOf[String]
    }

    def readOptionString(ordinal: Int): Option[String] = {
      if (isNone(ordinal)) None
      else {
        val x = readString(ordinal)
        Some(x)
      }
    }

    def isNone(ordinal: Int): Boolean = {
      row(ordinal) == null
    }
  }
}
