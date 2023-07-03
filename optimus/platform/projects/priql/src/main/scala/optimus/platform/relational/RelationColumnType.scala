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
package optimus.platform.relational

import java.time.{LocalDate, ZonedDateTime}

import optimus.platform.internal.ValueFetch
import optimus.platform.relational.tree.BinaryExpressionType._
import optimus.platform.relational.tree._

import scala.collection.immutable.List
import scala.collection.mutable
import scala.collection.mutable.{BitSet, ListBuffer}

/**
 * this is used for adding additional methods to dynamic generated anonymous column class ShapeType must be entity now
 */
trait ColumnResult[ShapeType]

trait ColumnViewToColumnResult[T, U] {
  def convert(view: RelationColumnView): U
}

/**
 * RelationColumnView holds an available column set. It is backed up by RelationColumnSource which holds the original
 * data source.
 *
 * indices is used for recording valid row index, since several views will share one column source, the source will
 * never change
 *
 * lazy fieldValue will store all columns fetched by client, so that when client fetches this column several times, it
 * will use the cached one instead of fetching from column source again
 */
class RelationColumnView(val indicies: List[Int], val src: RelationColumnSource) {
  lazy val fieldToValue = mutable.Map[String, Array[_]]()

  def get[T](field: String, cls: Class[T]): Array[T] = {
    fieldToValue.get(field) match {
      case Some(value) => value.asInstanceOf[Array[T]]
      case None =>
        val columns = src.columns.filter(p => p.columnName == field)
        if (columns.isEmpty)
          throw new RelationalException(s"the field $field doesn't exist in RelationColumnView")
        columns(0).columnValues match {
          case a: Array[Int] => {
            val newColumnValues = new Array[Int](indicies.size)
            var j = 0
            indicies.foreach(i => { newColumnValues(j) = a(i); j = j + 1 })
            fieldToValue.put(field, newColumnValues)
            newColumnValues.asInstanceOf[Array[T]]
          }
          case a: Array[Long] => {
            val newColumnValues = new Array[Long](indicies.size)
            var j = 0
            indicies.foreach(i => { newColumnValues(j) = a(i); j = j + 1 })
            fieldToValue.put(field, newColumnValues)
            newColumnValues.asInstanceOf[Array[T]]
          }
          case a: Array[Float] => {
            val newColumnValues = new Array[Float](indicies.size)
            var j = 0
            indicies.foreach(i => { newColumnValues(j) = a(i); j = j + 1 })
            fieldToValue.put(field, newColumnValues)
            newColumnValues.asInstanceOf[Array[T]]
          }
          case a: Array[Double] => {
            val newColumnValues = new Array[Double](indicies.size)
            var j = 0
            indicies.foreach(i => { newColumnValues(j) = a(i); j = j + 1 })
            fieldToValue.put(field, newColumnValues)
            newColumnValues.asInstanceOf[Array[T]]
          }
          case a: Array[Short] => {
            val newColumnValues = new Array[Short](indicies.size)
            var j = 0
            indicies.foreach(i => { newColumnValues(j) = a(i); j = j + 1 })
            fieldToValue.put(field, newColumnValues)
            newColumnValues.asInstanceOf[Array[T]]
          }
          case a: Array[Byte] => {
            val newColumnValues = new Array[Byte](indicies.size)
            var j = 0
            indicies.foreach(i => { newColumnValues(j) = a(i); j = j + 1 })
            fieldToValue.put(field, newColumnValues)
            newColumnValues.asInstanceOf[Array[T]]
          }
          case a: Array[Char] => {
            val newColumnValues = new Array[Char](indicies.size)
            var j = 0
            indicies.foreach(i => { newColumnValues(j) = a(i); j = j + 1 })
            fieldToValue.put(field, newColumnValues)
            newColumnValues.asInstanceOf[Array[T]]
          }
          case a: Array[String] => {
            val newColumnValues = new Array[String](indicies.size)
            var j = 0
            indicies.foreach(i => { newColumnValues(j) = a(i); j = j + 1 })
            fieldToValue.put(field, newColumnValues)
            newColumnValues.asInstanceOf[Array[T]]
          }
          case a: Array[AnyRef] => {
            if (!a.isEmpty) {
              a.head match {
                case b: java.lang.Integer => {
                  val newColumnValues = new Array[java.lang.Integer](indicies.size)
                  var j = 0
                  indicies.foreach(i => { newColumnValues(j) = a(i).asInstanceOf[java.lang.Integer]; j = j + 1 })
                  fieldToValue.put(field, newColumnValues)
                  newColumnValues.asInstanceOf[Array[T]]
                }
                case b: java.lang.Long => {
                  val newColumnValues = new Array[java.lang.Long](indicies.size)
                  var j = 0
                  indicies.foreach(i => { newColumnValues(j) = a(i).asInstanceOf[java.lang.Long]; j = j + 1 })
                  fieldToValue.put(field, newColumnValues)
                  newColumnValues.asInstanceOf[Array[T]]
                }
                case b: java.lang.Double => {
                  val newColumnValues = new Array[java.lang.Double](indicies.size)
                  var j = 0
                  indicies.foreach(i => { newColumnValues(j) = a(i).asInstanceOf[java.lang.Double]; j = j + 1 })
                  fieldToValue.put(field, newColumnValues)
                  newColumnValues.asInstanceOf[Array[T]]
                }
                case b: java.lang.Float => {
                  val newColumnValues = new Array[java.lang.Float](indicies.size)
                  var j = 0
                  indicies.foreach(i => { newColumnValues(j) = a(i).asInstanceOf[java.lang.Float]; j = j + 1 })
                  fieldToValue.put(field, newColumnValues)
                  newColumnValues.asInstanceOf[Array[T]]
                }
                case b: java.lang.Short => {
                  val newColumnValues = new Array[java.lang.Short](indicies.size)
                  var j = 0
                  indicies.foreach(i => { newColumnValues(j) = a(i).asInstanceOf[java.lang.Short]; j = j + 1 })
                  fieldToValue.put(field, newColumnValues)
                  newColumnValues.asInstanceOf[Array[T]]
                }
                case b: java.lang.Byte => {
                  val newColumnValues = new Array[java.lang.Byte](indicies.size)
                  var j = 0
                  indicies.foreach(i => { newColumnValues(j) = a(i).asInstanceOf[java.lang.Byte]; j = j + 1 })
                  fieldToValue.put(field, newColumnValues)
                  newColumnValues.asInstanceOf[Array[T]]
                }
                case b: LocalDate => {
                  val newColumnValues = new Array[LocalDate](indicies.size)
                  var j = 0
                  indicies.foreach(i => { newColumnValues(j) = a(i).asInstanceOf[LocalDate]; j = j + 1 })
                  fieldToValue.put(field, newColumnValues)
                  newColumnValues.asInstanceOf[Array[T]]
                }
                case b: ZonedDateTime => {
                  val newColumnValues = new Array[ZonedDateTime](indicies.size)
                  var j = 0
                  indicies.foreach(i => { newColumnValues(j) = a(i).asInstanceOf[ZonedDateTime]; j = j + 1 })
                  fieldToValue.put(field, newColumnValues)
                  newColumnValues.asInstanceOf[Array[T]]
                }
                case _ => throw new RelationalException(s"type conversion for ${a.head.getClass} is not implemented!")
              }
            } else null
          }
        }
    }
  }

}

/**
 * leftDefaultValue and rightDefaultValue type is ConstValueElement instead of FuncElement since we need to construct a
 * whole row entity from column
 *
 * use reflection if use FuncElement which accepts IRowType just like row based outer join
 */
class JoinColumnView(
    val joinMap: List[Tuple2[Int, Int]],
    val leftRelation: RelationColumnSource,
    val rightRelation: RelationColumnSource,
    val leftDefaultValueRelation: RelationColumnSource,
    val rightDefaultValueRelation: RelationColumnSource) {
  private def createNewArray[T](list: List[Int], columnValues: Array[T], defaultColumnValues: Array[_]): Array[T] = {
    columnValues match {
      case arr: Array[Int] => {
        val defaultarr = defaultColumnValues match {
          case null              => null
          case array: Array[Int] => array
          case _ =>
            throw new RelationalException(
              s"default value type is ${defaultColumnValues.getClass} while value type is ${columnValues.getClass}")
        }

        val newArr = new Array[Int](list.length)
        var i: Int = 0
        list.foreach(f => {
          newArr(i) =
            if (f >= 0) arr(f)
            else {
              if (defaultarr != null) defaultarr(-1 - f) // since all mismatched index starts from -1, -2 ....
              else 0
            }; i += 1
        })
        newArr
      }
      case arr: Array[Long] => {
        val defaultarr = defaultColumnValues match {
          case null               => null
          case array: Array[Long] => array
          case _ =>
            throw new RelationalException(
              s"default value type is ${defaultColumnValues.getClass} while value type is ${columnValues.getClass}")
        }

        val newArr = new Array[Long](list.length)
        var i: Int = 0
        list.foreach(f => {
          newArr(i) =
            if (f >= 0) arr(f)
            else {
              if (defaultarr != null) defaultarr(-1 - f) // since all mismatched index starts from -1, -2 ....
              else 0L
            }; i += 1
        })
        newArr
      }
      case arr: Array[Double] => {
        val defaultarr = defaultColumnValues match {
          case null                 => null
          case array: Array[Double] => array
          case _ =>
            throw new RelationalException(
              s"default value type is ${defaultColumnValues.getClass} while value type is ${columnValues.getClass}")
        }

        val newArr = new Array[Double](list.length)
        var i: Int = 0
        list.foreach(f => {
          newArr(i) =
            if (f >= 0) arr(f)
            else {
              if (defaultarr != null) defaultarr(-1 - f) // since all mismatched index starts from -1, -2 ....
              else 0.0
            }; i += 1
        })
        newArr
      }
      case arr: Array[Float] => {
        val defaultarr = defaultColumnValues match {
          case null                => null
          case array: Array[Float] => array
          case _ =>
            throw new RelationalException(
              "default value type is " + defaultColumnValues.getClass + " while value type is " + columnValues
                .getClass())
        }

        val newArr = new Array[Float](list.length)
        var i: Int = 0
        list.foreach(f => {
          newArr(i) =
            if (f >= 0) arr(f)
            else {
              if (defaultarr != null) defaultarr(-1 - f) // since all mismatched index starts from -1, -2 ....
              else 0f
            }; i += 1
        })
        newArr
      }
      case arr: Array[Short] => {
        val defaultarr = defaultColumnValues match {
          case null                => null
          case array: Array[Short] => array
          case _ =>
            throw new RelationalException(
              s"default value type is ${defaultColumnValues.getClass} while value type is ${columnValues.getClass}")
        }

        val newArr = new Array[Short](list.length)
        var i: Int = 0
        list.foreach(f => {
          newArr(i) =
            if (f >= 0) arr(f)
            else {
              if (defaultarr != null) defaultarr(-1 - f) // since all mismatched index starts from -1, -2 ....
              else 0
            }; i += 1
        })
        newArr
      }
      case arr: Array[Byte] => {
        val defaultarr = defaultColumnValues match {
          case null               => null
          case array: Array[Byte] => array
          case _ =>
            throw new RelationalException(
              s"default value type is ${defaultColumnValues.getClass} while value type is ${columnValues.getClass}")
        }

        val newArr = new Array[Byte](list.length)
        var i: Int = 0
        list.foreach(f => {
          newArr(i) =
            if (f >= 0) arr(f)
            else {
              if (defaultarr != null) defaultarr(-1 - f) // since all mismatched index starts from -1, -2 ....
              else 0
            }; i += 1
        })
        newArr
      }
      case arr: Array[Char] => {
        val defaultarr = defaultColumnValues match {
          case null               => null
          case array: Array[Char] => array
          case _ =>
            throw new RelationalException(
              s"default value type is ${defaultColumnValues.getClass} while value type is ${columnValues.getClass}")
        }

        val newArr = new Array[Char](list.length)
        var i: Int = 0
        list.foreach(f => {
          newArr(i) =
            if (f >= 0) arr(f)
            else {
              if (defaultarr != null) defaultarr(-1 - f) // since all mismatched index starts from -1, -2 ....
              else ' '
            }; i += 1
        })
        newArr
      }
      case arr: Array[String] => {
        val defaultarr = defaultColumnValues match {
          case null                 => null
          case array: Array[String] => array
          case _ =>
            throw new RelationalException(
              s"default value type is ${defaultColumnValues.getClass} while value type is ${columnValues.getClass}")
        }
        val newArr = new Array[String](list.length)
        var i: Int = 0
        list.foreach(f => {
          newArr(i) =
            if (f >= 0) arr(f)
            else {
              if (defaultarr != null) defaultarr(-1 - f) // since all mismatched index starts from -1, -2 ....
              else ""
            }; i += 1
        })
        newArr
      }
      case arr: Array[AnyRef] => {
        val defaultarr = defaultColumnValues match {
          case null                 => null
          case array: Array[AnyRef] => array
          case _ =>
            throw new RelationalException(
              s"default value type is ${defaultColumnValues.getClass} while value type is ${columnValues.getClass}")
        }
        val newArr = new Array[AnyRef](list.length)
        var i: Int = 0
        list.foreach(f => {
          newArr(i) =
            if (f >= 0) arr(f)
            else {
              if (defaultarr != null) defaultarr(-1 - f) // since all mismatched index starts from -1, -2 ....
              else new Object()
            }; i += 1
        })
        newArr
      }
      case _ => throw new RelationalException("")
    }
  }

  def upgradeToRelationColumnView(): RelationColumnView = {
    val leftList = ListBuffer[Int]()
    val rightList = ListBuffer[Int]()
    joinMap.foreach(f => { leftList += f._1; rightList += f._2; })
    val columns = ListBuffer[RelationColumn[_]]()
    leftRelation.columns.foreach(column => {
      val columnName = column.columnName
      val columnType = column.columnType
      val columnValues = column.columnValues
      val defaultColumnValues =
        if (leftDefaultValueRelation == null) null
        else leftDefaultValueRelation.columns.filter(p => p.columnName.equals(columnName)).head.columnValues
      columns += RelationColumn.generateRelationColumnFromArray(
        columnType,
        "_1." + columnName,
        createNewArray(leftList.toList, columnValues, defaultColumnValues))
    })

    rightRelation.columns.foreach(column => {
      val columnName = column.columnName
      val columnType = column.columnType
      val columnValues = column.columnValues
      val defaultColumnValues =
        if (rightDefaultValueRelation == null) null
        else rightDefaultValueRelation.columns.filter(p => p.columnName.equals(columnName)).head.columnValues
      columns += RelationColumn.generateRelationColumnFromArray(
        columnType,
        "_2." + columnName,
        createNewArray(rightList.toList, columnValues, defaultColumnValues))

    })
    new RelationColumnView(
      List.range(0, joinMap.size),
      new RelationColumnSource(TupleTypeInfo(leftRelation.shapeType, rightRelation.shapeType), columns.toList))
  }
}

/**
 * keyColumns is column which is specified as key or column combination through calculation, element in groups is a
 * group of row index
 */
class GroupbyColumnView(
    val keyColumns: List[RelationColumn[_]],
    val valueColumns: List[RelationColumn[_]],
    val groups: List[BitSet],
    val projectionType: TypeInfo[_]) {
  def upgradeToRelationColumnView(): RelationColumnView = {
    val keyNum = keyColumns.length
    var keyIndex = 0

    val keyIndecies = groups.map(f => f.head)

    val newKeyColumns = keyColumns.map(kc => {
      val newArray = RelationColumn.generateNewArrayFromOriginalArray(kc.columnValues, keyIndecies)
      keyIndex += 1
      if (keyNum == 1) RelationColumn.generateRelationColumnFromArray(kc.columnType, "_1", newArray)
      else RelationColumn.generateRelationColumnFromArray(kc.columnType, "_1._" + keyIndex, newArray)
    })

    val newValueColumns = valueColumns.map(vc => {
      val newArray = groups.map(f => RelationColumn.generateNewArrayFromOriginalArray(vc.columnValues, f.toList))
      RelationColumn.generateRelationColumnFromArray(vc.columnType, "_2." + vc.columnName, newArray.toArray)
    })

    new RelationColumnView(
      List.range(0, groups.size),
      new RelationColumnSource(projectionType, newKeyColumns.union(newValueColumns)))
  }
}

object RelationColumnView {

  /**
   * @return
   *   RelationColumn[_] or Any(ConstValueElement) (+-*%/) or BitSet(logical operator)
   */
  def executeBinaryExpression(view: RelationColumnView, element: RelationElement): Any = {
    element match {
      case BinaryExpressionElement(op, left, right, rowTypeInfo) =>
        val result = op match {
          case EQ => equals(executeBinaryExpression(view, left), executeBinaryExpression(view, right), view.indicies)
          case NE => notEquals(executeBinaryExpression(view, left), executeBinaryExpression(view, right), view.indicies)
          case GT => gt(executeBinaryExpression(view, left), executeBinaryExpression(view, right), view.indicies)
          case GE => ge(executeBinaryExpression(view, left), executeBinaryExpression(view, right), view.indicies)
          case LT => lt(executeBinaryExpression(view, left), executeBinaryExpression(view, right), view.indicies)
          case LE => le(executeBinaryExpression(view, left), executeBinaryExpression(view, right), view.indicies)
          case BOOLAND =>
            val l = executeBinaryExpression(view, left.asInstanceOf[BinaryExpressionElement])
            val r = executeBinaryExpression(view, right.asInstanceOf[BinaryExpressionElement])
            (l, r) match {
              case (leftRes: BitSet, rightRes: BitSet)               => leftRes.&(rightRes)
              case (leftRes: BitSet, rightRes) if (rightRes != null) => leftRes
              case (leftRes, rightRes: BitSet) if (leftRes != null)  => rightRes
              case (leftRes: RelationColumn[_], rightRes: RelationColumn[_]) =>
                val bitset = new BitSet
                view.indicies.foreach(f => bitset += f)
                bitset
              case _ =>
                throw new RelationalException(
                  "return type from RelationColumnView#executeBinaryExpression must be BitSet or RelationColumn")
            }
          case BOOLOR =>
            val l = executeBinaryExpression(view, left.asInstanceOf[BinaryExpressionElement])
            val r = executeBinaryExpression(view, right.asInstanceOf[BinaryExpressionElement])
            (l, r) match {
              case (leftRes: BitSet, rightRes: BitSet)               => leftRes.|(rightRes)
              case (leftRes: BitSet, rightRes) if (rightRes != null) => leftRes
              case (leftRes, rightRes: BitSet) if (leftRes != null)  => rightRes
              case (leftRes: RelationColumn[_], rightRes: RelationColumn[_]) =>
                val bitset = new BitSet
                view.indicies.foreach(f => bitset += f)
                bitset
              case _ =>
                throw new RelationalException(
                  "return type from RelationColumnView#executeBinaryExpression must be BitSet or RelationColumn")
            }
          case PLUS  => plus(executeBinaryExpression(view, left), executeBinaryExpression(view, right), view.indicies)
          case DIV   => div(executeBinaryExpression(view, left), executeBinaryExpression(view, right), view.indicies)
          case MINUS => minus(executeBinaryExpression(view, left), executeBinaryExpression(view, right), view.indicies)
          case MUL   => mul(executeBinaryExpression(view, left), executeBinaryExpression(view, right), view.indicies)
          case MODULO =>
            modulo(executeBinaryExpression(view, left), executeBinaryExpression(view, right), view.indicies)
        }
        result
      case _ =>
        ValueFetch.getColumnBasedValue(view, element)
    }
  }

  /**
   * compare RelationColumn with another value which may be RelationColumn or single value
   */
  private def equals(l: Any, r: Any, indicies: List[Int]): BitSet = {
    if ((l == null && r == null) || (l == null && r != null) || (l != null && r == null))
      new BitSet()
    else
      (l, r) match {
        case (left: RelationColumn[_], right) => left.equals(right, indicies)
        case (left, right: RelationColumn[_]) => right.equals(left, indicies)
        case _ =>
          throw new RelationalException(
            s"at least one of the left and right should be RelationColumn type, but found left: ${l.getClass} right: ${r.getClass}")
      }
  }

  private def notEquals(l: Any, r: Any, indicies: List[Int]): BitSet = {
    val temp = equals(l, r, indicies)
    val returnBitSet = new BitSet
    if (temp == null || temp.isEmpty)
      indicies.foreach(f => returnBitSet += f)
    else
      indicies.foreach(f => if (!temp.contains(f)) returnBitSet += f)
    returnBitSet
  }

  private def lt(l: Any, r: Any, indicies: List[Int]): BitSet = {
    if ((l == null && r == null) || (l == null && r != null) || (l != null && r == null))
      new BitSet()
    else
      (l, r) match {
        case (left: RelationColumn[_], right) => left.lt(right, indicies)
        case (left, right: RelationColumn[_]) => right.lt(left, indicies)
        case _ =>
          throw new RelationalException(
            s"at least one of the left and right should be RelationColumn type, but found left: ${l.getClass} right: ${r.getClass}")
      }
  }

  private def le(l: Any, r: Any, indicies: List[Int]): BitSet = {
    val temp = gt(l, r, indicies)
    val returnBitSet = new BitSet
    if (temp == null || temp.isEmpty)
      indicies.foreach(f => returnBitSet += f)
    else
      indicies.foreach(f => if (!temp.contains(f)) returnBitSet += f)
    returnBitSet
  }

  private def gt(l: Any, r: Any, indicies: List[Int]): BitSet = {
    if ((l == null && r == null) || (l == null && r != null) || (l != null && r == null))
      new BitSet()
    else
      (l, r) match {
        case (left: RelationColumn[_], right) => left.gt(right, indicies)
        case (left, right: RelationColumn[_]) => right.gt(left, indicies)
        case _ =>
          throw new RelationalException(
            s"at least one of the left and right should be RelationColumn type, but found left: ${l.getClass} right: ${r.getClass}")
      }
  }

  private def ge(l: Any, r: Any, indicies: List[Int]): BitSet = {
    val temp = lt(l, r, indicies)
    val returnBitSet = new BitSet
    if (temp == null || temp.isEmpty)
      indicies.foreach(f => returnBitSet += f)
    else
      indicies.foreach(f => if (!temp.contains(f)) returnBitSet += f)
    returnBitSet
  }

  /**
   * return a temp RelationColumn whose name has no meaning and not all values of which are useful
   */
  private def plus(l: Any, r: Any, indicies: List[Int]): Any = {
    (l, r) match {
      case (left: RelationColumn[_], right) => left.plus(right, indicies)
      case (left, right: RelationColumn[_]) => right.plus(left, indicies)
      case (l: Int, r: Int)                 => l + r
      case (l: Long, r: Long)               => l + r
      case (l: Float, r: Float)             => l + r
      case (l: Double, r: Double)           => l + r
      case (l: Short, r: Short)             => l + r
      case (l: Byte, r: Byte)               => l + r
      case (_, _) => throw new RelationalException(s"type not support in plus: ${l.getClass} ${r.getClass}")
    }
  }

  private def minus(l: Any, r: Any, indicies: List[Int]): Any = {
    (l, r) match {
      case (left: RelationColumn[_], right) => left.minus(right, indicies)
      case (left, right: RelationColumn[_]) => right.minus(left, indicies)
      case (l: Int, r: Int)                 => l - r
      case (l: Long, r: Long)               => l - r
      case (l: Float, r: Float)             => l - r
      case (l: Double, r: Double)           => l - r
      case (l: Short, r: Short)             => l - r
      case (l: Byte, r: Byte)               => l - r
      case (_, _) => throw new RelationalException(s"type not support in minus: ${l.getClass} ${r.getClass}")
    }
  }

  private def div(l: Any, r: Any, indicies: List[Int]): Any = {
    (l, r) match {
      case (left: RelationColumn[_], right) => left.div(right, indicies)
      case (left, right: RelationColumn[_]) => right.div(left, indicies)
      case (l: Int, r: Int)                 => l / r
      case (l: Long, r: Long)               => l / r
      case (l: Float, r: Float)             => l / r
      case (l: Double, r: Double)           => l / r
      case (l: Short, r: Short)             => l / r
      case (l: Byte, r: Byte)               => l / r
      case (_, _) => throw new RelationalException(s"type not support in div: ${l.getClass} ${r.getClass}")
    }
  }

  private def mul(l: Any, r: Any, indicies: List[Int]): Any = {
    (l, r) match {
      case (left: RelationColumn[_], right) => left.mul(right, indicies)
      case (left, right: RelationColumn[_]) => right.mul(left, indicies)
      case (l: Int, r: Int)                 => l * r
      case (l: Long, r: Long)               => l * r
      case (l: Float, r: Float)             => l * r
      case (l: Double, r: Double)           => l * r
      case (l: Short, r: Short)             => l * r
      case (l: Byte, r: Byte)               => l * r
      case (_, _) => throw new RelationalException(s"type not support in mul: ${l.getClass} ${r.getClass}")
    }
  }

  private def modulo(l: Any, r: Any, indicies: List[Int]): Any = {
    (l, r) match {
      case (left: RelationColumn[_], right) => left.modulo(right, indicies)
      case (left, right: RelationColumn[_]) => right.modulo(left, indicies)
      case (l: Int, r: Int)                 => l % r
      case (l: Long, r: Long)               => l % r
      case (l: Float, r: Float)             => l % r
      case (l: Double, r: Double)           => l % r
      case (l: Short, r: Short)             => l % r
      case (l: Byte, r: Byte)               => l % r
      case (_, _) => throw new RelationalException(s"type not support in modulo: ${l.getClass} ${r.getClass}")
    }
  }

}

class RelationColumnSource(val shapeType: TypeInfo[_], val columns: List[RelationColumn[_]]) {}

/**
 * since we use reflection to get the values of a column at run time which will return Object(Any), so we cannot specify
 * the specific type T as the columnType: Class[T]. It will cause mismatch between columnType:Class[T] and columnValues:
 * Array[Any] when create a RelationalColumn at run time
 */
class RelationColumn[@specialized T](val columnType: Class[_], val columnName: String, val columnValues: Array[T])(
    implicit columnOp: Operator[T]) {

  /**
   * two situations:
   *
   * #1 two columns compare: should compare column type and column value size and every column value
   *
   * #2 one is column and another is a single value: should compare every column value with this single value
   *
   * indices: which is used to fetch the available column value of that column
   */
  def lt(right: Any, indicies: List[Int]): BitSet = {
    if (right == null) {
      new BitSet()
    } else {
      right match {
        case r: RelationColumn[T] =>
          if (!columnType.equals(r.columnType) || columnValues.size != r.columnValues.size)
            new BitSet()
          else {
            columnOp.ltArray(columnValues, r.columnValues, indicies)
          }
        case _ => columnOp.ltSingle(columnValues, right, indicies)
      }
    }
  }

  def gt(right: Any, indicies: List[Int]): BitSet = {
    if (right == null) {
      new BitSet()
    } else {
      right match {
        case r: RelationColumn[T] =>
          if (!columnType.equals(r.columnType) || columnValues.size != r.columnValues.size)
            new BitSet()
          else {
            columnOp.gtArray(columnValues, r.columnValues, indicies)
          }
        case _ => columnOp.gtSingle(columnValues, right, indicies)
      }
    }
  }

  /**
   * two situations:
   *
   * #1 two columns compare: should compare column type and column value size and every column value
   *
   * #2 one is column and another is a single value: should compare every column value with this single value
   */
  def equals(right: Any, indicies: List[Int]): BitSet = {
    if (right == null) {
      new BitSet()
    } else {
      right match {
        case r: RelationColumn[T] =>
          if (!columnType.equals(r.columnType) || columnValues.size != r.columnValues.size)
            new BitSet()
          else {
            columnOp.eqArray(columnValues, r.columnValues, indicies)
          }
        case _ => columnOp.eqSingle(columnValues, right, indicies)
      }
    }
  }

  /**
   * two situations:
   *
   * #1 two columns compare: should compare column type and column value size and every column value
   *
   * #2 one is column and another is a single value: should add every column value with this single value
   *
   * indicies: which is used to fetch the available column value of that column
   */
  def plus(right: Any, indicies: List[Int]): RelationColumn[_] = {
    if (right == null) {
      throw new RelationalException("right side of plus is null")
    }
    right match {
      case r: RelationColumn[T] =>
        if (!columnType.equals(r.columnType) || columnValues.size != r.columnValues.size)
          null
        else {
          columnOp.plusArray(columnValues, r.columnValues, indicies)
        }
      case _ => columnOp.plusSingle(columnValues, right, indicies)
    }
  }

  def minus(right: Any, indicies: List[Int]): RelationColumn[_] = {
    if (right == null) {
      throw new RelationalException("right side of minus is null")
    }
    right match {
      case r: RelationColumn[T] =>
        if (!columnType.equals(r.columnType) || columnValues.size != r.columnValues.size)
          null
        else {
          columnOp.minusArray(columnValues, r.columnValues, indicies)
        }
      case _ => columnOp.minusSingle(columnValues, right, indicies)
    }
  }

  def mul(right: Any, indicies: List[Int]): RelationColumn[_] = {
    if (right == null) {
      throw new RelationalException("right side of mul is null")
    }
    right match {
      case r: RelationColumn[T] =>
        if (!columnType.equals(r.columnType) || columnValues.size != r.columnValues.size)
          null
        else {
          columnOp.mulArray(columnValues, r.columnValues, indicies)
        }
      case _ => columnOp.mulSingle(columnValues, right, indicies)
    }
  }

  def div(right: Any, indicies: List[Int]): RelationColumn[Double] = {
    if (right == null) {
      throw new RelationalException("right side of div is null")
    }
    right match {
      case r: RelationColumn[T] =>
        if (!columnType.equals(r.columnType) || columnValues.size != r.columnValues.size)
          null
        else {
          columnOp.divArray(columnValues, r.columnValues, indicies)
        }
      case _ => columnOp.divSingle(columnValues, right, indicies)
    }
  }

  def modulo(right: Any, indicies: List[Int]): RelationColumn[_] = {
    if (right == null) {
      throw new RelationalException("right side of modulo is null")
    }
    right match {
      case r: RelationColumn[T] =>
        if (!columnType.equals(r.columnType) || columnValues.size != r.columnValues.size)
          null
        else {
          columnOp.moduloArray(columnValues, r.columnValues, indicies)
        }
      case _ => columnOp.moduloSingle(columnValues, right, indicies)
    }
  }

  /**
   * generate a new array whose values are filtered using indices, so all the values are useful, it is only used on
   * extend, cast, project which needs to create a totally new RelationColumnSet
   */
  def purifyColumnValues(indicies: List[Int]): Array[T] = {
    columnOp.generateNewColumnValues(columnValues, indicies)
  }

}

trait Operator[@specialized T] {
  def ltArray(t1: Array[T], t2: Array[T], indicies: List[Int]): BitSet
  def ltSingle(t1: Array[T], t2: Any, indicies: List[Int]): BitSet
  def gtArray(t1: Array[T], t2: Array[T], indicies: List[Int]): BitSet
  def gtSingle(t1: Array[T], t2: Any, indicies: List[Int]): BitSet
  def eqArray(t1: Array[T], t2: Array[T], indicies: List[Int]): BitSet
  def eqSingle(t1: Array[T], t2: Any, indicies: List[Int]): BitSet

  /**
   * result array size is t1.size instead of indices' size so not all of these values are useful only we get from
   * indices. we need to keep the whole size instead of real valuable size since further process will operate on the
   * result array based on indices.
   */
  def plusArray(t1: Array[T], t2: Array[T], indicies: List[Int]): RelationColumn[_]
  def plusSingle(t1: Array[T], t2: Any, indicies: List[Int]): RelationColumn[_]
  def minusArray(t1: Array[T], t2: Array[T], indicies: List[Int]): RelationColumn[_]
  def minusSingle(t1: Array[T], t2: Any, indicies: List[Int]): RelationColumn[_]
  def mulArray(t1: Array[T], t2: Array[T], indicies: List[Int]): RelationColumn[_]
  def mulSingle(t1: Array[T], t2: Any, indicies: List[Int]): RelationColumn[_]
  def divArray(t1: Array[T], t2: Array[T], indicies: List[Int]): RelationColumn[Double]
  def divSingle(t1: Array[T], t2: Any, indicies: List[Int]): RelationColumn[Double]
  def moduloArray(t1: Array[T], t2: Array[T], indicies: List[Int]): RelationColumn[_]
  def moduloSingle(t1: Array[T], t2: Any, indicies: List[Int]): RelationColumn[_]

  /**
   * result array size is indices' size so all values are useful
   */
  def generateNewColumnValues(t1: Array[T], indicies: List[Int]): Array[T]
}

object Operator {

  implicit object ArrayDoubleOperator extends Operator[Double] {
    override def ltArray(t1: Array[Double], t2: Array[Double], indicies: List[Int]): BitSet = {
      val bitset = new BitSet
      indicies.foreach(f => {
        if (t1(f) < t2(f))
          bitset += f
      })
      bitset
    }
    override def ltSingle(t1: Array[Double], t2: Any, indicies: List[Int]): BitSet = {
      val bitset = new BitSet
      if (t2.isInstanceOf[Double] || t2.isInstanceOf[java.lang.Double]) {
        val t = t2.asInstanceOf[Double]
        indicies.foreach(f => {
          if (t1(f) < t)
            bitset += f
        })
        bitset
      } else throw new RelationalException(" t in ArrayDoubleOperator is " + t2.getClass + " but expexted Double")
    }

    override def gtArray(t1: Array[Double], t2: Array[Double], indicies: List[Int]): BitSet = {
      val bitset = new BitSet
      indicies.foreach(f => {
        if (t1(f) > t2(f))
          bitset += f
      })
      bitset
    }
    override def gtSingle(t1: Array[Double], t2: Any, indicies: List[Int]): BitSet = {
      val bitset = new BitSet
      if (t2.isInstanceOf[Double] || t2.isInstanceOf[java.lang.Double]) {
        val t = t2.asInstanceOf[Double]
        indicies.foreach(f => {
          if (t1(f) > t)
            bitset += f
        })
        bitset
      } else throw new RelationalException(" t in ArrayDoubleOperator is " + t2.getClass + " but expexted Double")
    }

    override def eqArray(t1: Array[Double], t2: Array[Double], indicies: List[Int]): BitSet = {
      val bitset = new BitSet
      indicies.foreach(f => {
        if (java.lang.Double.compare(t1(f), t2(f)) == 0)
          bitset += f
      })
      bitset
    }

    override def eqSingle(t1: Array[Double], t2: Any, indicies: List[Int]): BitSet = {
      val bitSet = new BitSet
      if (t2.isInstanceOf[Double] || t2.isInstanceOf[java.lang.Double]) {
        val t = t2.asInstanceOf[Double]
        indicies.foreach(f => {
          if (java.lang.Double.compare(t1(f), t) == 0)
            bitSet += f
        })
        bitSet
      } else throw new RelationalException(" t in ArrayDoubleOperator is " + t2.getClass + " but expexted Double")
    }

    override def plusArray(t1: Array[Double], t2: Array[Double], indicies: List[Int]): RelationColumn[Double] = {
      val temp = new Array[Double](t1.length)
      indicies.foreach(f => {
        temp(f) = t1(f) + t2(f)
      })
      new RelationColumn(classOf[Double], "temparrayplus", temp)
    }

    override def plusSingle(t1: Array[Double], t2: Any, indicies: List[Int]): RelationColumn[Double] = {
      if (t2.isInstanceOf[Double] || t2.isInstanceOf[java.lang.Double]) {
        val temp = new Array[Double](t1.length)
        val t = t2.asInstanceOf[Double]
        indicies.foreach(f => {
          temp(f) = t1(f) + t
        })
        new RelationColumn(classOf[Double], "tempsingleplus", temp)
      } else throw new RelationalException(" t in ArrayDoubleOperator is " + t2.getClass + " but expexted Double")
    }

    override def minusArray(t1: Array[Double], t2: Array[Double], indicies: List[Int]): RelationColumn[Double] = {
      val temp = new Array[Double](t1.size)
      indicies.foreach(f => {
        temp(f) = t1(f) - t2(f)
      })
      new RelationColumn(classOf[Double], "temparrayminus", temp)
    }

    override def minusSingle(t1: Array[Double], t2: Any, indicies: List[Int]): RelationColumn[Double] = {
      if (t2.isInstanceOf[Double] || t2.isInstanceOf[java.lang.Double]) {
        val temp = new Array[Double](t1.length)
        val t = t2.asInstanceOf[Double]
        indicies.foreach(f => {
          temp(f) = t1(f) - t
        })
        new RelationColumn(classOf[Double], "tempsingleminus", temp)
      } else throw new RelationalException(" t in ArrayDoubleOperator is " + t2.getClass + " but expexted Double")
    }

    override def mulArray(t1: Array[Double], t2: Array[Double], indicies: List[Int]): RelationColumn[Double] = {
      val temp = new Array[Double](t1.size)
      indicies.foreach(f => {
        temp(f) = t1(f) * t2(f)
      })
      new RelationColumn(classOf[Double], "temparraymul", temp)
    }

    override def mulSingle(t1: Array[Double], t2: Any, indicies: List[Int]): RelationColumn[Double] = {
      if (t2.isInstanceOf[Double] || t2.isInstanceOf[java.lang.Double]) {
        val temp = new Array[Double](t1.length)
        val t = t2.asInstanceOf[Double]
        indicies.foreach(f => {
          temp(f) = t1(f) * t
        })
        new RelationColumn(classOf[Double], "tempsinglemul", temp)
      } else throw new RelationalException(" t in ArrayDoubleOperator is " + t2.getClass + " but expexted Double")
    }

    override def divArray(t1: Array[Double], t2: Array[Double], indicies: List[Int]): RelationColumn[Double] = {
      val temp = new Array[Double](t1.size)
      indicies.foreach(f => {
        if (t2(f) != 0)
          temp(f) = t1(f) / t2(f)
        else throw new RelationalException(" t2(f) value in ArrayDoubleOperator div is zero")
      })
      new RelationColumn(classOf[Double], "temparraydiv", temp)
    }

    override def divSingle(t1: Array[Double], t2: Any, indicies: List[Int]): RelationColumn[Double] = {
      if (t2 != 0 && (t2.isInstanceOf[Double] || t2.isInstanceOf[java.lang.Double])) {
        val temp = new Array[Double](t1.length)
        val t = t2.asInstanceOf[Double]
        indicies.foreach(f => {
          temp(f) = t1(f) / t
        })
        new RelationColumn(classOf[Double], "tempsinglediv", temp)
      } else
        throw new RelationalException(
          " t in ArrayDoubleOperator is " + t2.getClass + " but expexted Double and t value is " + t2)
    }

    override def moduloArray(t1: Array[Double], t2: Array[Double], indicies: List[Int]): RelationColumn[Double] = {
      val temp = new Array[Double](t1.size)
      indicies.foreach(f => {
        if (t2(f) != 0)
          temp(f) = t1(f) % t2(f)
        else throw new RelationalException(" t2(f) value in ArrayDoubleOperator div is zero")
      })
      new RelationColumn(classOf[Double], "temparraymodulo", temp)
    }

    override def moduloSingle(t1: Array[Double], t2: Any, indicies: List[Int]): RelationColumn[Double] = {
      if (t2 != 0 && (t2.isInstanceOf[Double] || t2.isInstanceOf[java.lang.Double])) {
        val temp = new Array[Double](t1.length)
        val t = t2.asInstanceOf[Double]
        indicies.foreach(f => {
          temp(f) = t1(f) / t
        })
        new RelationColumn(classOf[Double], "tempsinglemodulo", temp)
      } else
        throw new RelationalException(
          " t in ArrayDoubleOperator is " + t2.getClass + " but expexted Double and t value is " + t2)
    }

    override def generateNewColumnValues(t1: Array[Double], indicies: List[Int]): Array[Double] = {
      val newArray = new Array[Double](indicies.size)
      var i = 0
      indicies.foreach(f => {
        newArray(i) = t1(f)
        i += 1
      })
      newArray
    }
  }

  implicit object ArrayIntOperator extends Operator[Int] {

    override def ltArray(t1: Array[Int], t2: Array[Int], indicies: List[Int]): BitSet = {
      val bitset = new BitSet
      indicies.foreach(f => {
        if (t1(f) < t2(f))
          bitset += f
      })
      bitset
    }
    override def ltSingle(t1: Array[Int], t2: Any, indicies: List[Int]): BitSet = {
      val bitset = new BitSet
      if (t2.isInstanceOf[Int] || t2.isInstanceOf[java.lang.Integer]) {
        val t = t2.asInstanceOf[Int]
        indicies.foreach(f => {
          if (t1(f) < t)
            bitset += f
        })
        bitset
      } else throw new RelationalException(" t in ArrayIntOperator is " + t2.getClass + " but expexted Integer or Int")
    }

    override def gtArray(t1: Array[Int], t2: Array[Int], indicies: List[Int]): BitSet = {
      val bitset = new BitSet
      indicies.foreach(f => {
        if (t1(f) > t2(f))
          bitset += f
      })
      bitset
    }
    override def gtSingle(t1: Array[Int], t2: Any, indicies: List[Int]): BitSet = {
      val bitset = new BitSet
      if (t2.isInstanceOf[Int] || t2.isInstanceOf[java.lang.Integer]) {
        val t = t2.asInstanceOf[Int]
        indicies.foreach(f => {
          if (t1(f) > t)
            bitset += f
        })
        bitset
      } else throw new RelationalException(" t in ArrayIntOperator is " + t2.getClass + " but expexted Integer or Int")
    }

    override def eqArray(t1: Array[Int], t2: Array[Int], indicies: List[Int]): BitSet = {
      val bitset = new BitSet
      indicies.foreach(f => {
        if (t1(f) == t2(f))
          bitset += f
      })
      bitset
    }

    override def eqSingle(t1: Array[Int], t2: Any, indicies: List[Int]): BitSet = {
      val bitset = new BitSet
      if (t2.isInstanceOf[Int] || t2.isInstanceOf[java.lang.Integer]) {
        val t = t2.asInstanceOf[Int]
        indicies.foreach(f => {
          if (t1(f) == t)
            bitset += f
        })
        bitset
      } else throw new RelationalException(" t in ArrayIntOperator is " + t2.getClass + " but expexted Integer or Int")
    }

    override def plusArray(t1: Array[Int], t2: Array[Int], indicies: List[Int]): RelationColumn[Int] = {
      val temp = new Array[Int](t1.size)
      indicies.foreach(f => {
        temp(f) = t1(f) + t2(f)
      })
      new RelationColumn(classOf[Int], "temparrayplus", temp)
    }

    override def plusSingle(t1: Array[Int], t2: Any, indicies: List[Int]): RelationColumn[Int] = {
      if (t2.isInstanceOf[Int] || t2.isInstanceOf[java.lang.Integer]) {
        val temp = new Array[Int](t1.length)
        val t = t2.asInstanceOf[Int]
        indicies.foreach(f => {
          temp(f) = t1(f) + t
        })
        new RelationColumn(classOf[Int], "tempsingleplus", temp)
      } else throw new RelationalException(" t in ArrayIntOperator is " + t2.getClass + " but expexted Integer")
    }

    override def minusArray(t1: Array[Int], t2: Array[Int], indicies: List[Int]): RelationColumn[Int] = {
      val temp = new Array[Int](t1.size)
      indicies.foreach(f => {
        temp(f) = t1(f) - t2(f)
      })
      new RelationColumn(classOf[Int], "temparrayminus", temp)
    }

    override def minusSingle(t1: Array[Int], t2: Any, indicies: List[Int]): RelationColumn[Int] = {
      if (t2.isInstanceOf[Int] || t2.isInstanceOf[java.lang.Integer]) {
        val temp = new Array[Int](t1.length)
        val t = t2.asInstanceOf[Int]
        indicies.foreach(f => {
          temp(f) = t1(f) - t
        })
        new RelationColumn(classOf[Int], "tempsingleminus", temp)
      } else throw new RelationalException(" t in ArrayIntOperator is " + t2.getClass + " but expexted Integer")
    }

    override def mulArray(t1: Array[Int], t2: Array[Int], indicies: List[Int]): RelationColumn[Int] = {
      val temp = new Array[Int](t1.size)
      indicies.foreach(f => {
        temp(f) = t1(f) * t2(f)
      })
      new RelationColumn(classOf[Int], "temparraymul", temp)
    }

    override def mulSingle(t1: Array[Int], t2: Any, indicies: List[Int]): RelationColumn[Int] = {
      if (t2.isInstanceOf[Int] || t2.isInstanceOf[java.lang.Integer]) {
        val temp = new Array[Int](t1.length)
        val t = t2.asInstanceOf[Int]
        indicies.foreach(f => {
          temp(f) = t1(f) * t
        })
        new RelationColumn(classOf[Int], "tempsinglemul", temp)
      } else throw new RelationalException(" t in ArrayIntOperator is " + t2.getClass + " but expexted Integer")
    }

    override def divArray(t1: Array[Int], t2: Array[Int], indicies: List[Int]): RelationColumn[Double] = {
      val temp = new Array[Double](t1.size)
      indicies.foreach(f => {
        if (t2(f) != 0)
          temp(f) = (t1(f) / t2(f)).toDouble
        else throw new RelationalException(" t2(f) value in ArrayIntOperator div is zero")
      })
      new RelationColumn(classOf[Double], "temparraydiv", temp)
    }

    override def divSingle(t1: Array[Int], t2: Any, indicies: List[Int]): RelationColumn[Double] = {
      if (t2 != 0 && (t2.isInstanceOf[Int] || t2.isInstanceOf[java.lang.Integer])) {
        val temp = new Array[Double](t1.length)
        val t = t2.asInstanceOf[Int]
        indicies.foreach(f => {
          temp(f) = (t1(f) / t).toDouble
        })
        new RelationColumn(classOf[Double], "tempsinglediv", temp)
      } else
        throw new RelationalException(
          " t in ArrayIntOperator is " + t2.getClass + " but expexted Integer and t value is " + t2)
    }

    override def moduloArray(t1: Array[Int], t2: Array[Int], indicies: List[Int]): RelationColumn[Int] = {
      val temp = new Array[Int](t1.size)
      indicies.foreach(f => {
        if (t2(f) != 0)
          temp(f) = t1(f) % t2(f)
        else throw new RelationalException(" t2(f) value in ArrayIntOperator div is zero")
      })
      new RelationColumn(classOf[Int], "temparraymodulo", temp)
    }

    override def moduloSingle(t1: Array[Int], t2: Any, indicies: List[Int]): RelationColumn[Int] = {
      if (t2 != 0 && (t2.isInstanceOf[Int] || t2.isInstanceOf[java.lang.Integer])) {
        val temp = new Array[Int](t1.length)
        val t = t2.asInstanceOf[Int]
        indicies.foreach(f => {
          temp(f) = t1(f) / t
        })
        new RelationColumn(classOf[Int], "tempsinglemodulo", temp)
      } else
        throw new RelationalException(
          " t in ArrayIntOperator is " + t2.getClass + " but expexted Integer and t value is " + t2)
    }

    override def generateNewColumnValues(t1: Array[Int], indicies: List[Int]): Array[Int] = {
      val newArray = new Array[Int](indicies.size)
      var i = 0
      indicies.foreach(f => {
        newArray(i) = t1(f)
        i += 1
      })
      newArray
    }
  }

  implicit object ArrayLongOperator extends Operator[Long] {
    override def ltArray(t1: Array[Long], t2: Array[Long], indicies: List[Int]): BitSet = {
      val bitset = new BitSet()
      indicies.foreach(f => {
        if (t1(f) < t2(f))
          bitset += f
      })
      bitset
    }
    override def ltSingle(t1: Array[Long], t2: Any, indicies: List[Int]): BitSet = {
      val bitset = new BitSet()
      if (t2.isInstanceOf[Long] || t2.isInstanceOf[java.lang.Long]) {
        val t = t2.asInstanceOf[Long]
        indicies.foreach(f => {
          if (t1(f) < t)
            bitset += f
        })
        bitset
      } else throw new RelationalException(" t in ArrayLongOperator is " + t2.getClass + " but expexted Long")
    }

    override def gtArray(t1: Array[Long], t2: Array[Long], indicies: List[Int]): BitSet = {
      val bitset = new BitSet
      indicies.foreach(f => {
        if (t1(f) > t2(f))
          bitset += f
      })
      bitset
    }
    override def gtSingle(t1: Array[Long], t2: Any, indicies: List[Int]): BitSet = {
      val bitset = new BitSet
      if (t2.isInstanceOf[Long] || t2.isInstanceOf[java.lang.Long]) {
        val t = t2.asInstanceOf[Long]
        indicies.foreach(f => {
          if (t1(f) > t)
            bitset += f
        })
        bitset
      } else throw new RelationalException(" t in ArrayLongOperator is " + t2.getClass + " but expexted Long")
    }

    override def eqArray(t1: Array[Long], t2: Array[Long], indicies: List[Int]): BitSet = {
      val bitset = new BitSet()
      indicies.foreach(f => {
        if (t1(f) == t2(f))
          bitset += f
      })
      bitset
    }

    override def eqSingle(t1: Array[Long], t2: Any, indicies: List[Int]): BitSet = {
      val bitset = new BitSet
      if (t2.isInstanceOf[Long] || t2.isInstanceOf[java.lang.Long]) {
        val t = t2.asInstanceOf[Long]
        indicies.foreach(f => {
          if (t1(f) == t)
            bitset += f
        })
        bitset
      } else throw new RelationalException(s"t in ArrayLongOperator is ${t2.getClass} but expexted Long")
    }

    override def plusArray(t1: Array[Long], t2: Array[Long], indicies: List[Int]): RelationColumn[Long] = {
      val temp = new Array[Long](t1.length)
      indicies.foreach(f => {
        temp(f) = t1(f) + t2(f)
      })
      new RelationColumn(classOf[Long], "temparrayplus", temp)
    }

    override def plusSingle(t1: Array[Long], t2: Any, indicies: List[Int]): RelationColumn[Long] = {
      if (t2.isInstanceOf[Long] || t2.isInstanceOf[java.lang.Long]) {
        val temp = new Array[Long](t1.length)
        val t = t2.asInstanceOf[Long]
        indicies.foreach(f => {
          temp(f) = t1(f) + t
        })
        new RelationColumn(classOf[Long], "tempsingleplus", temp)
      } else throw new RelationalException(" t in ArrayLongOperator is " + t2.getClass + " but expexted Long")
    }

    override def minusArray(t1: Array[Long], t2: Array[Long], indicies: List[Int]): RelationColumn[Long] = {
      val temp = new Array[Long](t1.size)
      indicies.foreach(f => {
        temp(f) = t1(f) - t2(f)
      })
      new RelationColumn(classOf[Long], "temparrayminus", temp)
    }

    override def minusSingle(t1: Array[Long], t2: Any, indicies: List[Int]): RelationColumn[Long] = {
      if (t2.isInstanceOf[Long] || t2.isInstanceOf[java.lang.Long]) {
        val temp = new Array[Long](t1.length)
        val t = t2.asInstanceOf[Long]
        indicies.foreach(f => {
          temp(f) = t1(f) - t
        })
        new RelationColumn(classOf[Long], "tempsingleminus", temp)
      } else throw new RelationalException(" t in ArrayLongOperator is " + t2.getClass + " but expexted Long")
    }

    override def mulArray(t1: Array[Long], t2: Array[Long], indicies: List[Int]): RelationColumn[Long] = {
      val temp = new Array[Long](t1.size)
      indicies.foreach(f => {
        temp(f) = t1(f) * t2(f)
      })
      new RelationColumn(classOf[Long], "temparraymul", temp)
    }

    override def mulSingle(t1: Array[Long], t2: Any, indicies: List[Int]): RelationColumn[Long] = {
      if (t2.isInstanceOf[Long] || t2.isInstanceOf[java.lang.Long]) {
        val temp = new Array[Long](t1.length)
        val t = t2.asInstanceOf[Long]
        indicies.foreach(f => {
          temp(f) = t1(f) * t
        })
        new RelationColumn(classOf[Long], "tempsinglemul", temp)
      } else throw new RelationalException(" t in ArrayLongOperator is " + t2.getClass + " but expexted Long")
    }

    override def divArray(t1: Array[Long], t2: Array[Long], indicies: List[Int]): RelationColumn[Double] = {
      val temp = new Array[Double](t1.size)
      indicies.foreach(f => {
        if (t2(f) != 0)
          temp(f) = (t1(f) / t2(f)).toDouble
        else throw new RelationalException(" t2(f) value in ArrayLongOperator div is zero")
      })
      new RelationColumn(classOf[Double], "temparraydiv", temp)
    }

    override def divSingle(t1: Array[Long], t2: Any, indicies: List[Int]): RelationColumn[Double] = {
      if (t2 != 0 && (t2.isInstanceOf[Long] || t2.isInstanceOf[java.lang.Long])) {
        val temp = new Array[Double](t1.length)
        val t = t2.asInstanceOf[Long]
        indicies.foreach(f => {
          temp(f) = (t1(f) / t).toDouble
        })
        new RelationColumn(classOf[Double], "tempsinglediv", temp)
      } else
        throw new RelationalException(
          " t in ArrayLongOperator is " + t2.getClass + " but expexted Long and t value is " + t2)
    }

    override def moduloArray(t1: Array[Long], t2: Array[Long], indicies: List[Int]): RelationColumn[Long] = {
      val temp = new Array[Long](t1.size)
      indicies.foreach(f => {
        if (t2(f) != 0)
          temp(f) = t1(f) % t2(f)
        else throw new RelationalException(" t2(f) value in ArrayLongOperator div is zero")
      })
      new RelationColumn(classOf[Long], "temparraymodulo", temp)
    }

    override def moduloSingle(t1: Array[Long], t2: Any, indicies: List[Int]): RelationColumn[Long] = {
      if (t2 != 0 && (t2.isInstanceOf[Long] || t2.isInstanceOf[java.lang.Long])) {
        val temp = new Array[Long](t1.length)
        val t = t2.asInstanceOf[Long]
        indicies.foreach(f => {
          temp(f) = t1(f) / t
        })
        new RelationColumn(classOf[Long], "tempsinglemodulo", temp)
      } else
        throw new RelationalException(
          " t in ArrayLongOperator is " + t2.getClass + " but expexted Integer and t value is " + t2)
    }

    override def generateNewColumnValues(t1: Array[Long], indicies: List[Int]): Array[Long] = {
      val newArray = new Array[Long](indicies.size)
      var i = 0
      indicies.foreach(f => {
        newArray(i) = t1(f)
        i += 1
      })
      newArray
    }
  }

  implicit object ArrayFloatOperator extends Operator[Float] {
    override def ltArray(t1: Array[Float], t2: Array[Float], indicies: List[Int]): BitSet = {
      val bitset = new BitSet
      indicies.foreach(f => {
        if (t1(f) < t2(f))
          bitset += f
      })
      bitset
    }
    override def ltSingle(t1: Array[Float], t2: Any, indicies: List[Int]): BitSet = {
      val bitset = new BitSet
      if (t2.isInstanceOf[Float] || t2.isInstanceOf[java.lang.Float]) {
        val t = t2.asInstanceOf[Float]
        indicies.foreach(f => {
          if (t1(f) < t)
            bitset += f
        })
        bitset
      } else throw new RelationalException(" t in ArrayFloatOperator is " + t2.getClass + " but expexted Float")
    }

    override def gtArray(t1: Array[Float], t2: Array[Float], indicies: List[Int]): BitSet = {
      val bitset = new BitSet
      indicies.foreach(f => {
        if (t1(f) > t2(f))
          bitset += f
      })
      bitset
    }
    override def gtSingle(t1: Array[Float], t2: Any, indicies: List[Int]): BitSet = {
      val bitset = new BitSet
      if (t2.isInstanceOf[Float] || t2.isInstanceOf[java.lang.Float]) {
        val t = t2.asInstanceOf[Float]
        indicies.foreach(f => {
          if (t1(f) > t)
            bitset += f
        })
        bitset
      } else throw new RelationalException(" t in ArrayFloatOperator is " + t2.getClass + " but expexted Float")
    }

    override def eqArray(t1: Array[Float], t2: Array[Float], indicies: List[Int]): BitSet = {
      val bitset = new BitSet
      indicies.foreach(f => {
        if (t1(f) == t2(f))
          bitset += f
      })
      bitset
    }

    override def eqSingle(t1: Array[Float], t2: Any, indicies: List[Int]): BitSet = {
      val bitSet = new BitSet
      if (t2.isInstanceOf[Float] || t2.isInstanceOf[java.lang.Float]) {
        val t = t2.asInstanceOf[Float]
        indicies.foreach(f => {
          if (t1(f) == t)
            bitSet += f
        })
        bitSet
      } else throw new RelationalException(s"t in ArrayFloatOperator is ${t2.getClass} but expexted Float")
    }

    override def plusArray(t1: Array[Float], t2: Array[Float], indicies: List[Int]): RelationColumn[Float] = {
      val temp = new Array[Float](t1.length)
      indicies.foreach(f => {
        temp(f) = t1(f) + t2(f)
      })
      new RelationColumn(classOf[Float], "temparrayplus", temp)
    }

    override def plusSingle(t1: Array[Float], t2: Any, indicies: List[Int]): RelationColumn[Float] = {
      if (t2.isInstanceOf[Float] || t2.isInstanceOf[java.lang.Float]) {
        val temp = new Array[Float](t1.size)
        val t = t2.asInstanceOf[Float]
        indicies.foreach(f => {
          temp(f) = t1(f) + t
        })
        new RelationColumn(classOf[Float], "tempsingleplus", temp)
      } else throw new RelationalException(" t in ArrayFloatOperator is " + t2.getClass + " but expexted Float")
    }

    override def minusArray(t1: Array[Float], t2: Array[Float], indicies: List[Int]): RelationColumn[Float] = {
      val temp = new Array[Float](t1.size)
      indicies.foreach(f => {
        temp(f) = t1(f) - t2(f)
      })
      new RelationColumn(classOf[Float], "temparrayminus", temp)
    }

    override def minusSingle(t1: Array[Float], t2: Any, indicies: List[Int]): RelationColumn[Float] = {
      if (t2.isInstanceOf[Float] || t2.isInstanceOf[java.lang.Float]) {
        val temp = new Array[Float](t1.length)
        val t = t2.asInstanceOf[Float]
        indicies.foreach(f => {
          temp(f) = t1(f) - t
        })
        new RelationColumn(classOf[Float], "tempsingleminus", temp)
      } else throw new RelationalException(" t in ArrayFloatOperator is " + t2.getClass + " but expexted Float")
    }

    override def mulArray(t1: Array[Float], t2: Array[Float], indicies: List[Int]): RelationColumn[Float] = {
      val temp = new Array[Float](t1.size)
      indicies.foreach(f => {
        temp(f) = t1(f) * t2(f)
      })
      new RelationColumn(classOf[Float], "temparraymul", temp)
    }

    override def mulSingle(t1: Array[Float], t2: Any, indicies: List[Int]): RelationColumn[Float] = {
      if (t2.isInstanceOf[Float] || t2.isInstanceOf[java.lang.Float]) {
        val temp = new Array[Float](t1.length)
        val t = t2.asInstanceOf[Float]
        indicies.foreach(f => {
          temp(f) = t1(f) * t
        })
        new RelationColumn(classOf[Float], "tempsinglemul", temp)
      } else throw new RelationalException(" t in ArrayFloatOperator is " + t2.getClass + " but expexted Float")
    }

    override def divArray(t1: Array[Float], t2: Array[Float], indicies: List[Int]): RelationColumn[Double] = {
      val temp = new Array[Double](t1.size)
      indicies.foreach(f => {
        if (t2(f) != 0)
          temp(f) = t1(f) / t2(f)
        else throw new RelationalException(" t2(f) value in ArrayFloatOperator div is zero")
      })
      new RelationColumn(classOf[Double], "temparraydiv", temp)
    }

    override def divSingle(t1: Array[Float], t2: Any, indicies: List[Int]): RelationColumn[Double] = {
      if (t2 != 0 && (t2.isInstanceOf[Float] || t2.isInstanceOf[java.lang.Float])) {
        val temp = new Array[Double](t1.length)
        val t = t2.asInstanceOf[Float]
        indicies.foreach(f => {
          temp(f) = t1(f) / t
        })
        new RelationColumn(classOf[Double], "tempsinglediv", temp)
      } else
        throw new RelationalException(
          " t in ArrayFloatOperator is " + t2.getClass + " but expexted Float and t value is " + t2)
    }

    override def moduloArray(t1: Array[Float], t2: Array[Float], indicies: List[Int]): RelationColumn[Float] = {
      val temp = new Array[Float](t1.size)
      indicies.foreach(f => {
        if (t2(f) != 0)
          temp(f) = t1(f) % t2(f)
        else throw new RelationalException(" t2(f) value in ArrayLongOperator div is zero")
      })
      new RelationColumn(classOf[Float], "temparraymodulo", temp)
    }

    override def moduloSingle(t1: Array[Float], t2: Any, indicies: List[Int]): RelationColumn[Float] = {
      if (t2 != 0 && (t2.isInstanceOf[Float] || t2.isInstanceOf[java.lang.Float])) {
        val temp = new Array[Float](t1.length)
        val t = t2.asInstanceOf[Float]
        indicies.foreach(f => {
          temp(f) = t1(f) / t
        })
        new RelationColumn(classOf[Float], "tempsinglemodulo", temp)
      } else
        throw new RelationalException(
          " t in ArrayFloatOperator is " + t2.getClass + " but expexted Integer and t value is " + t2)
    }

    override def generateNewColumnValues(t1: Array[Float], indicies: List[Int]): Array[Float] = {
      val newArray = new Array[Float](indicies.size)
      var i = 0
      indicies.foreach(f => {
        newArray(i) = t1(f)
        i += 1
      })
      newArray
    }
  }

  implicit object ArrayShortOperator extends Operator[Short] {
    override def ltArray(t1: Array[Short], t2: Array[Short], indicies: List[Int]): BitSet = {
      val bitset = new BitSet
      indicies.foreach(f => {
        if (t1(f) < t2(f))
          bitset += f
      })
      bitset
    }
    override def ltSingle(t1: Array[Short], t2: Any, indicies: List[Int]): BitSet = {
      val bitset = new BitSet
      if (t2.isInstanceOf[Short] || t2.isInstanceOf[java.lang.Short]) {
        val t = t2.asInstanceOf[Short]
        indicies.foreach(f => {
          if (t1(f) < t)
            bitset += f
        })
        bitset
      } else throw new RelationalException(" t in ArrayShortOperator is " + t2.getClass + " but expexted Short")
    }

    override def gtArray(t1: Array[Short], t2: Array[Short], indicies: List[Int]): BitSet = {
      val bitset = new BitSet
      indicies.foreach(f => {
        if (t1(f) > t2(f))
          bitset += f
      })
      bitset
    }
    override def gtSingle(t1: Array[Short], t2: Any, indicies: List[Int]): BitSet = {
      val bitset = new BitSet
      if (t2.isInstanceOf[Short] || t2.isInstanceOf[java.lang.Short]) {
        val t = t2.asInstanceOf[Short]
        indicies.foreach(f => {
          if (t1(f) > t)
            bitset += f
        })
        bitset
      } else throw new RelationalException(" t in ArrayDoubleOperator is " + t2.getClass + " but expexted Double")
    }

    override def eqArray(t1: Array[Short], t2: Array[Short], indicies: List[Int]): BitSet = {
      val bitset = new BitSet
      indicies.foreach(f => {
        if (t1(f) == t2(f))
          bitset += f
      })
      bitset
    }

    override def eqSingle(t1: Array[Short], t2: Any, indicies: List[Int]): BitSet = {
      val bitSet = new BitSet
      if (t2.isInstanceOf[Short] || t2.isInstanceOf[java.lang.Short]) {
        val t = t2.asInstanceOf[Short]
        indicies.foreach(f => {
          if (t1(f) == t)
            bitSet += f
        })
        bitSet
      } else throw new RelationalException(" t in ArrayShortOperator is " + t2.getClass + " but expexted Short")
    }

    override def plusArray(t1: Array[Short], t2: Array[Short], indicies: List[Int]): RelationColumn[Int] = {
      val temp = new Array[Int](t1.length)
      indicies.foreach(f => {
        temp(f) = t1(f) + t2(f)
      })
      new RelationColumn(classOf[Int], "temparrayplus", temp)
    }

    override def plusSingle(t1: Array[Short], t2: Any, indicies: List[Int]): RelationColumn[Int] = {
      if (t2.isInstanceOf[Short] || t2.isInstanceOf[java.lang.Short]) {
        val temp = new Array[Int](t1.length)
        val t = t2.asInstanceOf[Short]
        indicies.foreach(f => {
          temp(f) = t1(f) + t
        })
        new RelationColumn(classOf[Int], "tempsingleplus", temp)
      } else throw new RelationalException(" t in ArrayShortOperator is " + t2.getClass + " but expexted Short")
    }

    override def minusArray(t1: Array[Short], t2: Array[Short], indicies: List[Int]): RelationColumn[Int] = {
      val temp = new Array[Int](t1.size)
      indicies.foreach(f => {
        temp(f) = t1(f) - t2(f)
      })
      new RelationColumn(classOf[Int], "temparrayminus", temp)
    }

    override def minusSingle(t1: Array[Short], t2: Any, indicies: List[Int]): RelationColumn[Int] = {
      if (t2.isInstanceOf[Short] || t2.isInstanceOf[java.lang.Short]) {
        val temp = new Array[Int](t1.length)
        val t = t2.asInstanceOf[Short]
        indicies.foreach(f => {
          temp(f) = t1(f) - t
        })
        new RelationColumn(classOf[Int], "tempsingleminus", temp)
      } else throw new RelationalException(" t in ArrayShortOperator is " + t2.getClass + " but expexted Short")
    }

    override def mulArray(t1: Array[Short], t2: Array[Short], indicies: List[Int]): RelationColumn[Int] = {
      val temp = new Array[Int](t1.size)
      indicies.foreach(f => {
        temp(f) = t1(f) * t2(f)
      })
      new RelationColumn(classOf[Int], "temparraymul", temp)
    }

    override def mulSingle(t1: Array[Short], t2: Any, indicies: List[Int]): RelationColumn[Int] = {
      if (t2.isInstanceOf[Short] || t2.isInstanceOf[java.lang.Short]) {
        val temp = new Array[Int](t1.length)
        val t = t2.asInstanceOf[Short]
        indicies.foreach(f => {
          temp(f) = t1(f) * t
        })
        new RelationColumn(classOf[Int], "tempsinglemul", temp)
      } else throw new RelationalException(" t in ArrayShortOperator is " + t2.getClass + " but expexted Short")
    }

    override def divArray(t1: Array[Short], t2: Array[Short], indicies: List[Int]): RelationColumn[Double] = {
      val temp = new Array[Double](t1.size)
      indicies.foreach(f => {
        if (t2(f) != 0)
          temp(f) = (t1(f) / t2(f)).toDouble
        else throw new RelationalException(" t2(f) value in ArrayShortOperator div is zero")
      })
      new RelationColumn(classOf[Double], "temparraydiv", temp)
    }

    override def divSingle(t1: Array[Short], t2: Any, indicies: List[Int]): RelationColumn[Double] = {
      if (t2 != 0 && (t2.isInstanceOf[Double] || t2.isInstanceOf[java.lang.Double])) {
        val temp = new Array[Double](t1.length)
        val t = t2.asInstanceOf[Short]
        indicies.foreach(f => {
          temp(f) = (t1(f) / t).toDouble
        })
        new RelationColumn(classOf[Double], "tempsinglediv", temp)
      } else
        throw new RelationalException(
          " t in ArrayShortOperator is " + t2.getClass + " but expexted Short and t value is " + t2)
    }

    override def moduloArray(t1: Array[Short], t2: Array[Short], indicies: List[Int]): RelationColumn[Int] = {
      val temp = new Array[Int](t1.size)
      indicies.foreach(f => {
        if (t2(f) != 0)
          temp(f) = t1(f) % t2(f)
        else throw new RelationalException(" t2(f) value in ArrayShortOperator div is zero")
      })
      new RelationColumn(classOf[Int], "temparraymodulo", temp)
    }

    override def moduloSingle(t1: Array[Short], t2: Any, indicies: List[Int]): RelationColumn[Int] = {
      if (t2 != 0 && (t2.isInstanceOf[Short] || t2.isInstanceOf[java.lang.Short])) {
        val temp = new Array[Int](t1.length)
        val t = t2.asInstanceOf[Short]
        indicies.foreach(f => {
          temp(f) = t1(f) / t
        })
        new RelationColumn(classOf[Int], "tempsinglemodulo", temp)
      } else
        throw new RelationalException(
          " t in ArrayShortOperator is " + t2.getClass + " but expexted Short and t value is " + t2)
    }

    override def generateNewColumnValues(t1: Array[Short], indicies: List[Int]): Array[Short] = {
      val newArray = new Array[Short](indicies.size)
      var i = 0
      indicies.foreach(f => {
        newArray(i) = t1(f)
        i += 1
      })
      newArray
    }
  }

  implicit object ArrayByteOperator extends Operator[Byte] {
    override def ltArray(t1: Array[Byte], t2: Array[Byte], indicies: List[Int]): BitSet = {
      val bitset = new BitSet
      indicies.foreach(f => {
        if (t1(f) < t2(f))
          bitset += f
      })
      bitset
    }

    override def ltSingle(t1: Array[Byte], t2: Any, indicies: List[Int]): BitSet = {
      val bitset = new BitSet
      if (t2.isInstanceOf[Byte] || t2.isInstanceOf[java.lang.Byte]) {
        val t = t2.asInstanceOf[Byte]
        indicies.foreach(f => {
          if (t1(f) < t)
            bitset += f
        })
        bitset
      } else throw new RelationalException(" t in ArrayByteOperator is " + t2.getClass + " but expexted Byte")
    }

    override def gtArray(t1: Array[Byte], t2: Array[Byte], indicies: List[Int]): BitSet = {
      val bitset = new BitSet
      indicies.foreach(f => {
        if (t1(f) > t2(f))
          bitset += f
      })
      bitset
    }
    override def gtSingle(t1: Array[Byte], t2: Any, indicies: List[Int]): BitSet = {
      val bitset = new BitSet
      if (t2.isInstanceOf[Byte] || t2.isInstanceOf[java.lang.Byte]) {
        val t = t2.asInstanceOf[Byte]
        indicies.foreach(f => {
          if (t1(f) > t)
            bitset += f
        })
        bitset
      } else throw new RelationalException(" t in ArrayByteOperator is " + t2.getClass + " but expexted Byte")
    }

    override def eqArray(t1: Array[Byte], t2: Array[Byte], indicies: List[Int]): BitSet = {
      val bitset = new BitSet
      indicies.foreach(f => {
        if (t1(f) == t2(f))
          bitset += f
      })
      bitset
    }

    override def eqSingle(t1: Array[Byte], t2: Any, indicies: List[Int]): BitSet = {
      val bitSet = new BitSet
      if (t2.isInstanceOf[Byte] || t2.isInstanceOf[java.lang.Byte]) {
        val t = t2.asInstanceOf[Byte]
        indicies.foreach(f => {
          if (t1(f) == t)
            bitSet += f
        })
        bitSet
      } else throw new RelationalException(" t in ArrayByteOperator is " + t2.getClass + " but expexted Byte")
    }

    override def plusArray(t1: Array[Byte], t2: Array[Byte], indicies: List[Int]): RelationColumn[Byte] = {
      val temp = new Array[Int](t1.length)
      indicies.foreach(f => {
        temp(f) = t1(f) + t2(f)
      })
      new RelationColumn(classOf[Short], "temparrayplus", temp.asInstanceOf[Array[Byte]])
    }

    override def plusSingle(t1: Array[Byte], t2: Any, indicies: List[Int]): RelationColumn[Byte] = {
      if (t2.isInstanceOf[Byte] || t2.isInstanceOf[java.lang.Byte]) {
        val temp = new Array[Int](t1.length)
        val t = t2.asInstanceOf[Byte]
        indicies.foreach(f => {
          temp(f) = t1(f) + t
        })
        new RelationColumn(classOf[Long], "tempsingleplus", temp.asInstanceOf[Array[Byte]])
      } else throw new RelationalException(" t in ArrayByteOperator is " + t2.getClass + " but expexted Byte")
    }

    override def minusArray(t1: Array[Byte], t2: Array[Byte], indicies: List[Int]): RelationColumn[Int] = {
      val temp = new Array[Int](t1.size)
      indicies.foreach(f => {
        temp(f) = t1(f) - t2(f)
      })
      new RelationColumn(classOf[Int], "temparrayminus", temp)
    }

    override def minusSingle(t1: Array[Byte], t2: Any, indicies: List[Int]): RelationColumn[Int] = {
      if (t2.isInstanceOf[Byte] || t2.isInstanceOf[java.lang.Byte]) {
        val temp = new Array[Int](t1.length)
        val t = t2.asInstanceOf[Short]
        indicies.foreach(f => {
          temp(f) = t1(f) - t
        })
        new RelationColumn(classOf[Int], "tempsingleminus", temp)
      } else throw new RelationalException(" t in ArrayByteOperator is " + t2.getClass + " but expexted Byte")
    }

    override def mulArray(t1: Array[Byte], t2: Array[Byte], indicies: List[Int]): RelationColumn[Int] = {
      val temp = new Array[Int](t1.size)
      indicies.foreach(f => {
        temp(f) = t1(f) * t2(f)
      })
      new RelationColumn(classOf[Int], "temparraymul", temp)
    }

    override def mulSingle(t1: Array[Byte], t2: Any, indicies: List[Int]): RelationColumn[Int] = {
      if (t2.isInstanceOf[Byte] || t2.isInstanceOf[java.lang.Byte]) {
        val temp = new Array[Int](t1.length)
        val t = t2.asInstanceOf[Byte]
        indicies.foreach(f => {
          temp(f) = t1(f) * t
        })
        new RelationColumn(classOf[Int], "tempsinglemul", temp)
      } else throw new RelationalException(" t in ArrayByteOperator is " + t2.getClass + " but expexted Short")
    }

    override def divArray(t1: Array[Byte], t2: Array[Byte], indicies: List[Int]): RelationColumn[Double] = {
      val temp = new Array[Double](t1.size)
      indicies.foreach(f => {
        if (t2(f) != 0)
          temp(f) = (t1(f) / t2(f)).toDouble
        else throw new RelationalException(" t2(f) value in ArrayByteOperator div is zero")
      })
      new RelationColumn(classOf[Double], "temparraydiv", temp)
    }

    override def divSingle(t1: Array[Byte], t2: Any, indicies: List[Int]): RelationColumn[Double] = {
      if (t2 != 0 && (t2.isInstanceOf[Byte] || t2.isInstanceOf[java.lang.Byte])) {
        val temp = new Array[Double](t1.length)
        val t = t2.asInstanceOf[Byte]
        indicies.foreach(f => {
          temp(f) = (t1(f) / t).toDouble
        })
        new RelationColumn(classOf[Double], "tempsinglediv", temp)
      } else
        throw new RelationalException(
          " t in ArrayByteOperator is " + t2.getClass + " but expexted Byte and t value is " + t2)
    }

    override def moduloArray(t1: Array[Byte], t2: Array[Byte], indicies: List[Int]): RelationColumn[Int] = {
      val temp = new Array[Int](t1.size)
      indicies.foreach(f => {
        if (t2(f) != 0)
          temp(f) = t1(f) % t2(f)
        else throw new RelationalException(" t2(f) value in ArrayByteOperator div is zero")
      })
      new RelationColumn(classOf[Int], "temparraymodulo", temp)
    }

    override def moduloSingle(t1: Array[Byte], t2: Any, indicies: List[Int]): RelationColumn[Int] = {
      if (t2 != 0 && (t2.isInstanceOf[Byte] || t2.isInstanceOf[java.lang.Byte])) {
        val temp = new Array[Int](t1.length)
        val t = t2.asInstanceOf[Byte]
        indicies.foreach(f => {
          temp(f) = t1(f) / t
        })
        new RelationColumn(classOf[Int], "tempsinglemodulo", temp)
      } else
        throw new RelationalException(s"t in ArrayByteOperator is ${t2.getClass} but expexted Byte and t value is $t2")
    }

    override def generateNewColumnValues(t1: Array[Byte], indicies: List[Int]): Array[Byte] = {
      val newArray = new Array[Byte](indicies.size)
      var i = 0
      indicies.foreach(f => {
        newArray(i) = t1(f)
        i += 1
      })
      newArray
    }
  }

  implicit object ArrayCharOperator extends Operator[Char] {
    override def ltArray(t1: Array[Char], t2: Array[Char], indicies: List[Int]): BitSet = {
      val bitSet = new BitSet
      indicies.foreach(f => {
        if (t1(f) < t2(f))
          bitSet += f
      })
      bitSet
    }
    override def ltSingle(t1: Array[Char], t2: Any, indicies: List[Int]): BitSet = {
      val bitSet = new BitSet
      if (t2.isInstanceOf[Char] || t2.isInstanceOf[java.lang.Character]) {
        val t = t2.asInstanceOf[Char]
        indicies.foreach(f => {
          if (t1(f) < t)
            bitSet += f
        })
        bitSet
      } else throw new RelationalException(s"t in ArrayCharOperator is ${t2.getClass} but expexted Char or Character")
    }

    override def gtArray(t1: Array[Char], t2: Array[Char], indicies: List[Int]): BitSet = {
      val bitset = new BitSet
      indicies.foreach(f => {
        if (t1(f) > t2(f))
          bitset += f
      })
      bitset
    }

    override def gtSingle(t1: Array[Char], t2: Any, indicies: List[Int]): BitSet = {
      val bitset = new BitSet
      if (t2.isInstanceOf[Char] || t2.isInstanceOf[java.lang.Character]) {
        val t = t2.asInstanceOf[Char]
        indicies.foreach(f => {
          if (t1(f) > t)
            bitset += f
        })
        bitset
      } else throw new RelationalException(s"t in ArrayCharOperator is ${t2.getClass} but expexted Char")
    }

    override def eqArray(t1: Array[Char], t2: Array[Char], indicies: List[Int]): BitSet = {
      val bitSet = new BitSet
      indicies.foreach(f => {
        if (t1(f) == t2(f))
          bitSet += f
      })
      bitSet
    }

    override def eqSingle(t1: Array[Char], t2: Any, indicies: List[Int]): BitSet = {
      val bitSet = new BitSet
      if (t2.isInstanceOf[Char] || t2.isInstanceOf[java.lang.Character]) {
        val t = t2.asInstanceOf[Char]
        indicies.foreach(f => {
          if (t1(f) == t)
            bitSet += f
        })
        bitSet
      } else throw new RelationalException(s"t in ArrayCharOperator is ${t2.getClass} but expexted Char or Character")
    }

    override def plusArray(t1: Array[Char], t2: Array[Char], indicies: List[Int]): RelationColumn[Char] = {
      throw new RelationalException("plusarray is not supported in char column")
    }

    override def plusSingle(t1: Array[Char], t2: Any, indicies: List[Int]): RelationColumn[Char] = {
      throw new RelationalException("plussingle is not supported in char column")
    }

    override def minusArray(t1: Array[Char], t2: Array[Char], indicies: List[Int]): RelationColumn[Char] = {
      throw new RelationalException("minusArray is not supported in char column")
    }

    override def minusSingle(t1: Array[Char], t2: Any, indicies: List[Int]): RelationColumn[Char] = {
      throw new RelationalException("minusSingle is not supported in char column")
    }

    override def mulArray(t1: Array[Char], t2: Array[Char], indicies: List[Int]): RelationColumn[Char] = {
      throw new RelationalException("mulArray is not supported in char column")
    }

    override def mulSingle(t1: Array[Char], t2: Any, indicies: List[Int]): RelationColumn[Char] = {
      throw new RelationalException("mulSingle is not supported in char column")
    }

    override def divArray(t1: Array[Char], t2: Array[Char], indicies: List[Int]): RelationColumn[Double] = {
      throw new RelationalException("divArray is not supported in char column")
    }

    override def divSingle(t1: Array[Char], t2: Any, indicies: List[Int]): RelationColumn[Double] = {
      throw new RelationalException("divSingle is not supported in char column")
    }

    override def moduloArray(t1: Array[Char], t2: Array[Char], indicies: List[Int]): RelationColumn[Char] = {
      throw new RelationalException("moduloArray is not supported in char column")
    }

    override def moduloSingle(t1: Array[Char], t2: Any, indicies: List[Int]): RelationColumn[Char] = {
      throw new RelationalException("moduloSingle is not supported in char column")
    }

    override def generateNewColumnValues(t1: Array[Char], indicies: List[Int]): Array[Char] = {
      val newArray = new Array[Char](indicies.size)
      var i = 0
      indicies.foreach(f => {
        newArray(i) = t1(f)
        i += 1
      })
      newArray
    }
  }

  implicit object ArrayStringOperator extends Operator[String] {

    override def ltArray(t1: Array[String], t2: Array[String], indicies: List[Int]): BitSet = {
      val bitset = new BitSet
      indicies.foreach(f => {
        if (t1(f).asInstanceOf[Comparable[String]].compareTo(t2(f)) < 0)
          bitset += f
      })
      bitset
    }
    override def ltSingle(t1: Array[String], t2: Any, indicies: List[Int]): BitSet = {
      val bitset = new BitSet
      t2 match {
        case str: String =>
          indicies.foreach(f => {
            if (t1(f).asInstanceOf[Comparable[String]].compareTo(str) < 0)
              bitset += f
          })
          bitset
        case _ => throw new RelationalException(s"t in ArrayByteOperator is ${t2.getClass} but expexted String")
      }
    }

    override def gtArray(t1: Array[String], t2: Array[String], indicies: List[Int]): BitSet = {
      val bitset = new BitSet
      indicies.foreach(f => {
        if (t1(f).asInstanceOf[Comparable[String]].compareTo(t2(f)) > 0)
          bitset += f
      })
      bitset
    }
    override def gtSingle(t1: Array[String], t2: Any, indicies: List[Int]): BitSet = {
      val bitset = new BitSet
      t2 match {
        case str: String =>
          indicies.foreach(f => {
            if (t1(f).asInstanceOf[Comparable[String]].compareTo(str) > 0)
              bitset += f
          })
          bitset
        case _ => throw new RelationalException(s"t in ArrayByteOperator is ${t2.getClass} but expexted String")
      }
    }

    override def eqArray(t1: Array[String], t2: Array[String], indicies: List[Int]): BitSet = {
      val bitset = new BitSet
      indicies.foreach(f => {
        if (t1(f).equals(t2(f)))
          bitset += f
      })
      bitset
    }

    override def eqSingle(t1: Array[String], t2: Any, indicies: List[Int]): BitSet = {
      val bitset = new BitSet
      indicies.foreach(f => {
        if (t1(f).equals(t2))
          bitset += f
      })
      bitset
    }

    override def plusArray(t1: Array[String], t2: Array[String], indicies: List[Int]): RelationColumn[String] = {
      throw new RelationalException("plusArray is not supported in String column")
    }

    override def plusSingle(t1: Array[String], t2: Any, indicies: List[Int]): RelationColumn[String] = {
      throw new RelationalException("plusSingle is not supported in String column")
    }

    override def minusArray(t1: Array[String], t2: Array[String], indicies: List[Int]): RelationColumn[String] = {
      throw new RelationalException("minusArray is not supported in String column")
    }

    override def minusSingle(t1: Array[String], t2: Any, indicies: List[Int]): RelationColumn[String] = {
      throw new RelationalException("minusSingle is not supported in String column")
    }

    override def mulArray(t1: Array[String], t2: Array[String], indicies: List[Int]): RelationColumn[String] = {
      throw new RelationalException("mulArray is not supported in String column")
    }

    override def mulSingle(t1: Array[String], t2: Any, indicies: List[Int]): RelationColumn[String] = {
      throw new RelationalException("mulSingle is not supported in String column")
    }

    override def divArray(t1: Array[String], t2: Array[String], indicies: List[Int]): RelationColumn[Double] = {
      throw new RelationalException("divArray is not supported in String column")
    }

    override def divSingle(t1: Array[String], t2: Any, indicies: List[Int]): RelationColumn[Double] = {
      throw new RelationalException("divSingle is not supported in String column")
    }

    override def moduloArray(t1: Array[String], t2: Array[String], indicies: List[Int]): RelationColumn[String] = {
      throw new RelationalException("moduloArray is not supported in String column")
    }

    override def moduloSingle(t1: Array[String], t2: Any, indicies: List[Int]): RelationColumn[String] = {
      throw new RelationalException("moduloSingle is not supported in String column")
    }

    override def generateNewColumnValues(t1: Array[String], indicies: List[Int]): Array[String] = {
      val newArray = new Array[String](indicies.size)
      var i = 0
      indicies.foreach(f => {
        newArray(i) = t1(f)
        i += 1
      })
      newArray
    }
  }

  implicit object ArrayObjectOperator extends Operator[Any] {
    override def ltArray(t1: Array[Any], t2: Array[Any], indicies: List[Int]): BitSet = {
      val bitset = new BitSet
      indicies.foreach(f => {
        if (t1(f).asInstanceOf[Comparable[Any]].compareTo(t2(f)) < 0)
          bitset += f
      })
      bitset
    }
    override def ltSingle(t1: Array[Any], t2: Any, indicies: List[Int]): BitSet = {
      val bitset = new BitSet
      indicies.foreach(f => {
        if (t1(f).asInstanceOf[Comparable[Any]].compareTo(t2) < 0)
          bitset += f
      })
      bitset
    }

    override def gtArray(t1: Array[Any], t2: Array[Any], indicies: List[Int]): BitSet = {
      val bitset = new BitSet
      indicies.foreach(f => {
        if (t1(f).asInstanceOf[Comparable[Any]].compareTo(t2(f)) > 0)
          bitset += f
      })
      bitset
    }
    override def gtSingle(t1: Array[Any], t2: Any, indicies: List[Int]): BitSet = {
      val bitset = new BitSet
      indicies.foreach(f => {
        if (t1(f).asInstanceOf[Comparable[Any]].compareTo(t2) > 0)
          bitset += f
      })
      bitset
    }

    override def eqArray(t1: Array[Any], t2: Array[Any], indicies: List[Int]): BitSet = {
      val bitset = new BitSet
      indicies.foreach(f => {
        if (t1(f).equals(t2(f)))
          bitset += f
      })
      bitset
    }

    override def eqSingle(t1: Array[Any], t2: Any, indicies: List[Int]): BitSet = {
      val bitset = new BitSet
      indicies.foreach(f => {
        if (t1(f).equals(t2))
          bitset += f
      })
      bitset
    }

    override def plusArray(t1: Array[Any], t2: Array[Any], indicies: List[Int]): RelationColumn[Any] = {
      throw new RelationalException("plusArray is not supported in Any column")
    }

    override def plusSingle(t1: Array[Any], t2: Any, indicies: List[Int]): RelationColumn[Any] = {
      throw new RelationalException("plusSingle is not supported in Any column")
    }

    override def minusArray(t1: Array[Any], t2: Array[Any], indicies: List[Int]): RelationColumn[Any] = {
      throw new RelationalException("minusArray is not supported in Any column")
    }

    override def minusSingle(t1: Array[Any], t2: Any, indicies: List[Int]): RelationColumn[Any] = {
      throw new RelationalException("minusSingle is not supported in Any column")
    }

    override def mulArray(t1: Array[Any], t2: Array[Any], indicies: List[Int]): RelationColumn[Any] = {
      throw new RelationalException("mulArray is not supported in Any column")
    }

    override def mulSingle(t1: Array[Any], t2: Any, indicies: List[Int]): RelationColumn[Any] = {
      throw new RelationalException("mulSingle is not supported in Any column")
    }

    override def divArray(t1: Array[Any], t2: Array[Any], indicies: List[Int]): RelationColumn[Double] = {
      throw new RelationalException("divArray is not supported in Any column")
    }

    override def divSingle(t1: Array[Any], t2: Any, indicies: List[Int]): RelationColumn[Double] = {
      throw new RelationalException("divSingle is not supported in Any column")
    }

    override def moduloArray(t1: Array[Any], t2: Array[Any], indicies: List[Int]): RelationColumn[Any] = {
      throw new RelationalException("moduloArray is not supported in Any column")
    }

    override def moduloSingle(t1: Array[Any], t2: Any, indicies: List[Int]): RelationColumn[Any] = {
      throw new RelationalException("moduloSingle is not supported in Any column")
    }

    override def generateNewColumnValues(t1: Array[Any], indicies: List[Int]): Array[Any] = {
      val newArray = new Array[Any](indicies.size)
      var i = 0
      indicies.foreach(f => {
        newArray(i) = t1(f)
        i += 1
      })
      newArray
    }
  }
}

object RelationColumn {

  /**
   * used by other class which wants to create a relation column from a single value
   */
  def generateRelationColumnFromSingleValue[T](
      columnType: Class[T],
      columnName: String,
      columnValue: Any,
      length: Int): RelationColumn[_] = {
    columnValue match {
      case a: java.lang.Integer if columnType == classOf[Int] =>
        val arr = Array.fill(length)(a.intValue())
        new RelationColumn(columnType, columnName, arr)
      case a: java.lang.Long if columnType == classOf[Long] =>
        val arr = Array.fill(length)(a.longValue())
        new RelationColumn(columnType, columnName, arr)
      case a: java.lang.Double if columnType == classOf[Double] =>
        val arr = Array.fill(length)(a.doubleValue())
        new RelationColumn(columnType, columnName, arr)
      case a: java.lang.Float if columnType == classOf[Float] =>
        val arr = Array.fill(length)(a.floatValue())
        new RelationColumn(columnType, columnName, arr)
      case a: java.lang.Short if columnType == classOf[Short] =>
        val arr = Array.fill(length)(a.shortValue())
        new RelationColumn(columnType, columnName, arr)
      case a: java.lang.Byte if columnType == classOf[Byte] =>
        val arr = Array.fill(length)(a.byteValue())
        new RelationColumn(columnType, columnName, arr)
      case a: java.lang.Character if columnType == classOf[Char] =>
        val arr = Array.fill(length)(a.charValue())
        new RelationColumn(columnType, columnName, arr)
      case a: String if columnType == classOf[String] =>
        val arr = Array.fill(length)(a)
        new RelationColumn(columnType, columnName, arr)
      case a =>
        val arr = Array.fill(length)(a)
        new RelationColumn(columnType, columnName, arr)
    }
  }

  /**
   * used by other class which wants to create a relation column from a array but needs to provide a implicit arg
   * explicitly
   */
  def generateRelationColumnFromArray[T](
      columnType: Class[_],
      columnName: String,
      columnValues: Array[T]): RelationColumn[T] = {
    columnValues match {
      case i: Array[Int]    => new RelationColumn(columnType, columnName, i)
      case i: Array[Double] => new RelationColumn(columnType, columnName, i)
      case i: Array[Float]  => new RelationColumn(columnType, columnName, i)
      case i: Array[Long]   => new RelationColumn(columnType, columnName, i)
      case i: Array[Short]  => new RelationColumn(columnType, columnName, i)
      case i: Array[Char]   => new RelationColumn(columnType, columnName, i)
      case i: Array[Byte]   => new RelationColumn(columnType, columnName, i)
      case i: Array[String] => new RelationColumn(columnType, columnName, i)
      case i: Array[Any]    => new RelationColumn(columnType, columnName, i)
      case _                => throw new RelationalException("columnValues type is not expected")
    }
  }

  /**
   * used by other class which wants to generate a new array from an old one filled with the validated element in
   * indices
   */
  def generateNewArrayFromOriginalArray[T](srcArray: Array[T], indicies: List[Int]): Array[T] = {
    var index = 0
    srcArray match {
      case src: Array[Int] => {
        val desArray = new Array[Int](indicies.length); indicies.foreach(f => { desArray(index) = src(f); index += 1 });
        desArray
      }
      case src: Array[Double] => {
        val desArray = new Array[Double](indicies.length);
        indicies.foreach(f => { desArray(index) = src(f); index += 1 }); desArray
      }
      case src: Array[Float] => {
        val desArray = new Array[Float](indicies.length);
        indicies.foreach(f => { desArray(index) = src(f); index += 1 }); desArray
      }
      case src: Array[Long] => {
        val desArray = new Array[Long](indicies.length);
        indicies.foreach(f => { desArray(index) = src(f); index += 1 });
        desArray
      }
      case src: Array[Short] => {
        val desArray = new Array[Short](indicies.length);
        indicies.foreach(f => { desArray(index) = src(f); index += 1 }); desArray
      }
      case src: Array[Char] => {
        val desArray = new Array[Char](indicies.length);
        indicies.foreach(f => { desArray(index) = src(f); index += 1 });
        desArray
      }
      case src: Array[Byte] => {
        val desArray = new Array[Byte](indicies.length);
        indicies.foreach(f => { desArray(index) = src(f); index += 1 });
        desArray
      }
      case src: Array[String] => {
        val desArray = new Array[String](indicies.length);
        indicies.foreach(f => { desArray(index) = src(f); index += 1 }); desArray
      }
      case src: Array[AnyRef] => {
        val desArray = new Array[AnyRef](indicies.length);
        indicies.foreach(f => { desArray(index) = src(f); index += 1 }); desArray
      }
      case _ => throw new RelationalException("srcArray type is not expected")
    }
  }

}

class ColumnOrdering(val members: Array[MemberElement], val isAscs: Array[Boolean], val relation: RelationColumnSource)
    extends Ordering[scala.Int] {
  if (isAscs.size != members.size) {
    throw new RelationalException(
      "the size of MemberElement is not equal to isAscs number when constructing RowOrdering")
  }
  if (relation == null) {
    throw new RelationalException("relation is null so cannot fetch columns to compare")
  }

  val columns = members.map(memberName => {
    relation.columns.find(f => f.columnName.equals(memberName.memberName)).get
  })

  override def compare(left: scala.Int, right: scala.Int): Int = {
    val isize = isAscs.length
    var res: java.lang.Integer = null
    var isAsc: java.lang.Boolean = null
    for (i <- 0 until isize) {
      val lValue = columns(i).columnValues(left)
      val rValue = columns(i).columnValues(right)
      isAsc = isAscs(i)
      (lValue, rValue) match {
        case (l: scala.Short, r: scala.Short) => {
          if (l != r) {
            if (isAsc) return l - r
            else return r - l
          }
        }
        case (l: scala.Int, r: scala.Int) => {
          if (l != r) {
            if (isAsc) return l - r
            else return r - l
          }
        }
        case (l: Long, r: Long) => {
          if (l != r) {
            if (isAsc) return if (l < r) -1 else 1
            else return if (l < r) 1 else -1
          }
        }
        case (l: Double, r: Double) => {
          val res = java.lang.Double.compare(l, r)
          if (res != 0) {
            if (isAsc) return res
            else return -res
          }
        }
        case (l: Float, r: Float) => {
          val res = java.lang.Float.compare(l, r)
          if (res != 0) {
            if (isAsc) return res
            else return -res
          }
        }
        case (l: Comparable[_], r: AnyRef) => {
          val lc = l.asInstanceOf[Comparable[AnyRef]]
          res = lc.compareTo(r)
          if (res != 0) {
            if (isAsc) {
              return res.intValue
            } else {
              return -res.intValue
            }
          }
        }

        case (l, r) => throw new IllegalArgumentException(s"types ${l.getClass} and ${r.getClass} are not Comparable")
      }

    }
    0
  }
}
