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
package optimus.platform.relational.tree

import java.util.concurrent.atomic.AtomicLong

import scala.collection.mutable.ListBuffer

object RelationElementCnt {
  private val cnt = new AtomicLong(0)
  def next = cnt.incrementAndGet
  def prev = cnt.get()
}

abstract class RelationElement(
    val elementType: ElementType,
    val rowTypeInfo: TypeInfo[_],
    val pos: MethodPosition = MethodPosition.unknown) {

  val serial: Long = RelationElementCnt.next

  // Numerical hashCode is dirt cheap.
  override def hashCode() = serial.hashCode()

  def getType(): ElementType = elementType
  def projectedType(): TypeInfo[_] = rowTypeInfo

  def prettyPrint(indent: Int, out: StringBuilder): Unit = {
    indentAndStar(indent, out)
    out ++= (s"no print overload has been provided for this type (${elementType}) \n")
  }

  override def toString = dbgPrint()

  def dbgPrint(): String = {
    val sb = StringBuilder.newBuilder
    prettyPrint(0, sb)
    sb.toString()
  }

  def indentAndStar(indent: Int, out: StringBuilder): Unit =
    for (i <- 0 until indent) out ++= "  "

  /**
   * ************************************************************
   */
  /**
   * ***following is used for ExecNode to print query explain****
   */
  /**
   * ************************************************************
   */
  /**
   * sometimes it's useful when we need more info, such as get execute_time, sort according to one field
   */
  def printQueryExplain(): String = {

    val table = new ListBuffer[QueryExplainItem]
    fillQueryExplainItem(1, table)
    /*
     * use priql to sort this table
     */

    /*
     * output table entries to sb
     */
    generateQueryExplain(table)

  }

  def generateQueryExplain(table: ListBuffer[QueryExplainItem]): String = {
    var level: Int = 0
    val sb = StringBuilder.newBuilder

    if (!table.isEmpty) {
      val row = table(0)
      val maxLengths = new ListBuffer[Integer]
      row.fillHeaderLength(maxLengths)
      table.foreach(row => {
        if (row.level_id > level)
          level = row.level_id
        maxLengths(0) = Math.max(maxLengths(0), getLevelFieldLength(row.level_id))
        maxLengths(1) = Math.max(maxLengths(1), row.operator_type.length)
        maxLengths(3) = Math.max(maxLengths(3), row.operation_detail.split("\\n").map(_.length).max)
      })

      sb ++= "The execution order is 1, 2, 3.... \n"

      row.printHeader(maxLengths.toList, sb)
      var index = 0
      table foreach { f =>
        f.printItself(maxLengths.toList, table.toList, index, level, sb)
        index += 1
      }
    }
    sb.toString
  }

  def getLevelFieldLength(level_id: Int): Int = level_id + level_id.toString.length

  def fillQueryExplainItem(level_id: Integer, table: ListBuffer[QueryExplainItem]): Unit =
    table += new QueryExplainItem(level_id, "AbstractExecNode", "-", "-", 0)
}

/**
 * this class stands for the query plain statistics we want to gather
 */
final case class QueryExplainItem(
    level_id: Integer,
    operator_type: String,
    object_name: String,
    operation_detail: String,
    cost: Integer) {
  def fillHeaderLength(lengths: ListBuffer[Integer]): ListBuffer[Integer] = {
    lengths += "LEVEL_ID".length()
    lengths += "OPERATOR_TYPE".length()
    lengths += 50
    lengths += "OPERATION_DETAIL".length()
  }

  def printHeader(maxFieldLengths: List[Integer], sb: StringBuilder): StringBuilder = {
    sb ++= "LEVEL"
    printIndent(maxFieldLengths(0) - "LEVEL".length(), sb)
    sb ++= "||"
    sb ++= "OPERATOR_TYPE"
    printIndent(maxFieldLengths(1) - "OPERATOR_TYPE".length(), sb)
    sb ++= "||"
    sb ++= "OBJECT_NAME"
    printIndent(maxFieldLengths(2) - "OBJECT_NAME".length(), sb)
    sb ++= "||"
    sb ++= "OPERATION_DETAIL"
    printIndent(maxFieldLengths(3) - "OPERATION_DETAIL".length(), sb)
    sb ++= "||" + "\n"

    maxFieldLengths foreach { f =>
      printStar(f, sb)
      sb ++= "||"
    }

    sb ++= "\n"
  }

  def printItself(
      maxFieldLengths: List[Integer],
      table: List[QueryExplainItem],
      index: Int,
      maxlevel: Int,
      sb: StringBuilder): Unit = {
    val executionId = maxlevel + 1 - level_id
    printStar(executionId, sb)
    sb ++= executionId + ""
    printStar(maxFieldLengths(0) - executionId - executionId.toString.length, sb)

    sb ++= "||" + operator_type
    printIndent(maxFieldLengths(1) - operator_type.length(), sb)

    val objname1 :: objnames = object_name.split(s"(?<=\\G.{${maxFieldLengths(2)}})").toList
    sb ++= "||"
    sb ++= objname1
    printIndent(maxFieldLengths(2) - objname1.length, sb)

    val detail1 :: details = operation_detail.split("\\n").toList
    sb ++= "||"
    sb ++= detail1
    printIndent(maxFieldLengths(3) - detail1.length, sb)
    sb ++= "||\n"

    objnames.zipAll(details, "", "").foreach { case (n, d) =>
      printIndent(maxFieldLengths(0), sb)
      sb ++= "||"
      printIndent(maxFieldLengths(1), sb)
      sb ++= "||"
      sb ++= n
      printIndent(maxFieldLengths(2) - n.length, sb)
      sb ++= "||"
      sb ++= d
      printIndent(maxFieldLengths(3) - d.length, sb)
      sb ++= "||\n"
    }
  }

  private def printIndent(indent: Integer, sb: StringBuilder) = sb ++= " " * indent

  private def printStar(length: Integer, sb: StringBuilder) = sb ++= "-" * length

}
