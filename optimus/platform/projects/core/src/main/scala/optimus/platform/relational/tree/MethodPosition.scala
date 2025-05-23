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

import optimus.utils.MacroUtils.relativeSourcePath

import scala.reflect.macros.whitebox.Context

final case class MethodPosition(posInfo: String) {
  def orElse(pos: MethodPosition): MethodPosition = if (posInfo.isEmpty) pos else this
  def lineNum: Int = fileNameLineAndColumn._2

  def fileNameLineAndColumn: (String, Int, Int) =
    try {
      val details = posInfo.split(',')
      val file = details(0).split('/').last
      val line = details(1).split('-')(1).toInt
      val col = details(3).split('=')(1).toInt
      (file, line, col)
    } catch {
      case _: Exception => (posInfo, 0, 0)
    }

  // non ascii except for ., _, -
  def cleanName: String = {
    val (file, line, col) = fileNameLineAndColumn
    s"$file-$line-$col"
  }

  // for IDE console navigation
  def clickableName: String = {
    val (file, line, _) = fileNameLineAndColumn
    s".($file:$line)"
  }
}

/**
 * MethodPosition represents position information of a method, such as the file path, line and column number.
 */
object MethodPosition {
  implicit def position: MethodPosition = macro positionImpl

  def positionImpl(c: Context): c.Expr[MethodPosition] = {
    import c.universe._
    val pos = c.macroApplication.pos.focus
    val posInfo = "" + pos + ",column=" + c.macroApplication.pos.column
    val sourcePath = pos.source.file.canonicalPath
    val relativePosInfo = posInfo.replace(sourcePath, relativeSourcePath(c)(pos))
    val posExpr = c.Expr[String](Literal(Constant(relativePosInfo)))
    c.universe.reify { new MethodPosition(posExpr.splice) }
  }

  val unknown = new MethodPosition("")
}
