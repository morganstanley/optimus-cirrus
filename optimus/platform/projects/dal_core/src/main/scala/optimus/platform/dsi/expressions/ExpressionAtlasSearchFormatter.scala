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
package optimus.platform.dsi.expressions

import optimus.graph.DiagnosticSettings

class ExpressionAtlasSearchFormatter(threshold: AbbreviationThreshold)
    extends ExpressionPriQLFormatter(threshold.listThreshold.getOrElse(10)) {

  protected override def visitBinary(b: Binary): Expression = {
    import BinaryOperator._
    b.op match {
      case Equal              => writeBinary("==", b.left, b.right)
      case NotEqual           => writeBinary("!=", b.left, b.right)
      case LessThan           => writeBinary("<", b.left, b.right)
      case LessThanOrEqual    => writeBinary("<=", b.left, b.right)
      case GreaterThan        => writeBinary(">", b.left, b.right)
      case GreaterThanOrEqual => writeBinary(">=", b.left, b.right)
      case AndAlso            => writeBinary("&&", b.left, b.right)
      case OrElse             => writeBinary("||", b.left, b.right)
      case _ => throw new UnsupportedOperationException(s"Unsupported server side binary expression type: ${b.op}")
    }
    b
  }

  protected override def visitFunction(f: Function): Expression = {
    f match {
      case Function(TextOps.StartsWith.Name, List(p: Property, c: Constant)) =>
        visitProperty(p)
        write(".startsWith(")
        visitConstant(c)
        write(")")
        f
      case Function(TextOps.EndsWith.Name, List(p: Property, c: Constant)) =>
        visitProperty(p)
        write(".startsWith(")
        visitConstant(c)
        write(")")
        f
      case Function(TextOps.Contains.Name, List(p: Property, c: Constant)) =>
        visitProperty(p)
        write(".contains(")
        visitConstant(c)
        write(")")
        f
      case _ =>
        super.visitFunction(f)
    }
  }

}

object ExpressionAtlasSearchFormatter {
  val MaxThreshold = AbbreviationThreshold(None, None)
  lazy val DefaultThreshold = AbbreviationThreshold(
    Option(DiagnosticSettings.getIntProperty("optimus.priql.print.expressionListAbbreviationThreshold", 10)),
    Option(DiagnosticSettings.getIntProperty("optimus.priql.print.expressionWhereAbbreviationThreshold", 500))
  )

  def format(e: Expression, threshold: AbbreviationThreshold = DefaultThreshold): String = {
    val formatter = new ExpressionAtlasSearchFormatter(threshold)
    formatter.visit(OptionValueEvaluator.visit(e))
    formatter.toString
  }
}
