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
package optimus.utils
import scala.reflect.macros.whitebox
import scala.reflect.ClassTag

object StringTyping {
  private def getString(c: whitebox.Context)(s: c.Tree): String = {
    import c.universe._
    s match {
      case Literal(Constant(v: String)) => v
      case _ =>
        c.error(s.pos, s"$s is not a string literal")
        ""
    }
  }

  def classString[T](implicit tag: ClassTag[T]): String = tag.toString.replaceAllLiterally(".", "/")

  /**
   * methodFrameName[Clazz]("method") will return "fully/qualified/Clazz.method"
   * Valid for ScalaClass#method, JavaClass#method and a static JavaClass.method.
   * For scala object methods, see objectMethodFrameName below.
   * Turn # into $ so inner classes match java .getClass
   */
  def methodFrameString[X](method: String): String = macro methodFrameString$impl[X]
  def methodFrameString$impl[X: c.WeakTypeTag](c: whitebox.Context)(method: c.Expr[String]): c.Expr[String] = {
    import c.universe._
    val tpe = weakTypeTag[X].tpe
    val m: String = getString(c)(method.tree)
    if (
      // First look for declared members of class
      !tpe.members.exists(_.name.toString == m) &&
      // If tpe is a Java class, then its static methods are in a virtual companion object; if a scala class.
      // For a Scala class, the companion object is truly a different class.
      !(tpe.typeSymbol.isJava && tpe.companion.members.exists(_.name.toString == m))
    )
      c.error(c.enclosingPosition, s"Method $m not found in $tpe")
    val ret = tpe.typeConstructor.toString.replace(".", "/").replace("#", "$") + "." + m
    c.Expr[String](Literal(Constant(ret)))
  }

  /**
   * objectMethodFrameName(MyObject, "method") will return "fully/qualified/MyObject$.method"
   */
  def objectMethodFrameString[X](obj: X, method: String): String = macro objectMethodFrameString$impl[X]
  def objectMethodFrameString$impl[X: c.WeakTypeTag](
      c: whitebox.Context)(obj: c.Expr[X], method: c.Expr[String]): c.Expr[String] = {
    import c.universe._
    val m: String = getString(c)(method.tree)
    val tpe = obj.tree.tpe
    if (!obj.tree.symbol.isModule)
      c.error(obj.tree.pos, s"Object $obj is not a scala object")
    if (!tpe.members.exists(_.name.toString == m))
      c.error(c.enclosingPosition, s"Method $m not found in $tpe")
    val ret = obj.tree.symbol.fullName.replaceAllLiterally(".", "/") + "$." + m
    c.Expr[String](Literal(Constant(ret)))
  }

}
