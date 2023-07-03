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
package optimus.platform.relational.asm

import java.lang.reflect.Constructor
import java.lang.reflect.Executable
import java.lang.reflect.Method
import java.lang.reflect.Modifier
import com.google.common.primitives.Primitives
import optimus.core.CoreAPI
import optimus.graph.Node
import optimus.platform._
import optimus.platform.relational.AbstractLambdaReifier
import optimus.platform.staged
import optimus.platform.staged.scalaVersionRange
import optimus.platform.relational.RelationalUnsupportedException
import optimus.platform.relational.tree.{ElementFactory => EF, _}

import scala.runtime.Null$

/**
 * This class will check the rowTypeInfo of each element and try to fill the gap between Scala and Java type system.
 *
 * Gap1: Type promotion for primitive types For "(x: Int, y: Double) => x + y", when we capture the tree in macro, the
 * types of x and y are TypeInfo.INT and TypeInfo.DOUBLE. But we know that JVM can only add up 2 numbers with a same
 * type. Thus we need to convert x to DOUBLE. This also applies to "if...else..." (ConditionalElement)
 *
 * Gap2: Intersection type (e.g. A with B with C) Scala will choose one type (A/B/C) as the real type in JVM. The Typer
 * follows this scheme (via "majorClassFrom" method). But it might not choose the same type as Scala compiler does. Thus
 * when we do a member access, say "(x: A with B with C) => x.fieldOfB", we need to make sure that (This also applies to
 * method call):
 *   a. x is an instance of B b. if x.fieldOfB's type is also an intersection type, we have to convert it to our assumed
 *      type (via "majorClassFrom") when its real type (via Java method) is not assignable.
 *
 * Gap3: Multiple parameter lists (e.g. "foo(x)(y)") Scala support multiple parameter lists, but in JVM, it will be
 * flattened. Thus we flatten the parameter list for constructor & method. (In RelationElement tree, we model multiple
 * parameter lists as List(ExpressionListElement1, ExpressionListElement2, ...))
 *
 * In addition, it also tries to find the real Java method/constructor for MemberElement, FuncElement and NewElement
 * (via Java reflection). For MemberElement, if it could not find the method, it assumes that the member is a
 * non-structural field (accessed via reflection in LambdaCompiler); For other cases, it will report error. Once the
 * method is found, it will be attached to the related descriptor. The Typer will also check if the argument is
 * assignable to method parameters and add necessary "convert" in the element tree.
 *
 * Gap4: var-args method Scala T* parameter is not distinguishable in Java reflection. Thus we have some special code to
 * support "apply" method for Map/Seq/List/Vector/etc. For other cases, please pass the var-args using "Seq.apply()"
 * explicitly in the element tree.
 *   a. We support "Seq(1,2,3,4)" b. We do NOT support "Array(1,2,3,4)", but you could rewrite it as "Array(1,
 *      Seq(2,3,4))"
 *
 * Gap5: Methods do NOT exist in Java methods like String.+, Int.toDouble are frequently used in the reified lambda. We
 * have specific code to support such cases. For other case, we do not support them.
 *
 * Gap6: macro & compiler plugin specific methods Apparently, we cannot support macro. We support $queued method, but we
 * do not support $withNode (except "liftNode" since it is frequently used by PriQL). For example:
 *   a. We support "liftNode { t => t.asyncField1 }" b. We do NOT support "asNode { t => t.asyncField1 }", but you could
 *      rewrite it (element tree) as "asNode.apply$withNode(liftNode { t => t.asyncField1 })"
 */
class Typer private () extends QueryTreeVisitor {
  import BinaryExpressionType._
  import TypeInfo._
  import Typer._

  override protected def handleLambda(l: LambdaElement): RelationElement = {
    val body = visitElement(l.body)
    new LambdaElement(javaTypeInfo(functionClass(l.parameters.size)), body, l.parameters)
  }

  override protected def handleTypeIs(typeIs: TypeIsElement): RelationElement = {
    val elem = boxElement(visitElement(typeIs.element))
    updateTypeIs(typeIs, elem)
  }

  override protected def handleMemberRef(e: MemberElement): RelationElement = {
    var inst = visitElement(e.instanceProvider)
    val desc = e.member

    desc.declaringType.nonStructuralProperties.get(desc.name).map { m =>
      // make sure the inst is assignable to the method's declaring class, see Gap2.a
      inst = mkAssignable(m.getDeclaringClass, inst)
      // make sure the method's return type is assignable to e.rowTypeInfo, see Gap2.b
      if (majorClassFrom(e.rowTypeInfo).isAssignableFrom(m.getReturnType)) {
        val fd = new RuntimeFieldDescriptor(desc.declaringType, desc.name, e.rowTypeInfo, mkFieldMeta(m))
        EF.makeMemberAccess(inst, fd)
      } else {
        val fd =
          new RuntimeFieldDescriptor(desc.declaringType, desc.name, javaTypeInfo(m.getReturnType), mkFieldMeta(m))
        EF.convert(EF.makeMemberAccess(inst, fd), e.rowTypeInfo)
      }
    } getOrElse {
      if (inst.rowTypeInfo.classes.nonEmpty) {
        if (inst.rowTypeInfo.clazz.isPrimitive) {
          // support methods like toDouble, toInt, toLong, see Gap5
          if (desc.name.startsWith("to") && e.rowTypeInfo.clazz.isPrimitive)
            EF.convert(inst, e.rowTypeInfo)
          else ???
        } else if (inst.rowTypeInfo.clazz == STRING.clazz) {
          ???
        } else updateMemberRef(e, inst) // non-structural member access
      } else updateMemberRef(e, inst) // non-structural member access
    }
  }

  override protected def handleBinaryExpression(b: BinaryExpressionElement): RelationElement = {
    var left = visitElement(b.left)
    var right = visitElement(b.right)
    b.op match {
      case BOOLAND | BOOLOR =>
        assert(left.rowTypeInfo == BOOLEAN)
        assert(right.rowTypeInfo == BOOLEAN)
        updateBinary(b, left, right)
      case ITEM_IS_IN | ITEM_IS_NOT_IN =>
        resolveJavaMethod(right.rowTypeInfo, "contains", List(left.rowTypeInfo))
          .map { m =>
            val argClass = m.getParameterTypes.apply(0)
            left = mkAssignable(argClass, left)
            right = mkAssignable(m.getDeclaringClass, right)
            val desc =
              new RuntimeMethodDescriptor(right.rowTypeInfo, "contains", BOOLEAN, metadata = Map(MetaMethod -> m))
            val e = EF.makeBinary(ITEM_IS_IN, left, right, desc)
            if (b.op == ITEM_IS_IN) e
            else EF.makeBinary(EQ, e, EF.constant(false, BOOLEAN))
          }
          .getOrElse {
            throw new RelationalUnsupportedException(
              "'contains' method not found for ITEM_IS_IN/ITEM_IS_NOT_IN operation.")
          }
      case EQ | NE =>
        val (l, r) = promote(left, right)
        updateBinary(b, l, r)
      case GE | GT | LE | LT =>
        val (l, r) = promote(left, right)
        if (majorClassFrom(l.rowTypeInfo).isPrimitive)
          updateBinary(b, l, r)
        else {
          left = l
          right = r
          val method = left.rowTypeInfo.classes.find(c => classOf[Comparable[_]].isAssignableFrom(c)).map { c =>
            // for class like String
            classOf[Comparable[_]].getMethod("compareTo", Seq(classOf[Object]): _*)
          } getOrElse {
            // for class that implements methods like "def >(...): Boolean"
            resolveJavaMethod(left.rowTypeInfo, BinaryMethods(b.op), List(right.rowTypeInfo)).get
          }
          left = mkAssignable(method.getDeclaringClass, left)
          val desc =
            new RuntimeMethodDescriptor(left.rowTypeInfo, method.getName, BOOLEAN, metadata = Map(MetaMethod -> method))
          EF.makeBinary(b.op, left, right, desc)
        }
      case PLUS
          if left.rowTypeInfo <:< classOf[String] || right.rowTypeInfo <:< classOf[
            String] || right.rowTypeInfo <:< classOf[Null$] =>
        // support String.+, see Gap5
        if (!(left.rowTypeInfo <:< classOf[String]))
          left = boxElement(left)
        if (right.rowTypeInfo <:< classOf[Null$])
          right = EF.convert(right, STRING)
        if (!(right.rowTypeInfo <:< classOf[String]))
          right = boxElement(right)
        new BinaryExpressionElement(PLUS, left, right, STRING)
      case MUL if left.rowTypeInfo <:< classOf[String] =>
        ???
      case PLUS | MINUS | MUL | DIV | MODULO =>
        val (l, r) = promote(left, right)
        if (majorClassFrom(l.rowTypeInfo).isPrimitive)
          updateBinary(b, l, r)
        else {
          left = l
          right = r
          val method = resolveJavaMethod(left.rowTypeInfo, BinaryMethods(b.op), List(right.rowTypeInfo)).get
          left = mkAssignable(method.getDeclaringClass, left)
          if (majorClassFrom(b.rowTypeInfo).isAssignableFrom(method.getReturnType)) {
            val desc = new RuntimeMethodDescriptor(
              left.rowTypeInfo,
              method.getName,
              b.rowTypeInfo,
              metadata = Map(MetaMethod -> method))
            EF.makeBinary(b.op, left, right, desc)
          } else {
            val desc = new RuntimeMethodDescriptor(
              left.rowTypeInfo,
              method.getName,
              javaTypeInfo(method.getReturnType),
              metadata = Map(MetaMethod -> method))
            EF.convert(EF.makeBinary(b.op, left, right, desc), b.rowTypeInfo)
          }
        }

      case _ => ??? // we do not support these
    }
  }

  override def handleConditional(c: ConditionalElement): RelationElement = {
    val test = visitElement(c.test)
    assert(test.rowTypeInfo == BOOLEAN)
    val (ifTrue, ifFalse) = promote(visitElement(c.ifTrue), visitElement(c.ifFalse))
    val returnType = majorClassFrom(c.rowTypeInfo)
    updateConditional(c, test, mkAssignable(returnType, ifTrue), mkAssignable(returnType, ifFalse))
  }

  private def flatten(args: List[RelationElement]): List[RelationElement] = {
    if (args.forall(_.isInstanceOf[ExpressionListElement])) {
      args.flatMap { case x: ExpressionListElement => x.exprList }
    } else args
  }

  private def mkAssignableCall(
      m: Method,
      inst: RelationElement,
      arguments: List[RelationElement],
      desc: MethodDescriptor,
      meta: Map[String, Any]): RelationElement = {
    val reflectType = if (inst eq null) desc.declaringType else javaTypeInfo(m.getDeclaringClass)
    if (majorClassFrom(desc.returnType).isAssignableFrom(m.getReturnType)) {
      val d =
        new RuntimeMethodDescriptor(reflectType, m.getName, desc.returnType, desc.genericArgTypes, metadata = meta)
      EF.call(inst, d, arguments)
    } else {
      val d = new RuntimeMethodDescriptor(
        reflectType,
        m.getName,
        javaTypeInfo(m.getReturnType),
        desc.genericArgTypes,
        metadata = meta)
      EF.convert(EF.call(inst, d, arguments), desc.returnType)
    }
  }

  private def isCollectionApply(method: MethodDescriptor): Boolean = {
    if (scalaVersionRange("2.13:"): @staged) {
      (method.declaringType <:< classOf[scala.collection.IterableFactory[Any]] || method.declaringType <:< classOf[
        scala.collection.MapFactory[Any]]) && method.name == "apply"
    } else {
      (method.declaringType <:< classOf[
        scala.collection.generic.GenericCompanion[Nothing]] || method.declaringType <:< classOf[
        scala.collection.generic.GenMapFactory[Nothing]]) && method.name == "apply"
    }
  }

  override def handleFuncCall(func: FuncElement): RelationElement = {
    val c = func.callee.asInstanceOf[MethodCallee]
    var inst = visitElement(func.instance)
    var arguments = flatten(visitElementList(func.arguments))
    if ((inst eq null) && c.method.declaringType <:< classOf[CoreAPI] && c.method.name == "liftNode") {
      val m = Func.getClass.getMethod("liftNode", collection.Seq(arguments(0).rowTypeInfo.clazz): _*)
      val desc = new RuntimeMethodDescriptor(
        javaTypeInfo(m.getDeclaringClass),
        m.getName,
        c.method.returnType,
        c.method.genericArgTypes,
        metadata = Map(MetaMethod -> m))
      EF.call(null, desc, arguments)
    } else if (isCollectionApply(c.method)) {
      // specific logic to support apply method of Seq/Vector/Map/List
      val m = resolveJavaMethod(c.method.declaringType, "apply", List(javaTypeInfo(classOf[collection.Seq[_]]))).get
      var wrapAsArray = false
      val cls = if (c.method.genericArgTypes.size == 1) majorClassFrom(c.method.genericArgTypes(0)) else classOf[Object]
      if (arguments.size == 0 || arguments.size > 1 || !(arguments(0).rowTypeInfo <:< classOf[collection.Seq[_]])) {
        arguments = arguments.map(e => mkAssignable(cls, e))
        wrapAsArray = true
      }
      mkAssignableCall(m, inst, arguments, c.method, Map(MetaMethod -> m, MetaVargs -> wrapAsArray))
    } else {
      val m = c.method.metadata.get(MetaMethod).map(_.asInstanceOf[Method]).getOrElse {
        val descParams = c.method.getParameters()
        val paramTypes = if (descParams.size > 0) descParams.map(_.typeInfo) else arguments.map(_.rowTypeInfo)
        resolveJavaMethod(
          c.method.declaringType,
          c.method.name,
          paramTypes,
          c.method.metadata.get(AbstractLambdaReifier.OverloadSignature).map(_.toString)).getOrElse {
          throw new RelationalUnsupportedException(
            s"Cannot resolve method: def ${c.method.declaringType.name}.${c.method.name}(${paramTypes.map(_.name).mkString(", ")})")
        }
      }
      if (inst ne null)
        inst = mkAssignable(m.getDeclaringClass, inst)
      arguments = arguments.zip(m.getParameterTypes).map { case (e, cls) => mkAssignable(cls, e) }
      mkAssignableCall(m, inst, arguments, c.method, mkMethodMeta(m))
    }
  }

  override def handleNew(ne: NewElement): RelationElement = {
    var arguments = flatten(visitElementList(ne.arguments))
    if (ne.members.isEmpty) {
      val descParams = ne.ctor.getParameters()
      val paramTypes = if (descParams.size > 0) descParams.map(_.typeInfo) else arguments.map(_.rowTypeInfo)
      val ctor = resolveJavaCtor(ne.ctor.declaringType, paramTypes).getOrElse {
        throw new RelationalUnsupportedException(
          s"Cannot resolve ctor: ${ne.ctor.declaringType.name}.<init>(${paramTypes.map(_.name).mkString(", ")})")
      }
      arguments = arguments.zip(ctor.getParameterTypes).map { case (e, cls) => mkAssignable(cls, e) }
      val desc = new RuntimeConstructorDescriptor(ne.ctor.declaringType, Map(MetaMethod -> ctor))
      EF.makeNew(desc, arguments, ne.members)
    } else {
      assert(ne.members.size == ne.arguments.size)
      assert(ne.rowTypeInfo.classes.forall(_.isInterface))
      val result = arguments
        .zip(ne.members)
        .map { case (arg, desc) =>
          ne.rowTypeInfo.nonStructuralProperties.get(desc.name).map { m =>
            val fd = desc.asInstanceOf[FieldDescriptor]
            val arg1 = mkAssignable(m.getReturnType, arg)
            val fd1 = new RuntimeFieldDescriptor(fd.declaringType, fd.name, fd.fieldType, Map(MetaMethod -> m))
            (arg1, fd1)
          } getOrElse {
            ne.rowTypeInfo.propertyMap
              .get(desc.name)
              .map { fieldType =>
                val arg1 = mkAssignable(majorClassFrom(fieldType), arg)
                (arg1, desc)
              }
              .getOrElse {
                (arg, desc)
              }
          }
        } unzip;
      EF.makeNew(ne.ctor, result._1, result._2)
    }
  }

  override def handleMethod(method: MethodElement): RelationElement = {
    throw new RelationalUnsupportedException("MethodElement is not supported!")
  }

  override def handleQuerySrc(element: ProviderRelation): RelationElement = {
    throw new RelationalUnsupportedException("ProviderRelation is not supported!")
  }

  override def handleAggregateExpression(element: AggregateExpressionElement): RelationElement = {
    throw new RelationalUnsupportedException("AggregateExpressionElement is not supported!")
  }
}

object Typer {
  import BinaryExpressionType._
  import TypeInfo._

  def typeCheck(e: RelationElement): RelationElement = {
    new Typer().visitElement(e)
  }

  private[asm] val MetaVargs = "meta$vargs"
  private[asm] val MetaMethod = "meta$method"
  private[asm] val MetaMethodAsync = "meta$method$a"

  private[asm] val BinaryMethods: PartialFunction[BinaryExpressionType, String] = {
    case ITEM_IS_IN => "contains"
    case GT         => "$greater"
    case GE         => "$greater$eq"
    case LT         => "$less"
    case LE         => "$less$eq"
    case PLUS       => "$plus"
    case MINUS      => "$minus"
    case MUL        => "$times"
    case DIV        => "$div"
    case MODULO     => "$percent"
    case EQ         => "equals"
  }

  private[asm] def majorClassFrom(t: TypeInfo[_]): Class[_] = {
    if (t.classes.isEmpty) classOf[Object]
    else t.clazz
  }

  private def mkAssignable(assignee: Class[_], e: RelationElement): RelationElement = {
    if (assignee.isAssignableFrom(majorClassFrom(e.rowTypeInfo))) e
    else EF.convert(e, TypeInfo.javaTypeInfo(assignee))
  }

  private def mkFieldMeta(method: Method): Map[String, Any] = {
    val name = s"${method.getName}$$queued"
    method.getDeclaringClass.getMethods
      .find(m => m.getParameterCount == 0 && m.getReturnType == classOf[Node[_]] && m.getName == name)
      .map { am =>
        Map(MetaMethod -> method, MetaMethodAsync -> am)
      } getOrElse {
      Map(MetaMethod -> method)
    }
  }

  private def mkMethodMeta(method: Method): Map[String, Any] = {
    val name = s"${method.getName}$$queued"
    method.getDeclaringClass.getMethods.find { m =>
      m.getParameterCount == method.getParameterCount &&
      m.getReturnType == classOf[Node[_]] &&
      m.getName == name &&
      m.getParameterTypes.zip(method.getParameterTypes).forall(t => t._1 == t._2)
    } map { am =>
      Map(MetaMethod -> method, MetaMethodAsync -> am)
    } getOrElse {
      Map(MetaMethod -> method)
    }
  }

  private[relational] def functionClass(size: Int): Class[_] = {
    size match {
      case 0 => classOf[Function0[_]]
      case 1 => classOf[Function1[_, _]]
      case 2 => classOf[Function2[_, _, _]]
      case 3 => classOf[Function3[_, _, _, _]]
      case 4 => classOf[Function4[_, _, _, _, _]]
      case 5 => classOf[Function5[_, _, _, _, _, _]]
      case _ => Class.forName(s"scala.Function$size")
    }
  }

  private[relational] def nodeFunctionClass(size: Int): Class[_] = {
    size match {
      case 0 => classOf[NodeFunction0[_]]
      case 1 => classOf[NodeFunction1[_, _]]
      case 2 => classOf[NodeFunction2[_, _, _]]
      case 3 => classOf[NodeFunction3[_, _, _, _]]
      case 4 => classOf[NodeFunction4[_, _, _, _, _]]
      case 5 => classOf[NodeFunction5[_, _, _, _, _, _]]
      case _ => Class.forName(s"optimus.platform.NodeFunction$size")
    }
  }

  private def boxElement(e: RelationElement): RelationElement = {
    e.rowTypeInfo match {
      case BOOLEAN => EF.convert(e, javaTypeInfo(classOf[java.lang.Boolean]))
      case BYTE    => EF.convert(e, javaTypeInfo(classOf[java.lang.Byte]))
      case CHAR    => EF.convert(e, javaTypeInfo(classOf[java.lang.Character]))
      case SHORT   => EF.convert(e, javaTypeInfo(classOf[java.lang.Short]))
      case INT     => EF.convert(e, javaTypeInfo(classOf[java.lang.Integer]))
      case LONG    => EF.convert(e, javaTypeInfo(classOf[java.lang.Long]))
      case FLOAT   => EF.convert(e, javaTypeInfo(classOf[java.lang.Float]))
      case DOUBLE  => EF.convert(e, javaTypeInfo(classOf[java.lang.Double]))
      case _       => e
    }
  }

  private def promote(e1: RelationElement, e2: RelationElement): (RelationElement, RelationElement) = {
    val t1 = e1.rowTypeInfo
    val t2 = e2.rowTypeInfo

    val s1 = score(t1)
    val s2 = score(t2)

    if (s1 == -1 || s2 == -1) {
      if (t1 == t2) (e1, e2)
      else (boxElement(e1), boxElement(e2))
    } else if (s1 == s2) {
      (e1, e2)
    } else {
      val commonType =
        if (s1 > s2)
          promotions((s1 * (s1 - 1)) / 2 + s2)
        else
          promotions((s2 * (s2 - 1)) / 2 + s1)

      val newE1 = if (commonType == t1) e1 else EF.convert(e1, commonType)
      val newE2 = if (commonType == t2) e2 else EF.convert(e2, commonType)
      (newE1, newE2)
    }
  }

  private def score(t: TypeInfo[_]): Int = {
    import TypeInfo._

    t match {
      case BYTE | CHAR | SHORT | INT => 0
      case LONG                      => 1
      case FLOAT                     => 2
      case DOUBLE                    => 3
      case _                         => -1
    }
  }

  private def score(c: Class[_]): Int = {
    import TypeInfo._

    c match {
      case BYTE.clazz | CHAR.clazz | SHORT.clazz | INT.clazz => 0
      case LONG.clazz                                        => 1
      case FLOAT.clazz                                       => 2
      case DOUBLE.clazz                                      => 3
      case _                                                 => -1
    }
  }

  private val promotions = Array[TypeInfo[_]](
    /*         INT     LONG    DOUBLE*/
    /*INT   */
    /*LONG  */ LONG,
    /*FLOAT */ FLOAT,
    DOUBLE,
    /*DOUBLE*/ DOUBLE,
    DOUBLE,
    DOUBLE
  )

  private def resolveByArgTypes[T <: Executable](
      candidates: collection.Seq[T],
      argTypes: collection.Seq[TypeInfo[_]]): Option[T] = {
    if (candidates.isEmpty) None
    else if (candidates.size == 1) candidates.headOption
    else {
      // phase1: no boxing/unboxing
      val matches = candidates.filter(m => argTypes.zip(m.getParameterTypes).forall { case (t, c) => t <:< c })
      if (matches.nonEmpty) Some(matches.min(Ordering fromLessThan moreSpecific))
      else {
        // phase2: consider boxing/unboxing
        val matches2 = candidates.filter(m1 => phase2Matches(m1.getParameterTypes, argTypes))
        if (matches2.nonEmpty) Some(matches2.min(Ordering fromLessThan moreSpecific))
        else None
      }
    }
  }

  private[relational] def resolveJavaMethod(
      owner: TypeInfo[_],
      name: String,
      argTypes: collection.Seq[TypeInfo[_]],
      overloadSignature: Option[String] = None): Option[Method] = {
    val candidates =
      owner.classes.flatMap(_.getMethods.filter(m =>
        m.getParameterCount == argTypes.size && m.getName == name && !m.isBridge))
    import org.objectweb.asm.Type.getMethodDescriptor
    val byOverload = overloadSignature.flatMap(sig => candidates.find(c => getMethodDescriptor(c) == sig))
    byOverload.orElse(resolveByArgTypes(candidates, argTypes))
  }

  private[relational] def resolveJavaCtor(
      owner: TypeInfo[_],
      argTypes: collection.Seq[TypeInfo[_]]): Option[Constructor[_]] = {
    val candidates = owner.clazz
      .getConstructors()
      .toSeq
      .filter(c => Modifier.isPublic(c.getModifiers) && c.getParameterCount == argTypes.size)
    resolveByArgTypes(candidates, argTypes)
  }

  private def moreSpecific(m1: Executable, m2: Executable): Boolean = {
    def moreSpecificPrimitive(c1: Class[_], c2: Class[_]): Int = {
      val s1 = score(c1)
      val s2 = score(c2)
      if (s1 == s2) 0
      else if (s1 < s2) 1
      else -1
    }

    def moreSpecificClass(c1: Class[_], c2: Class[_]): Int = {
      if (c1.isPrimitive && c2.isPrimitive)
        moreSpecificPrimitive(c1, c2)
      else if (c1.isPrimitive) 1
      else if (c2.isPrimitive) -1
      else {
        val cp1 = Primitives.unwrap(c1)
        val cp2 = Primitives.unwrap(c2)
        if ((cp1 ne c1) || (cp2 ne c2))
          moreSpecificClass(cp1, cp2)
        else -1
      }
    }

    m1.getParameterTypes
      .zip(m2.getParameterTypes)
      .map { case (c1, c2) =>
        if (c1 == c2) 0
        else if (c2.isAssignableFrom(c1)) 1
        else moreSpecificClass(c1, c2)
      }
      .sum > 0
  }

  private def phase2Matches(paramClasses: collection.Seq[Class[_]], argTypes: collection.Seq[TypeInfo[_]]): Boolean = {
    var idx = 0
    while (idx < paramClasses.size) {
      val c = paramClasses(idx)
      val t = argTypes(idx)

      if (!(t <:< c)) {
        val ts = score(t)
        if (ts >= 0) {
          // t is primitive (except boolean)
          if (c.isPrimitive) {
            // both types are primitive
            if (ts > score(c)) return false
          } else {
            val cp = Primitives.unwrap(c)
            if ((cp ne c) && ts > score(cp)) return false
            if ((cp eq c) && !c.isAssignableFrom(Primitives.wrap(t.clazz))) return false
          }
        } else if (c.isPrimitive) {
          if (t == TypeInfo.BOOLEAN) {
            if (c != classOf[Boolean]) return false
          } else {
            val tp = Primitives.unwrap(t.clazz)
            if (tp != c) return false
          }
        } else return false
      }
      idx += 1
    }
    true
  }
}
