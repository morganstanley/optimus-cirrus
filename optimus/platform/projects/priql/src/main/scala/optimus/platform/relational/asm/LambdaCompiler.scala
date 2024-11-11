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

import java.io.FileOutputStream
import java.lang.reflect.Constructor
import java.lang.reflect.Method
import java.lang.reflect.Modifier

import optimus.graph.Node
import org.objectweb.asm.Opcodes._
import org.objectweb.asm._
import optimus.platform.relational.asm.ASMGeneratorUtils.Loader
import optimus.platform.relational.data.translation.ElementReplacer
import optimus.platform.relational.inmemory.MultiRelationElementRemover
import optimus.platform.relational.tree._
import optimus.scalacompat.isAtLeastScala2_13

class LambdaCompiler protected (protected val t: AnalyzedTree, l: LambdaElement)
    extends ASMGeneratorUtils
    with ASMConstants {
  import BinaryExpressionType._
  import Typer._

  protected val className: String = "Lambda"
  protected val root = new LambdaEmitter(t.nodes, l)
  protected val cw = new ClassWriter(ClassWriter.COMPUTE_FRAMES)
  protected lazy val loader = new Loader(this.getClass.getClassLoader)
  private var anonClasses: List[(String, Array[Byte])] = Nil
  private var lambdaCount: Int = 1

  {
    val arity = l.parameters.size
    cw.visit(ClassfileVersion, ACC_PUBLIC + ACC_SUPER, className, null, Object, Array(s"$Func$arity"))

    // constructor
    if (t.constants.values.nonEmpty) {
      cw.visitField(ACC_PRIVATE + ACC_FINAL, ConstsInst, s"[L$Object;", null, null)
      cw.visitEnd()
      val mv = cw.visitMethod(ACC_PRIVATE, Ctor, s"([L$Object;)V", null, null)
      mv.visitCode()
      mv.visitVarInsn(ALOAD, 0)
      mv.visitMethodInsn(INVOKESPECIAL, Object, Ctor, "()V", false)
      mv.visitVarInsn(ALOAD, 0)
      mv.visitVarInsn(ALOAD, 1)
      mv.visitFieldInsn(PUTFIELD, className, ConstsInst, s"[L$Object;")
      mv.visitInsn(RETURN)
      mv.visitMaxs(0, 0)
      mv.visitEnd()
    } else {
      val mv = cw.visitMethod(ACC_PRIVATE, Ctor, "()V", null, null)
      mv.visitCode()
      mv.visitVarInsn(ALOAD, 0)
      mv.visitMethodInsn(INVOKESPECIAL, Object, Ctor, "()V", false)
      mv.visitInsn(RETURN)
      mv.visitMaxs(0, 0)
      mv.visitEnd()
    }

    // interface
    val objDesc = s"L$Object;"
    val paramList = objDesc * arity
    val mv = cw.visitMethod(ACC_PUBLIC, "apply", s"($paramList)$objDesc", null, null)
    mv.visitCode()
    mv.visitVarInsn(ALOAD, 0)
    l.parameters.zipWithIndex.foreach { case (p, idx) =>
      loadMethodArg(mv, idx + 1, objDesc)
      if (majorClassFrom(p.rowTypeInfo) != ObjectClass)
        castTo(mv, descFrom(p.rowTypeInfo), p.rowTypeInfo.clazz)
    }
    mv.visitMethodInsn(INVOKEVIRTUAL, className, root.name, root.methodDesc, false)
    if (isPrimitive(root.resultDesc)) {
      val name = boxToName(root.resultDesc)
      mv.visitMethodInsn(INVOKESTATIC, name, "valueOf", s"(${root.resultDesc})L$name;", false)
    }
    mv.visitInsn(ARETURN)
    mv.visitMaxs(0, 0)
    mv.visitEnd()
  }

  final def compile[T](): T = {
    root.emitLambdaBody()
    anonClasses.foreach(t => loader.loadClass(t._1, t._2))
    val clazz = loader.loadClass(className, emitEnd())
    val factory = clazz.getMethod("get$Factory", Seq(classOf[Array[Object]]): _*)
    factory.invoke(null, Seq(t.constants.values.toArray): _*).asInstanceOf[T]
  }

  final def dumpToFile(file: String): Unit = {
    root.emitLambdaBody()
    val fs = new FileOutputStream(file)
    try {
      fs.write(emitEnd())
    } finally {
      fs.close()
    }
  }

  protected def emitEnd(): Array[Byte] = {
    // generate a factory method
    val arity = l.parameters.size
    val scalaFuncDesc = s"Lscala/Function$arity;"
    val mv = cw.visitMethod(ACC_PUBLIC + ACC_STATIC, "get$Factory", s"([L$Object;)$scalaFuncDesc", null, null)
    mv.visitCode()

    val funcObject = s"$Func$$"
    mv.visitFieldInsn(GETSTATIC, funcObject, "MODULE$", s"L$funcObject;")
    mv.visitTypeInsn(NEW, className)
    mv.visitInsn(DUP)
    if (t.constants.values.nonEmpty) {
      mv.visitVarInsn(ALOAD, 0)
      mv.visitMethodInsn(INVOKESPECIAL, className, Ctor, s"([L$Object;)V", false)
    } else {
      mv.visitMethodInsn(INVOKESPECIAL, className, Ctor, "()V", false)
    }
    if (root.resultIsNode)
      mv.visitMethodInsn(INVOKEVIRTUAL, funcObject, "wrap", s"(L$Func$arity;)$scalaFuncDesc", false)
    else
      mv.visitMethodInsn(INVOKEVIRTUAL, funcObject, "asFunction", s"(L$Func$arity;)$scalaFuncDesc", false)
    mv.visitInsn(ARETURN)
    mv.visitMaxs(0, 0)
    mv.visitEnd()

    cw.visitEnd()
    cw.toByteArray()
  }

  protected object LambdaEmitter {
    def mkName(e: LambdaEmitter): String = {
      if (e.parent eq null) "lambda$0"
      else {
        val id = lambdaCount
        lambdaCount = id + 1
        s"lambda$$${e.depth}$$$id"
      }
    }
  }

  protected class LambdaEmitter(
      val nodes: Set[RelationElement],
      val l: LambdaElement,
      val parent: LambdaEmitter = null,
      val isContinuation: Boolean = false) {
    val resultIsNode = isContinuation || t.nodeDependencies.get(l).map(_.nonEmpty).getOrElse(false)
    val resultDesc = if (resultIsNode) s"L$NodeFuture;" else descFrom(l.body.rowTypeInfo)
    protected var needsClosure = false
    protected var currentVar = if (parent eq null) 1 else 2
    protected val argOffset: Map[ParameterElement, Int] = {
      l.parameters.iterator
        .map(p => {
          val result = (p, currentVar)
          if (p.rowTypeInfo == TypeInfo.DOUBLE || p.rowTypeInfo == TypeInfo.LONG) currentVar += 2 else currentVar += 1
          result
        })
        .toMap
    }
    val depth: Int = if (parent eq null) 0 else parent.depth + 1
    val name: String = LambdaEmitter.mkName(this)
    protected val (hasPrimitiveParams, paramsDesc, boxedParamsDesc) = {
      val params = l.parameters.map(p => descFrom(p.rowTypeInfo))
      val hasPrimitiveParam = params.exists(d => isPrimitive(d))
      val paramsDescStr = params.mkString("")
      if (!hasPrimitiveParam) (hasPrimitiveParam, paramsDescStr, paramsDescStr)
      else (hasPrimitiveParam, paramsDescStr, params.map(boxToDesc).mkString(""))
    }

    val methodDesc = {
      val argumentsDesc = if (parent eq null) paramsDesc else s"L$Closure;$paramsDesc"
      s"($argumentsDesc)$resultDesc"
    }
    protected lazy val mv: MethodVisitor = cw.visitMethod(ACC_PRIVATE, name, methodDesc, null, null)

    protected def emitSyntheticStub(): Unit = {
      // generate a synthetic stub
      val mv =
        cw.visitMethod(
          ACC_PRIVATE + ACC_SYNTHETIC,
          s"$name$$synthetic",
          s"(L$Closure;${boxedParamsDesc})${resultDesc}",
          null,
          null)
      mv.visitCode()
      mv.visitVarInsn(ALOAD, 0)
      mv.visitVarInsn(ALOAD, 1)
      val objDesc = s"L$Object;"
      l.parameters.zipWithIndex.foreach { case (p, idx) =>
        loadMethodArg(mv, idx + 2, objDesc)
        if (majorClassFrom(p.rowTypeInfo).isPrimitive) {
          val desc = descFrom(p.rowTypeInfo)
          val name = p.rowTypeInfo.clazz.getName
          mv.visitMethodInsn(INVOKEVIRTUAL, boxToName(desc), s"${name}Value", s"()$desc", false)
        }
      }
      mv.visitMethodInsn(INVOKEVIRTUAL, className, name, methodDesc, false)
      returnFromMethod(mv, resultDesc)
      mv.visitMaxs(0, 0)
      mv.visitEnd()
    }

    protected def emitClosure(needsClosure: Boolean): Unit = {
      if (!needsClosure) {
        mv.visitInsn(ACONST_NULL)
      } else {
        if (isContinuation)
          mv.visitVarInsn(ALOAD, 1)
        else {
          // Closure(previous, paramsArray)
          if (parent eq null)
            mv.visitInsn(ACONST_NULL)
          else
            mv.visitVarInsn(ALOAD, 1)

          mv.visitIntInsn(BIPUSH, l.parameters.size)
          mv.visitTypeInsn(ANEWARRAY, Object)
          var idx = if (parent eq null) 1 else 2
          l.parameters.zipWithIndex.foreach { case (p, i) =>
            mv.visitInsn(DUP)
            mv.visitIntInsn(BIPUSH, i)
            idx = loadMethodArg(mv, idx, descFrom(p.rowTypeInfo))
            autoBoxing(mv, p.rowTypeInfo)
            mv.visitInsn(AASTORE)
          }
          mv.visitMethodInsn(INVOKESTATIC, ClosureCls, "apply", s"(L$Closure;[L$Object;)L$Closure;", false)
        }
      }
    }

    protected def emitNodeDependentElement(root: RelationElement, nodeDeps: List[RelationElement]): Unit = {
      if (nodeDeps.isEmpty) emitElement(root)
      else {
        val params = nodeDeps.zipWithIndex.map { case (e, idx) => ElementFactory.parameter(s"p$idx", e.rowTypeInfo) }
        val body = ElementReplacer.replace(root, nodeDeps, params)
        val lambda = ElementFactory.lambda(body, params)
        val n = if (!nodes.contains(root)) nodes else nodes + body
        val emitter = new LambdaEmitter(n, lambda, this, true)
        emitter.emitLambdaBody()

        mv.visitFieldInsn(GETSTATIC, ContinuationObject, "MODULE$", s"L$ContinuationObject;")
        nodeDeps match {
          case single :: Nil => // andThen
            emitNodeDependentElement(single, t.nodeDependencies.get(single).getOrElse(Nil))
            mv.visitVarInsn(ALOAD, 0)
            emitClosure(emitter.needsClosure)
            val methodName = if (emitter.hasPrimitiveParams) s"${emitter.name}$$synthetic" else emitter.name
            mv.visitInvokeDynamicInsn(
              "apply",
              s"(L$className;L$Closure;)L$Function;",
              mkBootstrapHandle,
              Seq(
                Type.getType(s"(L$Object;)L$Object;"),
                new Handle(
                  Opcodes.H_INVOKESPECIAL,
                  className,
                  methodName,
                  s"(L$Closure;${emitter.boxedParamsDesc})${emitter.resultDesc}",
                  false),
                Type.getType(s"(${emitter.boxedParamsDesc})${emitter.resultDesc}")
              ): _*
            )
            mv.visitMethodInsn(
              INVOKEVIRTUAL,
              ContinuationObject,
              "andThen",
              s"(L$NodeFuture;L$Function;)L$Node;",
              false)

            if (emitter.hasPrimitiveParams)
              emitter.emitSyntheticStub()

          case _ => // whenAll
            mv.visitIntInsn(BIPUSH, nodeDeps.size)
            mv.visitTypeInsn(ANEWARRAY, NodeFuture)
            nodeDeps.zipWithIndex.foreach { case (e, idx) =>
              mv.visitInsn(DUP)
              mv.visitIntInsn(BIPUSH, idx)
              emitNodeDependentElement(e, t.nodeDependencies.get(e).getOrElse(Nil))
              mv.visitInsn(AASTORE)
            }
            mv.visitVarInsn(ALOAD, 0)
            emitClosure(emitter.needsClosure)
            val objArrayDesc = s"[L$Object;"
            mv.visitInvokeDynamicInsn(
              "apply",
              s"(L$className;L$Closure;)L$Function;",
              mkBootstrapHandle,
              Seq(
                Type.getType(s"(L$Object;)L$Object;"),
                new Handle(
                  Opcodes.H_INVOKESPECIAL,
                  className,
                  s"${emitter.name}$$synthetic",
                  s"(L$Closure;$objArrayDesc)L$NodeFuture;",
                  false),
                Type.getType(s"($objArrayDesc)L$NodeFuture;")
              ): _*
            )
            mv.visitMethodInsn(
              INVOKEVIRTUAL,
              ContinuationObject,
              "whenAll",
              s"([L$NodeFuture;L$Function;)L$Node;",
              false)
        }
      }
    }

    def emitLambdaBody(): Unit = {
      mv.visitCode()
      val lambdaDep = t.nodeDependencies.get(l).getOrElse(Nil)
      val bodyIsNode = nodes.contains(l.body)
      wrapInNode(
        isContinuation && !bodyIsNode,
        l.body.rowTypeInfo, {
          emitNodeDependentElement(l.body, t.nodeDependencies.get(l.body).getOrElse(lambdaDep))
        })
      returnFromMethod(mv, resultDesc)
      mv.visitMaxs(0, 0)
      mv.visitEnd()

      if (isContinuation && l.parameters.size > 1) {
        val mv =
          cw.visitMethod(
            ACC_PRIVATE + ACC_SYNTHETIC,
            s"$name$$synthetic",
            s"(L$Closure;[L$Object;)L$NodeFuture;",
            null,
            null)
        mv.visitCode()
        mv.visitVarInsn(ALOAD, 0)
        mv.visitVarInsn(ALOAD, 1)
        // expand the array
        l.parameters.zipWithIndex.foreach { case (p, idx) =>
          mv.visitVarInsn(ALOAD, 2)
          mv.visitIntInsn(BIPUSH, idx)
          mv.visitInsn(AALOAD)
          if (majorClassFrom(p.rowTypeInfo) != ObjectClass)
            castTo(mv, descFrom(p.rowTypeInfo), p.rowTypeInfo.clazz)
        }
        mv.visitMethodInsn(INVOKESPECIAL, className, name, methodDesc, false)
        mv.visitInsn(ARETURN)
        mv.visitMaxs(0, 0)
        mv.visitEnd()
      }
    }

    private def wrapInNode(requireWrap: Boolean, shapeType: TypeInfo[_], f: => Unit): Unit = {
      if (requireWrap) {
        mv.visitTypeInsn(NEW, AlreadyCompletedNode)
        mv.visitInsn(DUP)
        f
        autoBoxing(mv, shapeType)
        mv.visitMethodInsn(INVOKESPECIAL, AlreadyCompletedNode, Ctor, s"(L$Object;)V", false)
      } else {
        f
      }
    }

    protected def emitElement(e: RelationElement): Unit = {
      import UnaryExpressionType.CONVERT

      e match {
        case null                                                              =>
        case c: ConstValueElement                                              => emitConstValue(c)
        case p: ParameterElement                                               => emitParameter(p)
        case m: MemberElement                                                  => emitMember(m)
        case b: BinaryExpressionElement if (b.op == BOOLAND || b.op == BOOLOR) => emitLogicalBinary(b)
        case b: BinaryExpressionElement                                        => emitBinary(b)
        case u: UnaryExpressionElement if u.op == CONVERT                      => emitConvert(u)
        case f: FuncElement                                                    => emitFuncCall(f)
        case c: ConditionalElement                                             => emitCondition(c)
        case l: LambdaElement                                                  => emitLambda(l)
        case n: NewElement                                                     => emitNew(n)
        case t: TypeIsElement                                                  => emitTypeIs(t)
      }
    }

    protected def emitLambda(l: LambdaElement): Unit = {
      val emitter = new LambdaEmitter(nodes, l, this)
      emitter.emitLambdaBody()

      val funcObject = s"$Func$$"
      mv.visitFieldInsn(GETSTATIC, funcObject, "MODULE$", s"L$funcObject;")

      mv.visitVarInsn(ALOAD, 0)
      emitClosure(emitter.needsClosure)
      val arity = l.parameters.size
      val func = s"$Func$arity"
      val scalaFuncDesc = s"Lscala/Function$arity;"
      val funcArgList = s"L$Object;" * arity
      val methodName = if (emitter.hasPrimitiveParams) s"${emitter.name}$$synthetic" else emitter.name
      mv.visitInvokeDynamicInsn(
        "apply",
        s"(L$className;L$Closure;)L$func;",
        mkBootstrapHandle,
        Seq(
          Type.getType(s"($funcArgList)L$Object;"),
          new Handle(
            Opcodes.H_INVOKESPECIAL,
            className,
            methodName,
            s"(L$Closure;${emitter.boxedParamsDesc})${emitter.resultDesc}",
            false),
          Type.getType(s"(${emitter.boxedParamsDesc})${emitter.resultDesc}")
        ): _*
      )

      if (emitter.resultIsNode)
        mv.visitMethodInsn(INVOKEVIRTUAL, funcObject, "wrap", s"(L$func;)$scalaFuncDesc", false)
      else
        mv.visitMethodInsn(INVOKEVIRTUAL, funcObject, "asFunction", s"(L$func;)$scalaFuncDesc", false)

      if (emitter.hasPrimitiveParams)
        emitter.emitSyntheticStub()
    }

    protected def emitConvert(c: UnaryExpressionElement): Unit = {
      emitElement(c.element)
      convert(mv, c.element.rowTypeInfo, c.rowTypeInfo)
    }

    protected def emitCondition(c: ConditionalElement): Unit = {
      val trueBranchIsNode = t.nodeDependencies.contains(c.ifTrue)
      val falseBranchIsNode = t.nodeDependencies.contains(c.ifFalse)
      val l0 = new Label
      val l1 = new Label
      emitElement(c.test)
      mv.visitJumpInsn(IFEQ, l0)
      if (trueBranchIsNode)
        emitNodeDependentElement(c.ifTrue, t.nodeDependencies(c.ifTrue))
      else
        wrapInNode(falseBranchIsNode, c.rowTypeInfo, emitElement(c.ifTrue))
      mv.visitJumpInsn(GOTO, l1)
      mv.visitLabel(l0)
      if (falseBranchIsNode)
        emitNodeDependentElement(c.ifFalse, t.nodeDependencies(c.ifFalse))
      else
        wrapInNode(trueBranchIsNode, c.rowTypeInfo, emitElement(c.ifFalse))
      mv.visitLabel(l1)
    }

    protected def emitLogicalBinary(b: BinaryExpressionElement): Unit = {
      val rightIsNode = t.nodeDependencies.contains(b.right)
      val l0 = new Label
      val l1 = new Label
      emitElement(b.left)
      mv.visitJumpInsn(if (b.op == BOOLOR) IFNE else IFEQ, l0)
      if (rightIsNode)
        emitNodeDependentElement(b.right, t.nodeDependencies(b.right))
      else
        emitElement(b.right)
      mv.visitJumpInsn(GOTO, l1)
      mv.visitLabel(l0)
      wrapInNode(rightIsNode, TypeInfo.BOOLEAN, mv.visitInsn(if (b.op == BOOLOR) ICONST_1 else ICONST_0))
      mv.visitLabel(l1)
    }

    protected def emitBinary(b: BinaryExpressionElement): Unit = {
      b.op match {
        case ITEM_IS_IN =>
          emitElement(b.right)
          emitElement(b.left)
          b.method.metadata.get(MetaMethod).foreach { case m: Method => invokeMethod(mv, m) }

        case PLUS if b.left.rowTypeInfo <:< classOf[String] || b.right.rowTypeInfo <:< classOf[String] =>
          mv.visitTypeInsn(NEW, StringBuilder)
          mv.visitInsn(DUP)
          mv.visitMethodInsn(INVOKESPECIAL, StringBuilder, Ctor, "()V", false)
          emitElement(b.left)
          mv.visitMethodInsn(INVOKEVIRTUAL, StringBuilder, "append", s"(L$Object;)L$StringBuilder;", false)
          emitElement(b.right)
          mv.visitMethodInsn(INVOKEVIRTUAL, StringBuilder, "append", s"(L$Object;)L$StringBuilder;", false)
          mv.visitMethodInsn(INVOKEVIRTUAL, StringBuilder, "toString", s"()L$String;", false)

        case _ =>
          emitElement(b.left)
          emitElement(b.right)
          emitBinaryOp(b.op, b.left.rowTypeInfo, b.method)
      }
    }

    protected def emitBinaryOp(op: BinaryExpressionType, typeInfo: TypeInfo[_], desc: MemberDescriptor): Unit = {
      op match {
        case EQ     => emitEqual(typeInfo)
        case NE     => emitNotEqual(typeInfo)
        case GT     => emitCompare(typeInfo, IF_ICMPLE, IFLE, desc)
        case GE     => emitCompare(typeInfo, IF_ICMPLT, IFLT, desc)
        case LT     => emitCompare(typeInfo, IF_ICMPGE, IFGE, desc)
        case LE     => emitCompare(typeInfo, IF_ICMPGT, IFGT, desc)
        case PLUS   => emitPlus(typeInfo, desc)
        case MINUS  => emitMinus(typeInfo, desc)
        case MUL    => emitMultiply(typeInfo, desc)
        case DIV    => emitDivision(typeInfo, desc)
        case MODULO => emitModulo(typeInfo, desc)
      }
    }

    private def emitModulo(typeInfo: TypeInfo[_], desc: MemberDescriptor): Unit = {
      descFrom(typeInfo) match {
        case "B" | "C" | "I" | "S" =>
          mv.visitInsn(IREM)
        case "D" => // double
          mv.visitInsn(DREM)
        case "J" => // long
          mv.visitInsn(LREM)
        case "F" =>
          mv.visitInsn(FREM)
        case _ =>
          invokeMethod(mv, desc.metadata(MetaMethod).asInstanceOf[Method])
      }
    }

    private def emitDivision(typeInfo: TypeInfo[_], desc: MemberDescriptor): Unit = {
      descFrom(typeInfo) match {
        case "B" | "C" | "I" | "S" =>
          mv.visitInsn(IDIV)
        case "D" => // double
          mv.visitInsn(DDIV)
        case "J" => // long
          mv.visitInsn(LDIV)
        case "F" =>
          mv.visitInsn(FDIV)
        case _ =>
          invokeMethod(mv, desc.metadata(MetaMethod).asInstanceOf[Method])
      }
    }

    private def emitMultiply(typeInfo: TypeInfo[_], desc: MemberDescriptor): Unit = {
      descFrom(typeInfo) match {
        case "B" | "C" | "I" | "S" =>
          mv.visitInsn(IMUL)
        case "D" => // double
          mv.visitInsn(DMUL)
        case "J" => // long
          mv.visitInsn(LMUL)
        case "F" =>
          mv.visitInsn(FMUL)
        case _ =>
          invokeMethod(mv, desc.metadata(MetaMethod).asInstanceOf[Method])
      }
    }

    private def emitMinus(typeInfo: TypeInfo[_], desc: MemberDescriptor): Unit = {
      descFrom(typeInfo) match {
        case "B" | "C" | "I" | "S" =>
          mv.visitInsn(ISUB)
        case "D" => // double
          mv.visitInsn(DSUB)
        case "J" => // long
          mv.visitInsn(LSUB)
        case "F" =>
          mv.visitInsn(FSUB)
        case _ =>
          invokeMethod(mv, desc.metadata(MetaMethod).asInstanceOf[Method])
      }
    }

    private def emitPlus(typeInfo: TypeInfo[_], desc: MemberDescriptor): Unit = {
      descFrom(typeInfo) match {
        case "B" | "C" | "I" | "S" =>
          mv.visitInsn(IADD)
        case "D" => // double
          mv.visitInsn(DADD)
        case "J" => // long
          mv.visitInsn(LADD)
        case "F" =>
          mv.visitInsn(FADD)
        case _ =>
          invokeMethod(mv, desc.metadata(MetaMethod).asInstanceOf[Method])
      }
    }

    private def emitCompare(typeInfo: TypeInfo[_], opCode1: Int, opCode2: Int, desc: MemberDescriptor): Unit = {
      val l0 = new Label()
      val l1 = new Label()
      descFrom(typeInfo) match {
        case "B" | "C" | "I" | "S" | "Z" =>
          mv.visitJumpInsn(opCode1, l0)
        case "D" => // double
          mv.visitInsn(DCMPL)
          mv.visitJumpInsn(opCode2, l0)
        case "J" => // long
          mv.visitInsn(LCMP)
          mv.visitJumpInsn(opCode2, l0)
        case "F" =>
          mv.visitInsn(FCMPL)
          mv.visitJumpInsn(opCode2, l0)
        case d =>
          if (desc.name == "compareTo") {
            mv.visitMethodInsn(INVOKEINTERFACE, Comparable, "compareTo", s"(L$Object;)I", true)
            mv.visitJumpInsn(opCode2, l0)
          } else {
            invokeMethod(mv, desc.metadata(MetaMethod).asInstanceOf[Method])
            mv.visitJumpInsn(GOTO, l1)
          }
      }
      mv.visitInsn(ICONST_1)
      mv.visitJumpInsn(GOTO, l1)
      mv.visitLabel(l0)
      mv.visitInsn(ICONST_0)
      mv.visitLabel(l1)
    }

    private def emitNotEqual(typeInfo: TypeInfo[_]): Unit = {
      val l0 = new Label()
      val l1 = new Label()
      descFrom(typeInfo) match {
        case "B" | "C" | "I" | "S" | "Z" =>
          mv.visitJumpInsn(IF_ICMPEQ, l0)
        case "D" => // double
          mv.visitInsn(DCMPL)
          mv.visitJumpInsn(IFEQ, l0)
        case "J" => // long
          mv.visitInsn(LCMP)
          mv.visitJumpInsn(IFEQ, l0)
        case "F" =>
          mv.visitInsn(FCMPL)
          mv.visitJumpInsn(IFEQ, l0)
        case _ =>
          val clazz = majorClassFrom(typeInfo)
          if (
            clazz == ObjectClass || classOf[Number].isAssignableFrom(clazz) || classOf[Character].isAssignableFrom(
              clazz)
          ) {
            mv.visitMethodInsn(INVOKESTATIC, BoxesRuntime, "equals", s"(L$Object;L$Object;)Z", false)
          } else {
            val var1 = currentVar + 1
            val var2 = currentVar
            mv.visitVarInsn(ASTORE, var2)
            mv.visitVarInsn(ASTORE, var1)

            mv.visitVarInsn(ALOAD, var1)
            val l2 = new Label()
            mv.visitJumpInsn(IFNONNULL, l2)
            mv.visitVarInsn(ALOAD, var2)
            mv.visitJumpInsn(IFNULL, l0)
            mv.visitInsn(ICONST_1)
            mv.visitJumpInsn(GOTO, l1)

            mv.visitLabel(l2)
            mv.visitVarInsn(ALOAD, var1)
            mv.visitVarInsn(ALOAD, var2)
            mv.visitMethodInsn(INVOKEVIRTUAL, Object, "equals", s"(L$Object;)Z", false)
          }
          mv.visitJumpInsn(IFNE, l0)
      }
      mv.visitInsn(ICONST_1)
      mv.visitJumpInsn(GOTO, l1)
      mv.visitLabel(l0)
      mv.visitInsn(ICONST_0)
      mv.visitLabel(l1)
    }

    private def emitEqual(typeInfo: TypeInfo[_]): Unit = {
      val l0 = new Label()
      val l1 = new Label()
      descFrom(typeInfo) match {
        case "B" | "C" | "I" | "S" | "Z" =>
          mv.visitJumpInsn(IF_ICMPNE, l0)
        case "D" => // double
          mv.visitInsn(DCMPL)
          mv.visitJumpInsn(IFNE, l0)
        case "J" => // long
          mv.visitInsn(LCMP)
          mv.visitJumpInsn(IFNE, l0)
        case "F" =>
          mv.visitInsn(FCMPL)
          mv.visitJumpInsn(IFNE, l0)
        case _ =>
          val clazz = majorClassFrom(typeInfo)
          if (
            clazz == ObjectClass || classOf[Number].isAssignableFrom(clazz) || classOf[Character].isAssignableFrom(
              clazz)
          ) {
            mv.visitMethodInsn(INVOKESTATIC, BoxesRuntime, "equals", s"(L$Object;L$Object;)Z", false)
          } else {
            val var1 = currentVar + 1
            val var2 = currentVar
            mv.visitVarInsn(ASTORE, var2)
            mv.visitVarInsn(ASTORE, var1)

            mv.visitVarInsn(ALOAD, var1)
            val l2 = new Label()
            mv.visitJumpInsn(IFNONNULL, l2)
            mv.visitVarInsn(ALOAD, var2)
            mv.visitJumpInsn(IFNONNULL, l0)
            mv.visitInsn(ICONST_1)
            mv.visitJumpInsn(GOTO, l1)

            mv.visitLabel(l2)
            mv.visitVarInsn(ALOAD, var1)
            mv.visitVarInsn(ALOAD, var2)
            mv.visitMethodInsn(INVOKEVIRTUAL, Object, "equals", s"(L$Object;)Z", false)
          }
          mv.visitJumpInsn(GOTO, l1)
      }
      mv.visitInsn(ICONST_1)
      mv.visitJumpInsn(GOTO, l1)
      mv.visitLabel(l0)
      mv.visitInsn(ICONST_0)
      mv.visitLabel(l1)
    }

    protected def emitNew(n: NewElement): Unit = {
      val (owner, ctorDesc) = if (n.members.isEmpty) {
        val cls = majorClassFrom(n.ctor.declaringType)
        val name = Type.getInternalName(cls)
        val desc = Type.getConstructorDescriptor(n.ctor.metadata(MetaMethod).asInstanceOf[Constructor[_]])
        (name, desc)
      } else {
        val name = mkAnonClassName
        val emitter = new AnonymousClassEmitter(name, n.rowTypeInfo, n.members.map(_.asInstanceOf[FieldDescriptor]))
        anonClasses = (name, emitter.emit()) :: anonClasses
        (name, emitter.constructorDesc)
      }
      mv.visitTypeInsn(NEW, owner)
      mv.visitInsn(DUP)
      n.arguments.foreach(emitElement)
      mv.visitMethodInsn(INVOKESPECIAL, owner, Ctor, ctorDesc, false)
    }

    protected def emitTypeIs(t: TypeIsElement): Unit = {
      emitElement(t.element)
      val l0 = new Label
      val l1 = new Label
      t.targetType.classes.foreach { cls =>
        val target = if (cls.isPrimitive) boxToName(Type.getDescriptor(cls)) else Type.getInternalName(cls)
        mv.visitInsn(DUP)
        mv.visitTypeInsn(INSTANCEOF, target)
        mv.visitJumpInsn(IFEQ, l0)
      }
      mv.visitInsn(POP)
      mv.visitInsn(ICONST_1)
      mv.visitJumpInsn(GOTO, l1)
      mv.visitLabel(l0)
      mv.visitInsn(POP)
      mv.visitInsn(ICONST_0)
      mv.visitLabel(l1)
    }

    protected def emitFuncCall(f: FuncElement): Unit = {
      f.callee match {
        case mc: MethodCallee =>
          mc.method.metadata(if (nodes.contains(f)) MetaMethodAsync else MetaMethod) match {
            case x: Method =>
              if ((f.instance eq null) && !Modifier.isStatic(x.getModifiers)) {
                val owner = Type.getInternalName(majorClassFrom(mc.method.declaringType))
                mv.visitFieldInsn(GETSTATIC, owner, "MODULE$", s"L$owner;")
              } else {
                emitElement(f.instance)
              }
              if (mc.method.metadata.get(MetaVargs).map(_ == true).getOrElse(false)) {
                val cls =
                  if (mc.method.genericArgTypes.size == 1) majorClassFrom(mc.method.genericArgTypes(0))
                  else classOf[Object]
                val clsName = Type.getInternalName(cls)
                val clsDesc = Type.getDescriptor(cls)

                mv.visitIntInsn(BIPUSH, f.arguments.size)
                if (cls.isPrimitive)
                  mv.visitIntInsn(NEWARRAY, arrayType(clsDesc))
                else
                  mv.visitTypeInsn(ANEWARRAY, clsName)
                f.arguments.zipWithIndex.foreach { case (e, i) =>
                  mv.visitInsn(DUP)
                  mv.visitIntInsn(BIPUSH, i)
                  emitElement(e)
                  storeIntoArray(mv, clsDesc)
                }
                if (cls.isPrimitive) {
                  val descriptor =
                    if (isAtLeastScala2_13) s"([$clsDesc)L$ArraySeq$$of${cls.getName.capitalize};"
                    else s"([$clsDesc)L$WrappedArray;"
                  mv.visitMethodInsn(INVOKESTATIC, Predef, s"wrap${cls.getName.capitalize}Array", descriptor, false)
                } else {
                  val descriptor =
                    if (isAtLeastScala2_13) s"([L$Object;)L$ArraySeq$$ofRef;"
                    else s"([L$Object;)L$WrappedArray;"
                  mv.visitMethodInsn(INVOKESTATIC, Predef, "wrapRefArray", descriptor, false)
                }
              } else {
                f.arguments.foreach(emitElement)
              }
              invokeMethod(mv, x)
          }
      }
    }

    protected def emitMember(m: MemberElement): Unit = {
      val desc = m.member
      if (nodes.contains(m)) {
        emitElement(m.instanceProvider)
        invokeMethod(mv, desc.metadata(MetaMethodAsync).asInstanceOf[Method])
      } else {
        desc.metadata.get(MetaMethod) map { case x: Method =>
          emitElement(m.instanceProvider)
          invokeMethod(mv, x)
        } getOrElse {
          emitAnonymousMember(m)
        }
      }
    }

    private def emitAnonymousMember(m: MemberElement): Unit = {
      val l0 = new Label()
      val l1 = new Label()
      val l2 = new Label()
      val l3 = new Label()

      mv.visitTryCatchBlock(l0, l1, l2, Exception)
      mv.visitLabel(l0)
      emitElement(m.instanceProvider)
      mv.visitInsn(DUP)
      mv.visitMethodInsn(INVOKEVIRTUAL, Object, "getClass", s"()L$Class;", false)
      mv.visitLdcInsn(m.member.name)
      mv.visitInsn(ICONST_0)
      mv.visitTypeInsn(ANEWARRAY, Class)
      mv.visitMethodInsn(INVOKEVIRTUAL, Class, "getMethod", s"(L$String;[L$Class;)L$Method;", false)
      mv.visitInsn(SWAP)
      mv.visitInsn(ICONST_0)
      mv.visitTypeInsn(ANEWARRAY, Object)
      mv.visitMethodInsn(INVOKEVIRTUAL, Method, "invoke", s"(L$Object;[L$Object;)L$Object;", false)
      if (majorClassFrom(m.rowTypeInfo) != ObjectClass)
        castTo(mv, descFrom(m.rowTypeInfo), m.rowTypeInfo.clazz)
      mv.visitLabel(l1)
      mv.visitJumpInsn(GOTO, l3)
      mv.visitLabel(l2)
      mv.visitVarInsn(ASTORE, currentVar)
      mv.visitTypeInsn(NEW, RelationalException)
      mv.visitInsn(DUP)
      mv.visitVarInsn(ALOAD, currentVar)
      mv.visitMethodInsn(INVOKEVIRTUAL, Exception, "getMessage", s"()L$String;", false)
      mv.visitVarInsn(ALOAD, currentVar)
      mv.visitMethodInsn(INVOKESPECIAL, RelationalException, Ctor, s"(L$String;Ljava/lang/Throwable;)V", false)
      mv.visitInsn(ATHROW)
      mv.visitLabel(l3)
    }

    protected def emitConstValue(c: ConstValueElement): Unit = {
      import TypeInfo._

      t.constants.indexes
        .get(c.value)
        .map { idx =>
          mv.visitVarInsn(ALOAD, 0) // this
          mv.visitFieldInsn(GETFIELD, className, ConstsInst, s"[L$Object;")
          mv.visitIntInsn(BIPUSH, idx)
          mv.visitInsn(AALOAD)
          if (majorClassFrom(c.rowTypeInfo) != ObjectClass)
            castTo(mv, descFrom(c.rowTypeInfo), c.rowTypeInfo.clazz)
        }
        .getOrElse {
          if (c.value == null) mv.visitInsn(ACONST_NULL)
          else
            c.rowTypeInfo match {
              case BOOLEAN => mv.visitInsn(if (c.value == true) ICONST_1 else ICONST_0)
              case BYTE    => mv.visitIntInsn(BIPUSH, c.value.asInstanceOf[Byte])
              case SHORT   => mv.visitIntInsn(SIPUSH, c.value.asInstanceOf[Short])
              case _ =>
                c.value match {
                  case c: Class[_] => mv.visitLdcInsn(Type.getType(c))
                  case v           => mv.visitLdcInsn(v)
                }
            }
        }
    }

    private def closureVarIndex(p: ParameterElement): Int = {
      needsClosure = true
      var lc = parent
      var idx = -1
      while ((lc ne null) && idx == -1) {
        if (!lc.isContinuation && lc.argOffset.contains(p))
          idx = lc.l.parameters.indexOf(p)
        lc.needsClosure = true
        lc = lc.parent
      }
      while (lc ne null) {
        if (!lc.isContinuation)
          idx += lc.l.parameters.size
        lc.needsClosure = true
        lc = lc.parent
      }
      idx
    }

    protected def emitParameter(p: ParameterElement): Unit = {
      // if the parameter is from arg list, load directly; else load from closure (idx?)
      argOffset.get(p).map(offset => loadMethodArg(mv, offset, descFrom(p.rowTypeInfo))).getOrElse {
        mv.visitVarInsn(ALOAD, 1) // load Closure
        mv.visitIntInsn(BIPUSH, closureVarIndex(p))
        mv.visitMethodInsn(INVOKEVIRTUAL, Closure, "resolve", s"(I)L$Object;", false)
        if (majorClassFrom(p.rowTypeInfo) != ObjectClass)
          castTo(mv, descFrom(p.rowTypeInfo), p.rowTypeInfo.clazz)
      }
    }
  }

  protected final class AnonymousClassEmitter(
      val className: String,
      shape: TypeInfo[_],
      members: List[FieldDescriptor]) {
    private val cw = new ClassWriter(ClassWriter.COMPUTE_FRAMES)
    private val paramsDescList = members.map(fd => fd -> descFrom(fd.fieldType))
    private val paramsDesc = paramsDescList.map(_._2).mkString("")

    val constructorDesc = s"($paramsDesc)V"

    def emit(): Array[Byte] = {
      val interfaces: Array[String] = shape.classes.iterator.map(i => Type.getInternalName(i)).toArray
      cw.visit(ClassfileVersion, ACC_PUBLIC + ACC_SUPER, className, null, Object, interfaces)
      // fields
      paramsDescList.foreach(t => cw.visitField(ACC_PRIVATE | ACC_FINAL, s"_${t._1.name}", t._2, null, null).visitEnd())
      // constructor
      emitConstructor()
      // accessors
      emitAccessors()
      // hashCode
      emitHashCode()
      // equals
      emitEquals()

      cw.visitEnd()
      cw.toByteArray
    }

    private def emitAccessors(): Unit = {
      paramsDescList.foreach { t =>
        // field descriptor
        val fd = t._1
        // field type desc
        val desc = t._2
        val methodDesc =
          fd.metadata.get(MetaMethod).map(m => Type.getMethodDescriptor(m.asInstanceOf[Method])).getOrElse(s"()$desc")
        val mv = cw.visitMethod(ACC_PUBLIC, fd.name, methodDesc, null, null)
        mv.visitCode()
        mv.visitVarInsn(ALOAD, 0)
        mv.visitFieldInsn(GETFIELD, className, s"_${fd.name}", desc)
        returnFromMethod(mv, desc)
        mv.visitMaxs(0, 0)
        mv.visitEnd()
      }
    }

    private def emitConstructor(): Unit = {
      val mv = cw.visitMethod(ACC_PUBLIC, Ctor, constructorDesc, null, null)
      mv.visitCode()
      mv.visitVarInsn(ALOAD, 0)
      mv.visitMethodInsn(INVOKESPECIAL, Object, Ctor, "()V", false)

      var idx = 1
      paramsDescList.foreach { t =>
        mv.visitVarInsn(ALOAD, 0)
        idx = loadMethodArg(mv, idx, t._2)
        mv.visitFieldInsn(PUTFIELD, className, s"_${t._1.name}", t._2)
      }
      mv.visitInsn(RETURN)
      mv.visitMaxs(0, 0)
      mv.visitEnd()
    }

    private def emitHashCode(): Unit = {
      val mv = cw.visitMethod(ACC_PUBLIC, "hashCode", "()I", null, null)
      mv.visitCode()
      mv.visitIntInsn(BIPUSH, 37) // int acc = 37
      mv.visitVarInsn(ISTORE, 1)

      paramsDescList.foreach { t =>
        // field descriptor
        val fd = t._1
        // field type desc
        val desc = t._2
        mv.visitVarInsn(ALOAD, 0)
        mv.visitFieldInsn(GETFIELD, className, s"_${fd.name}", desc)
        hashTheValue(mv, desc, 17)
      }
      mv.visitVarInsn(ILOAD, 1)
      mv.visitInsn(IRETURN)
      mv.visitMaxs(0, 0)
      mv.visitEnd()
    }

    private def emitEquals(): Unit = {
      val mv = cw.visitMethod(ACC_PUBLIC, "equals", s"(L$Object;)Z", null, null)
      mv.visitCode()
      equalsCommon(mv, className)

      paramsDescList.foreach { t =>
        // field descriptor
        val fd = t._1
        // field type desc
        val desc = t._2
        mv.visitVarInsn(ALOAD, 0)
        mv.visitFieldInsn(GETFIELD, className, s"_${fd.name}", desc)
        mv.visitVarInsn(ALOAD, 2)
        mv.visitFieldInsn(GETFIELD, className, s"_${fd.name}", desc)
        areEqual(mv, desc, 3, 4)
      }

      mv.visitInsn(ICONST_1)
      mv.visitInsn(IRETURN)
      mv.visitMaxs(0, 0)
      mv.visitEnd()
    }
  }

  protected def mkAnonClassName: String = {
    val suffix = anonClasses.size + 1
    s"$$anon$$$suffix"
  }
}

object LambdaCompiler {
  def compile[T](l: LambdaElement): T = {
    val removed = MultiRelationElementRemover.remove(l)
    val e = Typer.typeCheck(removed).asInstanceOf[LambdaElement]
    val (lambda, analyzedTree) = analyzeLambda(e)
    new LambdaCompiler(analyzedTree, lambda).compile[T]()
  }

  def compileAsNodeOrFunc0[R](l: LambdaElement): Either[() => Node[R], () => R] = {
    Func.asEither(compile[() => Any](l))
  }

  def compileAsNodeOrFunc1[T, R](l: LambdaElement): Either[T => Node[R], T => R] = {
    Func.asEither(compile[T => Any](l))
  }

  def compileAsNodeOrFunc2[T1, T2, R](l: LambdaElement): Either[(T1, T2) => Node[R], (T1, T2) => R] = {
    Func.asEither(compile[(T1, T2) => Any](l))
  }

  private[optimus] def dumpToFile(l: LambdaElement, file: String): Unit = {
    val e = Typer.typeCheck(l).asInstanceOf[LambdaElement]
    val (lambda, analyzedTree) = analyzeLambda(e)
    new LambdaCompiler(analyzedTree, lambda).dumpToFile(file)
  }

  private def analyzeLambda(l: LambdaElement): (LambdaElement, AnalyzedTree) = {
    val (lam: LambdaElement, nodeDependencies) = NodeAnalyzer.analyze(l)
    (lam, VariableBinder.bind(lam, nodeDependencies))
  }
}
