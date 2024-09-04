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

import java.lang.reflect.Method
import java.lang.reflect.Modifier
import java.math.BigInteger
import java.security.MessageDigest
import java.util

import msjava.slf4jutils.scalalog
import optimus.graph.Settings
import org.objectweb.asm.Opcodes._
import org.objectweb.asm._
import optimus.platform.relational.RelationalUnsupportedException
import optimus.platform.relational.tree.ProxyFinalModifier
import optimus.platform.relational.tree.TypeInfo
import optimus.platform.util.ScalaCompilerUtils

import scala.collection.mutable
import scala.reflect.internal
import scala.reflect.runtime.currentMirror
import scala.reflect.runtime.universe._
import scala.tools.reflect.ToolBoxError

final class ASMProxyGenerator(mainType: TypeInfo[_], extType: TypeInfo[_]) extends ASMGeneratorUtils with ASMConstants {
  import ASMProxyGenerator._

  val className = ASMProxyGenerator.classNameFor(mainType, extType)
  val majorMainBase = majorBaseFrom(mainType)
  val majorMainBaseDesc = s"L$majorMainBase;"
  val majorExtBase = majorBaseFrom(extType)
  val majorExtBaseDesc = s"L$majorExtBase;"

  val concreteType = if (extType.concreteClass.isDefined) extType else mainType
  val concreteBase = concreteType.concreteClass.map(Type.getInternalName(_)).getOrElse(Object)
  val pathDependentClass = findPathDependentClass(concreteType)
  val isEntity = concreteType.concreteClass.map(EntityClass.isAssignableFrom(_)).getOrElse(false)
  val interfaces = Set(mainType, extType).flatMap(_.interfaces).toList
  require(interfaces.forall(!EntityClass.isAssignableFrom(_)), "Trait derived from Entity is not supported!")

  val proxyFinals = {
    // all methods in interfaces, should be either abstract or annotated with '@ProxyFinalModifier final'
    val result = new mutable.ListBuffer[Method]
    val identities = new mutable.HashMap[String, Method]
    val cm = currentMirror
    val jcm = cm.asInstanceOf[{ def methodToJava(meth: internal.Symbols#MethodSymbol): Method }]
    val symbols = interfaces.flatMap(i => cm.classSymbol(i).baseClasses.filter(s => s.asClass.isTrait)).distinct
    symbols.foreach { clsSym =>
      clsSym.asType.toType.decls.filter(!_.isConstructor).foreach { memSym =>
        if (!memSym.isMethod)
          throw new RelationalUnsupportedException(s"$memSym of ${clsSym.fullName} is not a method!")
        if (!memSym.isAbstract) {
          if (!memSym.isFinal || !memSym.annotations.exists(_.tree.tpe =:= ProxyFinalModifierType))
            throw new RelationalUnsupportedException(
              s"$memSym of ${clsSym.fullName} must be marked as '@ProxyFinalModifier final'!")
          val meth = jcm.methodToJava(memSym.asInstanceOf[internal.Symbols#MethodSymbol])
          identities.put(methodIdentity(meth), meth).foreach { case m =>
            throw new RelationalUnsupportedException(
              s"Duplicate '@ProxyFinalModifier final def ${m.getName}' on ${meth.getDeclaringClass} and ${m.getDeclaringClass}")
          }
          result.append(meth)
        }
      }
    }
    result.toList
  }

  val baseCtorParams = {
    concreteType.concreteClass.foreach { c =>
      require(
        c.getConstructors.exists(_.getParameterCount == concreteType.primaryConstructorParams.size),
        s"Invalid primaryConstructorParams ${concreteType.primaryConstructorParams.map(_._2).mkString("(", ", ", ")")} from $concreteType"
      )
    }
    concreteType.primaryConstructorParams.map(t => Type.getDescriptor(t._2)).mkString("")
  }
  val ctorDesc = s"($majorMainBaseDesc$majorExtBaseDesc$baseCtorParams)V"
  val allFields = Seq(mainType, extType).flatMap(_.propertyNames).distinct
  val finalFields: Set[String] = List(mainType, extType)
    .flatMap(_.nonStructuralProperties)
    .filter { case (_, m) =>
      Modifier.isFinal(m.getModifiers()) // final fields must come from concreteClass
    }
    .iterator
    .map(_._1)
    .toSet
  val (asyncFields, otherFields) = {
    val (af1, of1) = partitionFields(mainType, mainType.propertyNames.filter(t => !finalFields.contains(t)))
    val (af2, of2) = partitionFields(extType, extType.propertyNames.filter(t => !finalFields.contains(t)))
    ((af1 ++ af2).toSet, (of1 ++ of2).toSet)
  }
  private val visited = new util.HashSet[(String, String)]
  private var factoryRequireReflection = false

  private val mainFieldLookup = mkFieldDescLookup(mainType, mainType.propertyNames)
  private val extFieldLookup = mkFieldDescLookup(extType, extType.propertyNames)

  private def fieldDesc(fn: String): (String, Boolean) = {
    // returns (FieldDesc, IsMain)
    mainFieldLookup.get(fn).map(t => (t, true)).getOrElse((extFieldLookup(fn), false))
  }

  def generateProxyClass(): Array[Byte] = {
    val cw = new ClassWriter(ClassWriter.COMPUTE_FRAMES)
    val interfaceArray = (ProxyMarkerTrait :: interfaces.map(Type.getInternalName(_))).toArray
    cw.visit(ClassfileVersion, ACC_PUBLIC + ACC_SUPER, className, null, concreteBase, interfaceArray)

    // fields
    defineFields(cw)
    // constructor
    defineConstructor(cw)
    // getters for interfaces
    defineGetters(cw)
    // equals
    defineEquals(cw)
    // hashCode
    defineHashCode(cw)
    // toString
    defineToString(cw)
    // delegates to all non-property interface methods
    defineDelegates(cw)

    // factory
    defineFactory(cw)
    // statics
    defineStatics(cw)

    cw.visitEnd()
    cw.toByteArray()
  }

  private def defineFields(cw: ClassWriter): Unit = {
    cw.visitField(ACC_PRIVATE + ACC_FINAL, LeftInst, majorMainBaseDesc, null, null).visitEnd()
    cw.visitField(ACC_PRIVATE + ACC_FINAL, RightInst, majorExtBaseDesc, null, null).visitEnd()
  }

  private def defineConstructor(cw: ClassWriter): Unit = {
    val mv = cw.visitMethod(ACC_PUBLIC, Ctor, ctorDesc, null, null)
    mv.visitCode()
    // super(...)
    mv.visitVarInsn(ALOAD, 0)

    var paramListDesc = baseCtorParams
    pathDependentClass.foreach { outerClass =>
      val desc = Type.getDescriptor(outerClass)
      mv.visitFieldInsn(GETSTATIC, className, OuterField, desc)
      paramListDesc = desc + paramListDesc
    }

    if (concreteType.primaryConstructorParams.nonEmpty) {
      var index = 3
      concreteType.primaryConstructorParams.foreach { t =>
        index = loadMethodArg(mv, index, Type.getDescriptor(t._2))
      }
    }

    mv.visitMethodInsn(INVOKESPECIAL, concreteBase, Ctor, s"($paramListDesc)V", false)
    // left
    mv.visitVarInsn(ALOAD, 0)
    mv.visitVarInsn(ALOAD, 1)
    mv.visitFieldInsn(PUTFIELD, className, LeftInst, majorMainBaseDesc)
    // right
    mv.visitVarInsn(ALOAD, 0)
    mv.visitVarInsn(ALOAD, 2)
    mv.visitFieldInsn(PUTFIELD, className, RightInst, majorExtBaseDesc)

    mv.visitInsn(RETURN)
    mv.visitMaxs(0, 0)
    mv.visitEnd()
  }

  private def defineGetters(cw: ClassWriter): Unit = {
    val (hasError, fields) = ScalaCompilerUtils.rightMasksLeft(extType, mainType)
    if (hasError) // to be compatible with ScalaToolboxProxyFactory
      throw new ToolBoxError(
        s"Cannot proxy left: $mainType and right: $extType. Concrete right type visible constructor parameters (${fields
            .mkString(",")}) mask left defs/vals")

    asyncFields.foreach(fn => {
      val (desc, fromMain) = fieldDesc(fn)
      val instType = if (fromMain) mainType else extType
      val inst = if (fromMain) LeftInst else RightInst
      val instDesc = if (fromMain) majorMainBaseDesc else majorExtBaseDesc
      val instName = if (fromMain) majorMainBase else majorExtBase

      {
        val mv = cw.visitMethod(ACC_PUBLIC, fn, s"()$desc", null, null)
        mv.visitCode()
        mv.visitVarInsn(ALOAD, 0)
        mv.visitFieldInsn(GETFIELD, className, inst, instDesc)
        invokeField(mv, instType.nonStructuralProperties(fn), instName, instType)
        returnFromMethod(mv, desc)
        mv.visitMaxs(0, 0)
        mv.visitEnd()
      }

      {
        val fn$queued = s"$fn$$queued"
        val fn$queuedDesc = s"()L$Node;"
        visited.add((fn$queued, fn$queuedDesc))
        val mv = cw.visitMethod(ACC_PUBLIC, fn$queued, fn$queuedDesc, s"()L$Node<$desc>;", null)
        mv.visitCode()
        mv.visitVarInsn(ALOAD, 0)
        mv.visitFieldInsn(GETFIELD, className, inst, instDesc)
        invokeAsyncField(mv, fn, desc, instName, instType)
        returnFromMethod(mv, s"L$Node;")
        mv.visitMaxs(0, 0)
        mv.visitEnd()
      }
    })

    otherFields.foreach { fn =>
      val (desc, fromMain) = fieldDesc(fn)
      val mv = cw.visitMethod(ACC_PUBLIC, fn, s"()$desc", null, null)
      mv.visitCode()

      val instType = if (fromMain) mainType else extType
      val inst = if (fromMain) LeftInst else RightInst
      val instDesc = if (fromMain) majorMainBaseDesc else majorExtBaseDesc
      val instName = if (fromMain) majorMainBase else majorExtBase

      instType.nonStructuralProperties.get(fn) match {
        case Some(m) =>
          mv.visitVarInsn(ALOAD, 0)
          mv.visitFieldInsn(GETFIELD, className, inst, instDesc)
          invokeField(mv, m, instName, instType)
          returnFromMethod(mv, desc)
          mv.visitMaxs(1, 1)
          mv.visitEnd()

        case _ => // pure structural method
          readFieldUsingReflection(mv, className, fn, desc, inst, instDesc, instType)
          mv.visitMaxs(0, 0)
          mv.visitEnd()
      }
    }
  }

  private def defineEquals(cw: ClassWriter): Unit = {
    visited.add(("equals", s"(L$Object;)Z"))
    if (isEntity) {
      visited.add(("argsEquals", "(Loptimus/platform/storable/Entity;)Z"))
      val mv = cw.visitMethod(ACC_PUBLIC, "argsEquals", "(Loptimus/platform/storable/Entity;)Z", null, null)
      mv.visitCode()
      equalsCommon(mv, className)

      // compare left$instance
      mv.visitVarInsn(ALOAD, 0)
      mv.visitFieldInsn(GETFIELD, className, LeftInst, majorMainBaseDesc)
      mv.visitVarInsn(ALOAD, 2)
      mv.visitFieldInsn(GETFIELD, className, LeftInst, majorMainBaseDesc)
      areEqual(mv, majorMainBaseDesc, 3, 4)

      // compare right$instance
      mv.visitVarInsn(ALOAD, 0)
      mv.visitFieldInsn(GETFIELD, className, RightInst, majorExtBaseDesc)
      mv.visitVarInsn(ALOAD, 2)
      mv.visitFieldInsn(GETFIELD, className, RightInst, majorExtBaseDesc)
      areEqual(mv, majorExtBaseDesc, 3, 4)

      mv.visitInsn(ICONST_1)
      mv.visitInsn(IRETURN)
      mv.visitMaxs(0, 0)
      mv.visitEnd()
    } else {
      val mv = cw.visitMethod(ACC_PUBLIC, "equals", s"(L$Object;)Z", null, null)
      mv.visitCode()
      equalsCommon(mv, className)

      allFields.foreach { fn =>
        val (desc, _) = fieldDesc(fn)
        mv.visitVarInsn(ALOAD, 0)
        mv.visitMethodInsn(INVOKESPECIAL, className, fn, s"()$desc", false)
        mv.visitVarInsn(ALOAD, 2)
        mv.visitMethodInsn(INVOKESPECIAL, className, fn, s"()$desc", false)
        areEqual(mv, desc, 3, 4)
      }

      mv.visitInsn(ICONST_1)
      mv.visitInsn(IRETURN)
      mv.visitMaxs(0, 0)
      mv.visitEnd()
    }
  }

  private def defineHashCode(cw: ClassWriter): Unit = {
    visited.add(("hashCode", "()I"))
    if (isEntity) {
      visited.add(("argsHash", "()I"))
      val mv = cw.visitMethod(ACC_PUBLIC, "argsHash", "()I", null, null)
      mv.visitCode()
      mv.visitIntInsn(BIPUSH, 37) // int acc = 37
      mv.visitVarInsn(ISTORE, 1)

      // left$instance
      mv.visitVarInsn(ALOAD, 0)
      mv.visitFieldInsn(GETFIELD, className, LeftInst, majorMainBaseDesc)
      hashTheValue(mv, majorMainBaseDesc, 17)
      // right$instance
      mv.visitVarInsn(ALOAD, 0)
      mv.visitFieldInsn(GETFIELD, className, RightInst, majorExtBaseDesc)
      hashTheValue(mv, majorExtBaseDesc, 17)

      mv.visitVarInsn(ILOAD, 1)
      mv.visitInsn(IRETURN)
      mv.visitMaxs(0, 0)
      mv.visitEnd()
    } else {
      val mv = cw.visitMethod(ACC_PUBLIC, "hashCode", "()I", null, null)
      mv.visitCode()
      mv.visitIntInsn(BIPUSH, 37) // int acc = 37
      mv.visitVarInsn(ISTORE, 1)

      allFields.foreach { fn =>
        val (desc, _) = fieldDesc(fn)
        mv.visitVarInsn(ALOAD, 0)
        mv.visitMethodInsn(INVOKESPECIAL, className, fn, s"()$desc", false)
        hashTheValue(mv, desc, 17)
      }
      mv.visitVarInsn(ILOAD, 1)
      mv.visitInsn(IRETURN)
      mv.visitMaxs(0, 0)
      mv.visitEnd()
    }

  }

  private def defineToString(cw: ClassWriter): Unit = {
    val stringDesc = s"L$String;"
    visited.add(("toString", s"()$stringDesc"))
    val mv = cw.visitMethod(ACC_PUBLIC, "toString", s"()$stringDesc", null, null)
    mv.visitCode()

    mv.visitTypeInsn(NEW, StringBuilder)
    mv.visitInsn(DUP)
    mv.visitMethodInsn(INVOKESPECIAL, StringBuilder, Ctor, "()V", false)

    val types = TypeInfo.intersect(mainType, extType).classes.map(_.getSimpleName).mkString(" with ")
    mv.visitLdcInsn(s"ScalaProxy(")
    mv.visitMethodInsn(INVOKEVIRTUAL, StringBuilder, "append", s"($stringDesc)L$StringBuilder;", false)

    var first = true
    allFields.foreach { case fn =>
      if (first) first = false
      else {
        mv.visitLdcInsn(",")
        mv.visitMethodInsn(INVOKEVIRTUAL, StringBuilder, "append", s"($stringDesc)L$StringBuilder;", false)
      }

      mv.visitLdcInsn(fn)
      mv.visitMethodInsn(INVOKEVIRTUAL, StringBuilder, "append", s"(L$Object;)L$StringBuilder;", false)
      mv.visitLdcInsn("=")
      mv.visitMethodInsn(INVOKEVIRTUAL, StringBuilder, "append", s"(L$Object;)L$StringBuilder;", false)

      val (desc, _) = fieldDesc(fn)
      mv.visitVarInsn(ALOAD, 0)
      mv.visitMethodInsn(INVOKESPECIAL, className, fn, s"()$desc", false)
      if (isPrimitive(desc) || desc == stringDesc)
        mv.visitMethodInsn(INVOKEVIRTUAL, StringBuilder, "append", s"($desc)L$StringBuilder;", false)
      else
        mv.visitMethodInsn(INVOKEVIRTUAL, StringBuilder, "append", s"(L$Object;)L$StringBuilder;", false)
    }

    mv.visitLdcInsn(")")
    mv.visitMethodInsn(INVOKEVIRTUAL, StringBuilder, "append", s"($stringDesc)L$StringBuilder;", false)
    mv.visitMethodInsn(INVOKEVIRTUAL, Object, "toString", s"()$stringDesc", false)

    mv.visitInsn(ARETURN)
    mv.visitMaxs(0, 0)
    mv.visitEnd()
  }

  private def getDelegateMethods(t: TypeInfo[_]) = {
    t.interfaces
      .flatMap(_.getMethods.filter(m =>
        m.getParameterCount != 0 || !(asyncFields.contains(m.getName) || otherFields.contains(m.getName))))
      .filter(m => !Modifier.isStatic(m.getModifiers))
  }

  private def defineProxyFinalDelegate(cw: ClassWriter, m: Method): Unit = {
    val name = m.getName
    val desc = Type.getMethodDescriptor(m)
    val unprocessed = visited.add((name, desc))
    if (unprocessed && !isScala212OrLater) {
      val ownerClass = m.getDeclaringClass
      val owner = Type.getInternalName(ownerClass)
      val mv = cw.visitMethod(ACC_PUBLIC, name, desc, null, null)
      mv.visitCode()
      mv.visitVarInsn(ALOAD, 0)
      mv.visitTypeInsn(CHECKCAST, owner)

      // layout arguments
      var index = 1
      m.getParameters.foreach { p =>
        index = loadMethodArg(mv, index, Type.getDescriptor(p.getType))
      }
      val implDesc = s"(L$owner;${desc.substring(1)}"
      val implOwner = s"$owner$$class"
      mv.visitMethodInsn(INVOKESTATIC, implOwner, name, implDesc, false)
      returnFromMethod(mv, Type.getDescriptor(m.getReturnType))
      mv.visitMaxs(0, 0)
      mv.visitEnd()
    }
  }

  private def defineAbstractDelegate(cw: ClassWriter, m: Method, fromMain: Boolean): Unit = {
    val name = m.getName
    val desc = Type.getMethodDescriptor(m)
    if (visited.add((name, desc))) {
      val ownerClass = m.getDeclaringClass
      val owner = Type.getInternalName(ownerClass)
      val mv = cw.visitMethod(ACC_PUBLIC, name, desc, null, null)
      mv.visitCode()
      mv.visitVarInsn(ALOAD, 0)
      if (fromMain) mv.visitFieldInsn(GETFIELD, className, LeftInst, majorMainBaseDesc)
      else mv.visitFieldInsn(GETFIELD, className, RightInst, majorExtBaseDesc)
      mv.visitTypeInsn(CHECKCAST, owner)

      // layout arguments
      var index = 1
      m.getParameters.foreach { p =>
        index = loadMethodArg(mv, index, Type.getDescriptor(p.getType))
      }
      mv.visitMethodInsn(INVOKEINTERFACE, owner, name, desc, true)
      returnFromMethod(mv, Type.getDescriptor(m.getReturnType))
      mv.visitMaxs(0, 0)
      mv.visitEnd()
    }
  }

  private def defineDelegates(cw: ClassWriter): Unit = {
    concreteType.concreteClass.foreach { c =>
      // for all public instance methods in concrete class, we add it to visited
      c.getMethods
        .filter(m => {
          val mods = m.getModifiers
          Modifier.isPublic(mods) && !Modifier.isStatic(mods)
        })
        .foreach(m => visited.add((m.getName, Type.getMethodDescriptor(m))))
    }

    proxyFinals.foreach(m => defineProxyFinalDelegate(cw, m))

    val mainDelegates = getDelegateMethods(mainType)
    mainDelegates.foreach(m => defineAbstractDelegate(cw, m, true))
    val extDelegates = getDelegateMethods(extType)
    extDelegates.foreach(m => defineAbstractDelegate(cw, m, false))
  }

  private def defineFactory(cw: ClassWriter): Unit = {
    val hasCtorParams = concreteType.primaryConstructorParams.nonEmpty

    {
      val mv = cw.visitMethod(
        ACC_PRIVATE + ACC_STATIC,
        "apply",
        s"($majorMainBaseDesc$majorExtBaseDesc)L$className;",
        null,
        null)
      mv.visitCode()

      mv.visitTypeInsn(NEW, className)
      mv.visitInsn(DUP)
      mv.visitVarInsn(ALOAD, 0) // left
      mv.visitVarInsn(ALOAD, 1) // right

      if (hasCtorParams) {
        val varInst = if (concreteType eq mainType) 0 else 1
        val methods = ASMGeneratorUtils.extractCtorParamMethods(
          concreteType.concreteClass.get,
          concreteType.primaryConstructorParams.iterator.map(_._1).toArray)
        concreteType.primaryConstructorParams.map(_._2).zipWithIndex.foreach { case (clazz, idx) =>
          val m = methods(idx)
          if (Modifier.isPublic(m.getModifiers)) {
            mv.visitVarInsn(ALOAD, varInst)
            invokeField(mv, m, concreteBase, concreteType)
          } else {
            factoryRequireReflection = true
            mv.visitFieldInsn(GETSTATIC, className, CtorParamsField, s"[L$Method;")
            mv.visitIntInsn(BIPUSH, idx)
            mv.visitInsn(AALOAD)
            mv.visitVarInsn(ALOAD, varInst)
            mv.visitInsn(ACONST_NULL)
            mv.visitMethodInsn(INVOKEVIRTUAL, Method, "invoke", s"(L$Object;[L$Object;)L$Object;", false)
            castTo(mv, Type.getDescriptor(clazz), clazz)
          }
        }
      }

      mv.visitMethodInsn(INVOKESPECIAL, className, Ctor, ctorDesc, false)
      mv.visitInsn(ARETURN)
      mv.visitMaxs(0, 0)
      mv.visitEnd()
    }

    {
      val mv = cw.visitMethod(ACC_PUBLIC + ACC_STATIC, ASMGeneratorUtils.FactoryName, s"()L$BiFunction;", null, null)
      mv.visitCode()
      mv.visitInvokeDynamicInsn(
        "apply",
        s"()L$BiFunction;",
        mkBootstrapHandle,
        Seq(
          Type.getType(s"(L$Object;L$Object;)L$Object;"),
          new Handle(
            Opcodes.H_INVOKESTATIC,
            className,
            "apply",
            s"($majorMainBaseDesc$majorExtBaseDesc)L$className;",
            false),
          Type.getType(s"($majorMainBaseDesc$majorExtBaseDesc)L$className;")
        ): _*
      )
      mv.visitInsn(ARETURN)
      mv.visitMaxs(0, 0)
      mv.visitEnd()
    }
  }

  // must be called after defineFactory method
  private def defineStatics(cw: ClassWriter): Unit = {
    if (factoryRequireReflection)
      cw.visitField(ACC_PRIVATE + ACC_STATIC + ACC_FINAL, CtorParamsField, s"[L$Method;", null, null).visitEnd()
    pathDependentClass.foreach { outerClass =>
      cw.visitField(ACC_PRIVATE + ACC_STATIC + ACC_FINAL, OuterField, Type.getDescriptor(outerClass), null, null)
        .visitEnd()
    }

    val mv = cw.visitMethod(ACC_STATIC, "<clinit>", "()V", null, null)
    mv.visitCode()

    if (factoryRequireReflection) {
      mv.visitIntInsn(BIPUSH, concreteType.primaryConstructorParams.size)
      mv.visitTypeInsn(ANEWARRAY, String)
      mv.visitVarInsn(ASTORE, 1)
      concreteType.primaryConstructorParams.map(_._1).zipWithIndex.foreach { case (name, idx) =>
        mv.visitVarInsn(ALOAD, 1)
        mv.visitIntInsn(BIPUSH, idx)
        mv.visitLdcInsn(name)
        mv.visitInsn(AASTORE)
      }

      mv.visitLdcInsn(Type.getType(concreteType.concreteClass.get))
      mv.visitVarInsn(ALOAD, 1)
      mv.visitMethodInsn(
        INVOKESTATIC,
        ASMGeneratorUtilsCls,
        "extractCtorParamMethods",
        s"(L$Class;[L$String;)[L$Method;",
        true)
      mv.visitFieldInsn(PUTSTATIC, className, CtorParamsField, s"[L$Method;")
    }

    pathDependentClass.foreach { outerClass =>
      val outer = Type.getInternalName(outerClass)
      mv.visitTypeInsn(NEW, outer)
      mv.visitInsn(DUP)
      mv.visitMethodInsn(INVOKESPECIAL, outer, Ctor, s"()V", false)
      mv.visitFieldInsn(PUTSTATIC, className, OuterField, Type.getDescriptor(outerClass))
    }

    mv.visitInsn(RETURN)
    mv.visitMaxs(0, 0)
    mv.visitEnd()
  }
}

object ASMProxyGenerator {
  import scala.util.Properties

  val log = scalalog.getLogger[ASMProxyGenerator.type]
  // since we won't have 2.10, 2.08 anyway !!!
  val isScala212OrLater = !Properties.versionNumberString.startsWith("2.11")

  private val ProxyFinalModifierType = weakTypeOf[ProxyFinalModifier]

  private def methodIdentity(m: Method): String = {
    val parameters = m.getParameterTypes()
    if (parameters.length == 0) s"${m.getName}()"
    else parameters.map(Type.getDescriptor).mkString(s"${m.getName}(", "", ")")
  }

  def classNameFor(mainType: TypeInfo[_], extType: TypeInfo[_]): String = {
    val sb = new StringBuffer()
    sb.append(mainType.typeString)
    if (mainType.primaryConstructorParams.nonEmpty) {
      sb.append('(')
      sb.append(mainType.primaryConstructorParams.map { case (name, clazz) => s"$name:${clazz.getName}" }.mkString(","))
      sb.append(')')
    }
    sb.append("---intersect---")
    sb.append(extType.typeString)
    if (extType.primaryConstructorParams.nonEmpty) {
      sb.append('(')
      sb.append(extType.primaryConstructorParams.map { case (name, clazz) => s"$name:${clazz.getName}" }.mkString(","))
      sb.append(')')
    }

    val code = sb.toString
    val digest = MessageDigest.getInstance("SHA-1").digest(code.getBytes)
    val name = s"proxy$$sha${new BigInteger(1, digest).toString(16)}"
    if (Settings.warnOnDemandCompiles)
      log.warn(s"Generate proxy class: $name -> $code")
    name
  }
}
