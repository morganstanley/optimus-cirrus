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

import java.math.BigInteger
import java.security.MessageDigest

import msjava.slf4jutils.scalalog
import optimus.graph.Settings
import org.objectweb.asm.Opcodes._
import org.objectweb.asm._
import optimus.platform.RelationKey
import optimus.platform.relational.tree.TypeInfo

final class ASMKeyGenerator(shapeType: TypeInfo[_], fields: Seq[String]) extends ASMGeneratorUtils with ASMConstants {
  val isComparable = RelationKey.isFieldsComparable(shapeType, fields)
  val (asyncFields, otherFields) = partitionFields(shapeType, fields)
  val asyncFieldsLookup = asyncFields.toSet
  val keyClassName: String = ASMKeyGenerator.classNameFor(shapeType, fields)
  val majorBase = majorBaseFrom(shapeType)
  val majorBaseDesc = s"L$majorBase;"
  val fieldDescLookup: Map[String, String] = mkFieldDescLookup(shapeType, fields)

  val ctorDesc = {
    val paramList = asyncFields.map(fn => fieldDesc(fn)).mkString("")
    s"($majorBaseDesc$paramList)V"
  }

  def generateKeyClass(): Array[Byte] = {
    val cw = new ClassWriter(ClassWriter.COMPUTE_FRAMES)
    if (isComparable) {
      val signature = s"L$Object;L$Comparable<L$keyClassName;>;"
      cw.visit(ClassfileVersion, ACC_PUBLIC + ACC_SUPER, keyClassName, signature, Object, Array(Comparable))
    } else cw.visit(ClassfileVersion, ACC_PUBLIC + ACC_SUPER, keyClassName, null, Object, null)

    // fields
    defineFields(cw)
    // constructor
    defineConstructor(cw)
    // getters for otherFields
    defineGetters(cw)
    // equals
    defineEquals(cw)
    // hashCode
    defineHashCode(cw)
    // compareTo
    if (isComparable)
      defineCompareTo(cw)

    defineFactory(cw)

    cw.visitEnd()
    cw.toByteArray()
  }

  private def defineFields(cw: ClassWriter): Unit = {
    if (otherFields.nonEmpty)
      cw.visitField(ACC_PRIVATE + ACC_FINAL, FieldInst, majorBaseDesc, null, null).visitEnd()

    asyncFields foreach { fn =>
      val desc = fieldDesc(fn)
      cw.visitField(ACC_PRIVATE + ACC_FINAL, fn, desc, null, null).visitEnd()
    }
  }

  private def defineConstructor(cw: ClassWriter): Unit = {
    val mv = cw.visitMethod(ACC_PUBLIC, Ctor, ctorDesc, null, null)
    mv.visitCode()
    mv.visitVarInsn(ALOAD, 0)
    mv.visitMethodInsn(INVOKESPECIAL, Object, Ctor, "()V", false)

    if (otherFields.nonEmpty) {
      mv.visitVarInsn(ALOAD, 0)
      mv.visitVarInsn(ALOAD, 1)
      mv.visitFieldInsn(PUTFIELD, keyClassName, FieldInst, majorBaseDesc)
    }

    var index = 2
    asyncFields.foreach { fn =>
      mv.visitVarInsn(ALOAD, 0)
      val desc = fieldDesc(fn)
      index = loadMethodArg(mv, index, desc)
      mv.visitFieldInsn(PUTFIELD, keyClassName, fn, desc)
    }
    mv.visitInsn(RETURN)
    mv.visitMaxs(3, index)
    mv.visitEnd()
  }

  private def defineGetters(cw: ClassWriter): Unit = {
    otherFields.foreach { fn =>
      val desc = fieldDesc(fn)
      val mv = cw.visitMethod(ACC_PRIVATE, fn, s"()$desc", null, null)
      mv.visitCode()

      shapeType.nonStructuralProperties.get(fn) match {
        case Some(m) =>
          mv.visitVarInsn(ALOAD, 0)
          mv.visitFieldInsn(GETFIELD, keyClassName, FieldInst, majorBaseDesc)
          invokeField(mv, m, majorBase, shapeType)
          returnFromMethod(mv, desc)
          mv.visitMaxs(1, 1)
          mv.visitEnd()

        case _ => // pure structural method
          // use reflection to access the field
          readFieldUsingReflection(mv, keyClassName, fn, desc, FieldInst, majorBaseDesc, shapeType)
          mv.visitMaxs(0, 0)
          mv.visitEnd()
      }
    }
  }

  private def defineEquals(cw: ClassWriter): Unit = {
    val mv = cw.visitMethod(ACC_PUBLIC, "equals", s"(L$Object;)Z", null, null)
    mv.visitCode()
    equalsCommon(mv, keyClassName)

    fields.foreach { fn =>
      val desc = fieldDesc(fn)
      loadLocalAndRemoteFields(mv, fn, desc, 0, 2)
      areEqual(mv, desc, 3, 4)
    }

    mv.visitInsn(ICONST_1)
    mv.visitInsn(IRETURN)
    mv.visitMaxs(0, 0)
    mv.visitEnd()
  }

  private def defineHashCode(cw: ClassWriter): Unit = {
    val mv = cw.visitMethod(ACC_PUBLIC, "hashCode", "()I", null, null)
    mv.visitCode()
    mv.visitIntInsn(BIPUSH, 37) // int acc = 37
    mv.visitVarInsn(ISTORE, 1)

    fields.foreach { fn =>
      val desc = fieldDesc(fn)
      if (asyncFieldsLookup.contains(fn)) {
        mv.visitVarInsn(ALOAD, 0)
        mv.visitFieldInsn(GETFIELD, keyClassName, fn, desc)
      } else {
        mv.visitVarInsn(ALOAD, 0)
        mv.visitMethodInsn(INVOKESPECIAL, keyClassName, fn, s"()$desc", false)
      }
      hashTheValue(mv, desc, 37)
    }
    mv.visitVarInsn(ILOAD, 1)
    mv.visitInsn(IRETURN)
    mv.visitMaxs(0, 0)
    mv.visitEnd()
  }

  private def defineCompareTo(cw: ClassWriter): Unit = {
    val positive = new Label
    val negative = new Label
    val return2 = new Label
    val compareToDesc = s"(L$keyClassName;)I";
    {
      val mv = cw.visitMethod(ACC_PUBLIC, "compareTo", compareToDesc, null, null)
      mv.visitCode()
      mv.visitFieldInsn(GETSTATIC, RelationKeyObject, "MODULE$", s"L$RelationKeyObject;")
      mv.visitVarInsn(ASTORE, 2)

      fields.foreach { fn =>
        val desc = fieldDesc(fn)
        if (!isPrimitive(desc))
          mv.visitVarInsn(ALOAD, 2)
        loadLocalAndRemoteFields(mv, fn, desc, 0, 1)
        compareFields(mv, fn, desc, positive, negative, return2)
      }
      mv.visitInsn(ICONST_0)
      mv.visitInsn(IRETURN)

      mv.visitLabel(positive)
      mv.visitInsn(ICONST_1)
      mv.visitInsn(IRETURN)

      mv.visitLabel(negative)
      mv.visitInsn(ICONST_M1)
      mv.visitInsn(IRETURN)

      mv.visitLabel(return2)
      mv.visitVarInsn(ILOAD, 3)
      mv.visitInsn(IRETURN)

      mv.visitMaxs(0, 0)
      mv.visitEnd()
    }
    {
      val mv = cw.visitMethod(ACC_PUBLIC + ACC_BRIDGE + ACC_SYNTHETIC, "compareTo", s"(L$Object;)I", null, null)
      mv.visitCode()
      mv.visitVarInsn(ALOAD, 0)
      mv.visitVarInsn(ALOAD, 1)
      mv.visitTypeInsn(CHECKCAST, keyClassName)
      mv.visitMethodInsn(INVOKEVIRTUAL, keyClassName, "compareTo", compareToDesc, false)
      mv.visitInsn(IRETURN)
      mv.visitMaxs(2, 2)
      mv.visitEnd()
    }
  }

  private def compareFields(
      mv: MethodVisitor,
      fn: String,
      typeDesc: String,
      positive: Label,
      negative: Label,
      return2: Label): Unit = {
    typeDesc match {
      case "B" | "C" | "I" | "S" | "Z" =>
        mv.visitVarInsn(ISTORE, 3)
        mv.visitVarInsn(ISTORE, 4)
        mv.visitVarInsn(ILOAD, 4)
        mv.visitVarInsn(ILOAD, 3)
        mv.visitJumpInsn(IF_ICMPGT, positive)
        mv.visitVarInsn(ILOAD, 4)
        mv.visitVarInsn(ILOAD, 3)
        mv.visitJumpInsn(IF_ICMPLT, negative)
        return
      case "D" =>
        mv.visitInsn(DCMPG)
      case "J" =>
        mv.visitInsn(LCMP)
      case "F" =>
        mv.visitInsn(FCMPG)
      case _ =>
        mv.visitMethodInsn(INVOKEVIRTUAL, RelationKeyObject, "compareTwoValues", s"(L$Object;L$Object;)I", false)
    }
    mv.visitVarInsn(ISTORE, 3)
    mv.visitVarInsn(ILOAD, 3)
    mv.visitJumpInsn(IFNE, return2)
  }

  private def loadLocalAndRemoteFields(mv: MethodVisitor, fn: String, desc: String, var1: Int, var2: Int): Unit = {
    if (asyncFieldsLookup.contains(fn)) {
      mv.visitVarInsn(ALOAD, var1)
      mv.visitFieldInsn(GETFIELD, keyClassName, fn, desc)
      mv.visitVarInsn(ALOAD, var2)
      mv.visitFieldInsn(GETFIELD, keyClassName, fn, desc)
    } else {
      mv.visitVarInsn(ALOAD, var1)
      mv.visitMethodInsn(INVOKESPECIAL, keyClassName, fn, s"()$desc", false)
      mv.visitVarInsn(ALOAD, var2)
      mv.visitMethodInsn(INVOKESPECIAL, keyClassName, fn, s"()$desc", false)
    }
  }

  private def defineFactory(cw: ClassWriter): Unit = {
    if (asyncFields.nonEmpty)
      defineLeft(cw)
    else
      defineRight(cw)
  }

  private def defineLeft(cw: ClassWriter): Unit = {
    {
      val mv = cw.visitMethod(
        ACC_PRIVATE + ACC_STATIC,
        "left",
        s"($majorBaseDesc)L$Node;",
        s"($majorBaseDesc)L$Node<L$keyClassName;>;",
        null)
      mv.visitCode()
      mv.visitFieldInsn(GETSTATIC, ContinuationObject, "MODULE$", s"L$ContinuationObject;")
      if (asyncFields.size == 1) {
        val fn = asyncFields.head
        val fDesc = fieldDesc(fn)
        mv.visitVarInsn(ALOAD, 0)
        invokeAsyncField(mv, fn, fDesc, majorBase, shapeType)

        mv.visitVarInsn(ALOAD, 0)
        mv.visitInvokeDynamicInsn(
          "apply",
          s"($majorBaseDesc)L$Function;",
          mkBootstrapHandle,
          Seq(
            Type.getType(s"(L$Object;)L$Object;"),
            new Handle(Opcodes.H_INVOKESTATIC, keyClassName, "lambda$left$0", s"($majorBaseDesc$fDesc)L$Node;", false),
            Type.getType(s"(${boxToDesc(fDesc)})L$Node;")
          ): _*
        )
        mv.visitMethodInsn(INVOKEVIRTUAL, ContinuationObject, "andThen", s"(L$NodeFuture;L$Function;)L$Node;", false)
        mv.visitInsn(ARETURN)
        mv.visitMaxs(0, 0)
        mv.visitEnd()

        {
          // the lambda impl bound by InvokeDynamic
          val mv = cw.visitMethod(
            ACC_PRIVATE + ACC_STATIC + ACC_SYNTHETIC,
            "lambda$left$0",
            s"($majorBaseDesc$fDesc)L$Node;",
            null,
            null)
          mv.visitCode()
          mv.visitTypeInsn(NEW, AlreadyCompletedNode)
          mv.visitInsn(DUP)
          mv.visitTypeInsn(NEW, keyClassName)
          mv.visitInsn(DUP)
          mv.visitVarInsn(ALOAD, 0)
          loadMethodArg(mv, 1, fDesc)
          mv.visitMethodInsn(INVOKESPECIAL, keyClassName, Ctor, ctorDesc, false)
          mv.visitMethodInsn(INVOKESPECIAL, AlreadyCompletedNode, Ctor, s"(L$Object;)V", false)
          mv.visitInsn(ARETURN)
          mv.visitMaxs(0, 0)
          mv.visitEnd()
        }
      } else {
        mv.visitIntInsn(BIPUSH, asyncFields.size)
        mv.visitTypeInsn(ANEWARRAY, Node)
        mv.visitVarInsn(ASTORE, 1)

        asyncFields.zipWithIndex.foreach { case (fn, idx) =>
          val fDesc = fieldDesc(fn)
          mv.visitVarInsn(ALOAD, 1)
          mv.visitIntInsn(BIPUSH, idx)
          mv.visitVarInsn(ALOAD, 0)
          invokeAsyncField(mv, fn, fDesc, majorBase, shapeType)
          mv.visitInsn(AASTORE)
        }

        mv.visitVarInsn(ALOAD, 1)
        mv.visitVarInsn(ALOAD, 0)
        val objArrayDesc = s"[L$Object;"
        mv.visitInvokeDynamicInsn(
          "apply",
          s"($majorBaseDesc)L$Function;",
          mkBootstrapHandle,
          Seq(
            Type.getType(s"(L$Object;)L$Object;"),
            new Handle(
              Opcodes.H_INVOKESTATIC,
              keyClassName,
              "lambda$left$0",
              s"($majorBaseDesc$objArrayDesc)L$Node;",
              false),
            Type.getType(s"($objArrayDesc)L$Node;")
          ): _*
        )
        mv.visitMethodInsn(INVOKEVIRTUAL, ContinuationObject, "whenAll", s"([L$NodeFuture;L$Function;)L$Node;", false)
        mv.visitInsn(ARETURN)
        mv.visitMaxs(0, 0)
        mv.visitEnd()

        {
          val mv = cw.visitMethod(
            ACC_PRIVATE + ACC_STATIC + ACC_SYNTHETIC,
            "lambda$left$0",
            s"($majorBaseDesc$objArrayDesc)L$Node;",
            null,
            null)
          mv.visitCode()
          mv.visitTypeInsn(NEW, AlreadyCompletedNode)
          mv.visitInsn(DUP)
          mv.visitTypeInsn(NEW, keyClassName)
          mv.visitInsn(DUP)
          mv.visitVarInsn(ALOAD, 0)
          asyncFields.zipWithIndex.foreach { case (fn, idx) =>
            mv.visitVarInsn(ALOAD, 1) // Object[]
            mv.visitIntInsn(BIPUSH, idx)
            mv.visitInsn(AALOAD)
            castTo(mv, fieldDesc(fn), shapeType.propertyMap(fn).clazz)
          }
          mv.visitMethodInsn(INVOKESPECIAL, keyClassName, Ctor, ctorDesc, false)
          mv.visitMethodInsn(INVOKESPECIAL, AlreadyCompletedNode, Ctor, s"(L$Object;)V", false)
          mv.visitInsn(ARETURN)
          mv.visitMaxs(0, 0)
          mv.visitEnd()
        }
      }
    }
    {
      val mv = cw.visitMethod(ACC_PUBLIC + ACC_STATIC, ASMGeneratorUtils.FactoryName, s"()L$Function;", null, null)
      mv.visitCode()
      mv.visitInvokeDynamicInsn(
        "apply",
        s"()L$Function;",
        mkBootstrapHandle,
        Seq(
          Type.getType(s"(L$Object;)L$Object;"),
          new Handle(Opcodes.H_INVOKESTATIC, keyClassName, "left", s"($majorBaseDesc)L$Node;", false),
          Type.getType(s"($majorBaseDesc)L$Node;")
        ): _*
      )
      mv.visitInsn(ARETURN)
      mv.visitMaxs(0, 0)
      mv.visitEnd()
    }
  }

  private def defineRight(cw: ClassWriter): Unit = {
    {
      val mv = cw.visitMethod(ACC_PRIVATE + ACC_STATIC, "right", s"($majorBaseDesc)L$keyClassName;", null, null)
      mv.visitCode()
      // "new KeyClassImpl(x)"
      mv.visitTypeInsn(NEW, keyClassName)
      mv.visitInsn(DUP)
      mv.visitVarInsn(ALOAD, 0)
      mv.visitMethodInsn(INVOKESPECIAL, keyClassName, Ctor, s"($majorBaseDesc)V", false)
      mv.visitInsn(ARETURN)
      mv.visitMaxs(0, 0)
      mv.visitEnd()
    }
    {
      val mv = cw.visitMethod(ACC_PUBLIC + ACC_STATIC, ASMGeneratorUtils.FactoryName, s"()L$Function;", null, null)
      mv.visitCode()
      mv.visitInvokeDynamicInsn(
        "apply",
        s"()L$Function;",
        mkBootstrapHandle,
        Seq(
          Type.getType(s"(L$Object;)L$Object;"),
          new Handle(Opcodes.H_INVOKESTATIC, keyClassName, "right", s"($majorBaseDesc)L$keyClassName;", false),
          Type.getType(s"($majorBaseDesc)L$keyClassName;")
        ): _*
      )
      mv.visitInsn(ARETURN)
      mv.visitMaxs(0, 0)
      mv.visitEnd()
    }
  }

  private def fieldDesc(name: String): String = {
    fieldDescLookup(name)
  }
}

object ASMKeyGenerator {
  val log = scalalog.getLogger[ASMKeyGenerator.type]

  def classNameFor(shapeType: TypeInfo[_], fields: Seq[String]): String = {
    val sb = new StringBuffer()
    sb.append(shapeType.typeString)
    if (shapeType.primaryConstructorParams.nonEmpty) {
      sb.append('(')
      sb.append(
        shapeType.primaryConstructorParams.map { case (name, clazz) => s"$name:${clazz.getName}" }.mkString(","))
      sb.append(')')
    }
    if (fields.nonEmpty) {
      sb.append("{{")
      sb.append(fields.mkString(","))
      sb.append("}}")
    }
    val code = sb.toString
    val digest = MessageDigest.getInstance("SHA-1").digest(code.getBytes)
    val name = s"key$$sha${new BigInteger(1, digest).toString(16)}"
    if (Settings.warnOnDemandCompiles)
      log.warn(s"Generate key class: $name -> $code")
    name
  }
}
