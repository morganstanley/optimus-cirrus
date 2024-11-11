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
import optimus.platform.relational.tree.TypeInfo
import optimus.scalacompat.isAtLeastScala2_13

/**
 * Generate DynamicObject using a proxy.
 *
 * Suppose we have trait S
 * {{{
 * trait S {
 *   @node def node1: T1
 *   @node def node2: T2
 *   def prop1: T3
 *   def prop2: T4
 *   @ProxyFinalModifier final def prop3: T5 = ...
 * }
 * }}}
 *
 * The generated class looks like following:
 * {{{
 * public class DynamicGen extends AbstractDynamicObject {
 *   private final inst: S
 *   private final node1: T1
 *   private final node2: T2
 *   public DynamicGen(s: S, n1: T1, n2: T2) {
 *     super();
 *     inst = s;
 *     node1 = n1;
 *     node2 = n2;
 *   }
 *
 *   private final T3 prop1() { return inst.prop1; }
 *   private final T4 prop2() { return inst.prop2; }
 *   // prop3 won't be generated according to the behavior of PreshapedDynamicObject
 *
 *   private static final s_propNames = ... // In Scala: immutable.Map("node1" -> 0, "node2" -> 1, "prop1" -> 2, "prop2" -> 3)
 *
 *   public final boolean contains(p: String) {
 *     return s_propNames.contains(p);
 *   }
 *   public final Map[String, Any] getAll() {
 *     ...
 *     // equivalent java code as "s_propNames.map{ case (n, _) => n -> get(n) }
 *   }
 *   public final Object get(p: String) {
 *     int idx = s_propNames.getOrElse(p, () -> -1)
 *     switch(idx) {  // table-switch with O(1) performance
 *       case 0: return this.node1;
 *       case 1: return this.node2;
 *       case 2: return prop1();
 *       case 3: return prop2();
 *       default: throw new RelationalException(...)
 *     }
 *   }
 *   public static Node<DynamicObject> apply(Object o) {
 *     S s = (S)o;
 *     Node<?>[] arr = new Node<?>[2]
 *     arr[0] = s.node1$queued
 *     arr[1] = s.node2$queued
 *     Continuation.whenAll(arr, res -> new DynamicGen(s, res[0], res[1]))
 *   }
 *
 *   public static java.util.Function<Object, DynamicObject> factory() {
 *     // invokedynamic to apply method
 *   }
 * }
 * }}}
 */
final class ASMDynamicObjectGenerator(shapeType: TypeInfo[_]) extends ASMGeneratorUtils with ASMConstants {
  val (asyncFields, otherFields) = partitionFields(shapeType, shapeType.propertyNames)
  val asyncFieldsLookup = asyncFields.toSet
  val className = ASMDynamicObjectGenerator.classNameFor(shapeType)
  val majorBase = majorBaseFrom(shapeType)
  val majorBaseDesc = s"L$majorBase;"
  val fieldDescLookup: Map[String, String] = mkFieldDescLookup(shapeType, shapeType.propertyNames)
  val ctorDesc = {
    val paramList = asyncFields.map(fn => fieldDesc(fn)).mkString("")
    s"($majorBaseDesc$paramList)V"
  }

  def generateClass(): Array[Byte] = {
    val cw = new ClassWriter(ClassWriter.COMPUTE_FRAMES)
    val interfaces = Array(ProxyMarkerTrait, DynamicObject)
    cw.visit(ClassfileVersion, ACC_PUBLIC + ACC_SUPER, className, null, AbstractDynamicObject, interfaces)

    // static fields
    defineStaticField(cw)
    // fields
    defineFields(cw)
    // constructor
    defineConstructor(cw)
    // getters for otherFields
    defineGetters(cw)
    // generate "get", "getAll", "contains"
    defineDynamicObjectMethods(cw)

    defineFactory(cw)

    cw.visitEnd()
    cw.toByteArray()
  }

  private def defineStaticField(cw: ClassWriter): Unit = {
    // declare static variable (properties: Set[String])
    cw.visitField(ACC_PRIVATE + ACC_STATIC + ACC_FINAL, ConstsInst, s"L$ImmutableMap;", null, null).visitEnd()

    // static block
    val mv = cw.visitMethod(ACC_STATIC, "<clinit>", "()V", null, null)
    mv.visitCode()

    mv.visitIntInsn(SIPUSH, shapeType.propertyNames.size)
    mv.visitTypeInsn(ANEWARRAY, String)
    shapeType.propertyNames.zipWithIndex.foreach { case (n, i) =>
      mv.visitInsn(DUP)
      mv.visitIntInsn(SIPUSH, i)
      mv.visitLdcInsn(n)
      mv.visitInsn(AASTORE)
    }
    mv.visitMethodInsn(
      INVOKESTATIC,
      Predef,
      "wrapRefArray",
      if (isAtLeastScala2_13) s"([L$Object;)L$ArraySeq$$ofRef;" else s"([L$Object;)L$WrappedArray;",
      false
    ) // end Predef.wrapRefArray
    mv.visitMethodInsn(INVOKESTATIC, ASMGeneratorUtilsCls, "zipWithIndex", s"(L$Sequence;)L$ImmutableMap;", true)

    // set into static variable
    mv.visitFieldInsn(PUTSTATIC, className, ConstsInst, s"L$ImmutableMap;")

    mv.visitInsn(RETURN)
    mv.visitMaxs(0, 0)
    mv.visitEnd()
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
    mv.visitMethodInsn(INVOKESPECIAL, AbstractDynamicObject, Ctor, s"()V", false)

    if (otherFields.nonEmpty) {
      mv.visitVarInsn(ALOAD, 0)
      mv.visitVarInsn(ALOAD, 1)
      mv.visitFieldInsn(PUTFIELD, className, FieldInst, majorBaseDesc)
    }

    var index = 2
    asyncFields.foreach { fn =>
      mv.visitVarInsn(ALOAD, 0)
      val desc = fieldDesc(fn)
      index = loadMethodArg(mv, index, desc)
      mv.visitFieldInsn(PUTFIELD, className, fn, desc)
    }
    mv.visitInsn(RETURN)
    mv.visitMaxs(0, 0)
    mv.visitEnd()
  }

  private def defineGetters(cw: ClassWriter): Unit = {
    otherFields.foreach { fn =>
      val desc = fieldDesc(fn)
      val mv = cw.visitMethod(ACC_PRIVATE + ACC_FINAL, fn, s"()$desc", null, null)
      mv.visitCode()

      shapeType.nonStructuralProperties.get(fn) match {
        case Some(m) =>
          mv.visitVarInsn(ALOAD, 0)
          mv.visitFieldInsn(GETFIELD, className, FieldInst, majorBaseDesc)
          invokeField(mv, m, majorBase, shapeType)
          returnFromMethod(mv, desc)
          mv.visitMaxs(1, 1)
          mv.visitEnd()

        case _ => // pure structural method
          // use reflection to access the field
          readFieldUsingReflection(mv, className, fn, desc, FieldInst, majorBaseDesc, shapeType)
          mv.visitMaxs(0, 0)
          mv.visitEnd()
      }
    }
  }

  private def defineDynamicObjectMethods(cw: ClassWriter): Unit = {
    {
      // "contains"
      val mv = cw.visitMethod(ACC_PUBLIC + ACC_FINAL, "contains", s"(L$String;)Z", null, null)
      mv.visitCode()
      mv.visitFieldInsn(GETSTATIC, className, ConstsInst, s"L$ImmutableMap;")
      mv.visitVarInsn(ALOAD, 1)
      mv.visitMethodInsn(INVOKEINTERFACE, ImmutableMap, "contains", s"(L$Object;)Z", true)
      mv.visitInsn(IRETURN)
      mv.visitMaxs(0, 0)
      mv.visitEnd()
    }

    {
      // "getAll"
      val mv = cw.visitMethod(ACC_PUBLIC + ACC_FINAL, "getAll", s"()L$ImmutableMap;", null, null)
      mv.visitCode()
      mv.visitVarInsn(ALOAD, 0)
      mv.visitFieldInsn(GETSTATIC, className, ConstsInst, s"L$ImmutableMap;")
      mv.visitMethodInsn(
        INVOKESTATIC,
        ASMGeneratorUtilsCls,
        "getAll",
        s"(L$DynamicObject;L$ImmutableMap;)L$ImmutableMap;",
        true)
      mv.visitInsn(ARETURN)
      mv.visitMaxs(0, 0)
      mv.visitEnd()
    }

    {
      // "get"
      val mv = cw.visitMethod(ACC_PUBLIC, "get", s"(L$String;)L$Object;", null, null)
      mv.visitCode()
      mv.visitFieldInsn(GETSTATIC, className, ConstsInst, s"L$ImmutableMap;")
      mv.visitVarInsn(ALOAD, 1)
      mv.visitMethodInsn(INVOKESTATIC, ASMGeneratorUtilsCls, "lazyM1", s"()L$ScalaFunction0;", true)
      mv.visitMethodInsn(INVOKEINTERFACE, ImmutableMap, "getOrElse", s"(L$Object;L$ScalaFunction0;)L$Object;", true)
      castTo(mv, "I", classOf[Int])

      val labels = shapeType.propertyNames.map(n => new Label)
      val defaultLabel = new Label

      mv.visitTableSwitchInsn(0, labels.size - 1, defaultLabel, labels: _*)
      shapeType.propertyNames.zip(labels).foreach { case (fn, l) =>
        mv.visitLabel(l)
        mv.visitVarInsn(ALOAD, 0)
        val desc = fieldDesc(fn)
        if (asyncFieldsLookup.contains(fn))
          mv.visitFieldInsn(GETFIELD, className, fn, desc)
        else
          mv.visitMethodInsn(INVOKESPECIAL, className, fn, s"()$desc", false)
        if (isPrimitive(desc)) {
          val name = boxToName(desc)
          mv.visitMethodInsn(INVOKESTATIC, name, "valueOf", s"($desc)L$name;", false)
        }
        mv.visitInsn(ARETURN)
      }
      mv.visitLabel(defaultLabel)
      mv.visitTypeInsn(NEW, RelationalException)
      mv.visitInsn(DUP)

      // generate the message for RelationalException
      mv.visitTypeInsn(NEW, StringBuilder)
      mv.visitInsn(DUP)
      mv.visitMethodInsn(INVOKESPECIAL, StringBuilder, Ctor, "()V", false)
      mv.visitLdcInsn("Do not have property: ")
      mv.visitMethodInsn(INVOKEVIRTUAL, StringBuilder, "append", s"(L$Object;)L$StringBuilder;", false)
      mv.visitVarInsn(ALOAD, 1)
      mv.visitMethodInsn(INVOKEVIRTUAL, StringBuilder, "append", s"(L$Object;)L$StringBuilder;", false)
      mv.visitMethodInsn(INVOKEVIRTUAL, StringBuilder, "toString", s"()L$String;", false)

      mv.visitMethodInsn(INVOKESPECIAL, RelationalException, Ctor, s"(L$String;)V", false)
      mv.visitInsn(ATHROW)
      mv.visitMaxs(0, 0)
      mv.visitEnd()
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
        s"($majorBaseDesc)L$Node<L$className;>;",
        null)
      mv.visitCode()
      mv.visitFieldInsn(GETSTATIC, ContinuationObject, "MODULE$", s"L$ContinuationObject;")
      if (asyncFields.size == 1) {
        val fn = asyncFields.head
        val fd = fieldDesc(fn)
        val requireBoxing = isPrimitive(fd)
        val fDesc = boxToDesc(fd)
        mv.visitVarInsn(ALOAD, 0)
        invokeAsyncField(mv, fn, fd, majorBase, shapeType)

        mv.visitVarInsn(ALOAD, 0)
        mv.visitInvokeDynamicInsn(
          "apply",
          s"($majorBaseDesc)L$Function;",
          mkBootstrapHandle,
          Seq(
            Type.getType(s"(L$Object;)L$Object;"),
            new Handle(Opcodes.H_INVOKESTATIC, className, "lambda$left$0", s"($majorBaseDesc$fDesc)L$Node;", false),
            Type.getType(s"($fDesc)L$Node;")
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
          mv.visitTypeInsn(NEW, className)
          mv.visitInsn(DUP)
          mv.visitVarInsn(ALOAD, 0)
          loadMethodArg(mv, 1, fDesc)
          if (requireBoxing) {
            val name = Type.getType(fd).getClassName
            mv.visitMethodInsn(INVOKEVIRTUAL, boxToName(fd), s"${name}Value", s"()$fd", false)
          }
          mv.visitMethodInsn(INVOKESPECIAL, className, Ctor, ctorDesc, false)
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
              className,
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
          mv.visitTypeInsn(NEW, className)
          mv.visitInsn(DUP)
          mv.visitVarInsn(ALOAD, 0)
          asyncFields.zipWithIndex.foreach { case (fn, idx) =>
            mv.visitVarInsn(ALOAD, 1) // Object[]
            mv.visitIntInsn(BIPUSH, idx)
            mv.visitInsn(AALOAD)
            castTo(mv, fieldDesc(fn), shapeType.propertyMap(fn).clazz)
          }
          mv.visitMethodInsn(INVOKESPECIAL, className, Ctor, ctorDesc, false)
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
          new Handle(Opcodes.H_INVOKESTATIC, className, "left", s"($majorBaseDesc)L$Node;", false),
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
      val mv = cw.visitMethod(ACC_PRIVATE + ACC_STATIC, "right", s"($majorBaseDesc)L$className;", null, null)
      mv.visitCode()
      mv.visitTypeInsn(NEW, className)
      mv.visitInsn(DUP)
      mv.visitVarInsn(ALOAD, 0)
      mv.visitMethodInsn(INVOKESPECIAL, className, Ctor, ctorDesc, false)
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
          new Handle(Opcodes.H_INVOKESTATIC, className, "right", s"($majorBaseDesc)L$className;", false),
          Type.getType(s"($majorBaseDesc)L$className;")
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

object ASMDynamicObjectGenerator {
  val log = scalalog.getLogger[ASMKeyGenerator.type]

  def classNameFor(shapeType: TypeInfo[_]): String = {
    val sb = new StringBuffer()
    sb.append(shapeType.typeString)
    if (shapeType.primaryConstructorParams.nonEmpty) {
      sb.append('(')
      sb.append(
        shapeType.primaryConstructorParams.map { case (name, clazz) => s"$name:${clazz.getName}" }.mkString(","))
      sb.append(')')
    }
    val code = sb.toString
    val digest = MessageDigest.getInstance("SHA-1").digest(code.getBytes)
    val name = s"dyn$$sha${new BigInteger(1, digest).toString(16)}"
    if (Settings.warnOnDemandCompiles)
      log.warn(s"Generate DynamicObject proxy class: $name -> $code")
    name
  }
}
