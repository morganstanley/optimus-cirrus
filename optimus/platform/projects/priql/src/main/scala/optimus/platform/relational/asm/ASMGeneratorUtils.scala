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

import com.google.common.primitives.Primitives
import org.objectweb.asm.Opcodes._
import org.objectweb.asm._
import optimus.platform.DynamicObject
import optimus.platform.relational.RelationalUnsupportedException
import optimus.platform.relational.tree.TypeInfo

trait ASMGeneratorUtils { this: ASMConstants =>

  protected def descFrom(t: TypeInfo[_]): String = {
    if (t.classes.isEmpty)
      s"L$Object;"
    else Type.getDescriptor(t.clazz)
  }

  private def descForConv(t: TypeInfo[_]): String = {
    descFrom(t) match {
      case "B" | "S" | "C" => "I"
      case x               => x
    }
  }

  protected def autoBoxing(mv: MethodVisitor, shapeType: TypeInfo[_]): Unit = {
    val desc = descFrom(shapeType)
    if (isPrimitive(desc)) {
      val name = boxToName(desc)
      mv.visitMethodInsn(INVOKESTATIC, name, "valueOf", s"($desc)L$name;", false)
    }
  }

  protected def convertPrimitive(mv: MethodVisitor, fromDesc: String, toDesc: String): Unit = {
    (fromDesc, toDesc) match {
      case ("I", "B") => mv.visitInsn(I2B)
      case ("I", "C") => mv.visitInsn(I2C)
      case ("I", "S") => mv.visitInsn(I2S)
      case ("I", "J") => mv.visitInsn(I2L)
      case ("I", "F") => mv.visitInsn(I2F)
      case ("I", "D") => mv.visitInsn(I2D)
      case ("J", "F") => mv.visitInsn(L2F)
      case ("J", "D") => mv.visitInsn(L2D)
      case ("J", "I") => mv.visitInsn(L2I)
      case ("F", "I") => mv.visitInsn(F2I)
      case ("F", "J") => mv.visitInsn(F2L)
      case ("F", "D") => mv.visitInsn(F2D)
      case ("D", "I") => mv.visitInsn(D2I)
      case ("D", "J") => mv.visitInsn(D2L)
      case ("D", "F") => mv.visitInsn(D2F)
      case (f, t)     => throw new IllegalArgumentException(s"don't know how to convert from type $f to type $f")
    }
  }

  protected def convert(mv: MethodVisitor, fromType: TypeInfo[_], toType: TypeInfo[_]): Unit = {
    val fromDesc = descForConv(fromType)
    val toDesc = descFrom(toType)
    if (fromDesc != toDesc) {
      val fromIsPrimitive = isPrimitive(fromDesc)
      val toIsPrimitive = isPrimitive(toDesc)
      if (fromIsPrimitive && toIsPrimitive) {
        convertPrimitive(mv, fromDesc, toDesc)
      } else if (fromIsPrimitive) {
        if (toType.classes.isEmpty) autoBoxing(mv, fromType)
        else {
          val toUnwrap = Primitives.unwrap(toType.clazz)
          if (toUnwrap.isPrimitive) {
            val toUnwrapType = TypeInfo.javaTypeInfo(toUnwrap)
            val toUnwrapDesc = descForConv(toUnwrapType)
            if (toUnwrapDesc != fromDesc)
              convertPrimitive(mv, fromDesc, toUnwrapDesc)
            autoBoxing(mv, toUnwrapType)
          } else autoBoxing(mv, fromType)
        }
      } else if (toType.classes.nonEmpty) {
        castTo(mv, toDesc, toType.clazz)
      }
    }
  }

  /**
   * Find the path dependent class, if it exist, it must has a public default constructor and cannot be abstract
   */
  protected def findPathDependentClass(concreteType: TypeInfo[_]): Option[Class[_]] = {
    concreteType.concreteClass.flatMap(baseClass => {
      val outerClass = baseClass.getDeclaringClass()
      if ((outerClass ne null) && !Modifier.isStatic(baseClass.getModifiers)) {
        if (Modifier.isAbstract(outerClass.getModifiers))
          throw new RelationalUnsupportedException(s"Owner class($outerClass) cannot be abstract!")

        outerClass.getConstructors.find(c => c.getParameterCount == 0 && Modifier.isPublic(c.getModifiers)) match {
          case Some(_) => Some(outerClass)
          case _ =>
            throw new RelationalUnsupportedException(s"Owner class($outerClass) has no public default constructor!")
        }
      } else None
    })
  }

  /**
   * Calculate the hash code of the value on the stack and mix it into var1=1
   */
  protected def hashTheValue(mv: MethodVisitor, typeDesc: String, seed: Int): Unit = {
    val l = typeDesc match {
      case "B" | "C" | "I" | "S" =>
        null
      case "Z" =>
        val l = new Label
        mv.visitJumpInsn(IFEQ, l)
        mv.visitIntInsn(BIPUSH, 41)
        l
      case "D" | "J" =>
        if (typeDesc == "D")
          mv.visitMethodInsn(INVOKESTATIC, Double, "doubleToLongBits", "(D)J", false)
        mv.visitInsn(DUP2)
        mv.visitIntInsn(BIPUSH, 32)
        mv.visitInsn(LUSHR)
        mv.visitInsn(LXOR)
        mv.visitInsn(L2I)
        null
      case "F" =>
        mv.visitMethodInsn(INVOKESTATIC, Float, "floatToIntBits", "(F)I", false)
        null
      case _ =>
        val l = new Label
        mv.visitVarInsn(ASTORE, 2)
        mv.visitVarInsn(ALOAD, 2)
        mv.visitJumpInsn(IFNULL, l)
        mv.visitVarInsn(ALOAD, 2)
        mv.visitMethodInsn(INVOKEVIRTUAL, Object, "hashCode", "()I", false)
        l
    }
    mv.visitVarInsn(ILOAD, 1)
    mv.visitIntInsn(BIPUSH, seed)
    mv.visitInsn(IMUL)
    mv.visitInsn(IADD)
    mv.visitVarInsn(ISTORE, 1)
    if (l ne null)
      mv.visitLabel(l)
  }

  /**
   * Common part of equals method
   */
  protected def equalsCommon(mv: MethodVisitor, ownerClass: String): Unit = {
    mv.visitVarInsn(ALOAD, 0) // if (this == other) return true;
    mv.visitVarInsn(ALOAD, 1)
    val referenceNotEqual = new Label
    mv.visitJumpInsn(IF_ACMPNE, referenceNotEqual)
    mv.visitInsn(ICONST_1)
    mv.visitInsn(IRETURN)
    mv.visitLabel(referenceNotEqual)

    mv.visitVarInsn(ALOAD, 1) // if (other == null) return false;
    val nonNull = new Label()
    mv.visitJumpInsn(IFNONNULL, nonNull)
    mv.visitInsn(ICONST_0)
    mv.visitInsn(IRETURN)
    mv.visitLabel(nonNull)

    mv.visitVarInsn(ALOAD, 1) // if (other.getClass() != this.getClass()) return false;
    mv.visitMethodInsn(INVOKEVIRTUAL, Object, "getClass", s"()L$Class;", false)
    mv.visitVarInsn(ALOAD, 0)
    mv.visitMethodInsn(INVOKEVIRTUAL, Object, "getClass", s"()L$Class;", false)
    val classEqual = new Label()
    mv.visitJumpInsn(IF_ACMPEQ, classEqual)
    mv.visitInsn(ICONST_0)
    mv.visitInsn(IRETURN)
    mv.visitLabel(classEqual)

    mv.visitVarInsn(ALOAD, 1) // ImplClass x = (ImplClass)other;
    mv.visitTypeInsn(CHECKCAST, ownerClass)
    mv.visitVarInsn(ASTORE, 2)
  }

  /**
   * Equality compare of 2 values on the stack, use var1, var2 for temp ref value store
   */
  protected def areEqual(mv: MethodVisitor, typeDesc: String, var1: Int, var2: Int): Unit = {
    val l = new Label
    typeDesc match {
      case "B" | "C" | "I" | "S" | "Z" =>
        mv.visitJumpInsn(IF_ICMPEQ, l)
      case "D" => // double
        mv.visitInsn(DCMPL)
        mv.visitJumpInsn(IFEQ, l)
      case "J" => // long
        mv.visitInsn(LCMP)
        mv.visitJumpInsn(IFEQ, l)
      case "F" =>
        mv.visitInsn(FCMPL)
        mv.visitJumpInsn(IFEQ, l)
      case _ => // v1 == v2 || v1 != null && v1.equals(v2)
        mv.visitVarInsn(ASTORE, var1)
        mv.visitVarInsn(ASTORE, var2)

        mv.visitVarInsn(ALOAD, var2)
        val field1IsNull = new Label
        mv.visitJumpInsn(IFNULL, field1IsNull)

        mv.visitVarInsn(ALOAD, var2)
        mv.visitVarInsn(ALOAD, var1)
        mv.visitMethodInsn(INVOKEVIRTUAL, Object, "equals", s"(L$Object;)Z", false)
        mv.visitJumpInsn(IFNE, l)
        mv.visitInsn(ICONST_0)
        mv.visitInsn(IRETURN)
        mv.visitLabel(field1IsNull) // if field1 == null

        mv.visitVarInsn(ALOAD, var1)
        mv.visitJumpInsn(IFNULL, l)
    }
    mv.visitInsn(ICONST_0)
    mv.visitInsn(IRETURN)
    mv.visitLabel(l)
  }

  /**
   * Invoke field method from instance object
   *
   * @param mv
   *   the method visitor
   * @param field
   *   field method
   * @param instName
   *   instance type name
   * @param instType
   *   instance full TypeInfo
   */
  protected def invokeField(mv: MethodVisitor, field: Method, instName: String, instType: TypeInfo[_]): Unit = {
    val ownerClass = field.getDeclaringClass
    val owner = if (ownerClass == instType.clazz) instName else Type.getInternalName(ownerClass)
    if (!ownerClass.isAssignableFrom(instType.clazz))
      mv.visitTypeInsn(CHECKCAST, owner)
    if (ownerClass.isInterface)
      mv.visitMethodInsn(INVOKEINTERFACE, owner, field.getName, Type.getMethodDescriptor(field), true)
    else
      mv.visitMethodInsn(INVOKEVIRTUAL, owner, field.getName, Type.getMethodDescriptor(field), false)
  }

  protected def invokeMethod(mv: MethodVisitor, m: Method): Unit = {
    val owner = Type.getInternalName(m.getDeclaringClass)
    val isInterface = m.getDeclaringClass.isInterface
    if (Modifier.isStatic(m.getModifiers))
      mv.visitMethodInsn(INVOKESTATIC, owner, m.getName, Type.getMethodDescriptor(m), isInterface)
    else if (isInterface)
      mv.visitMethodInsn(INVOKEINTERFACE, owner, m.getName, Type.getMethodDescriptor(m), true)
    else
      mv.visitMethodInsn(INVOKEVIRTUAL, owner, m.getName, Type.getMethodDescriptor(m), false)
  }

  /**
   * Invoke async field from instance object
   *
   * @param mv
   *   the method visitor
   * @param field
   *   field name
   * @param fieldDesc
   *   field type desc
   * @param instName
   *   instance type name (as a field in ownerClass)
   * @param instType
   *   instance full TypeInfo
   */
  protected def invokeAsyncField(
      mv: MethodVisitor,
      field: String,
      fieldDesc: String,
      instName: String,
      instType: TypeInfo[_]): Unit = {
    val method = instType.nonStructuralProperties(field)
    val ownerClass = method.getDeclaringClass
    val method$queued = ownerClass.getMethod(s"$field$$queued")
    val owner = if (ownerClass == instType.clazz) instName else Type.getInternalName(ownerClass)
    if (!ownerClass.isAssignableFrom(instType.clazz))
      mv.visitTypeInsn(CHECKCAST, owner)
    if (ownerClass.isInterface)
      mv.visitMethodInsn(INVOKEINTERFACE, owner, method$queued.getName, Type.getMethodDescriptor(method$queued), true)
    else
      mv.visitMethodInsn(INVOKEVIRTUAL, owner, method$queued.getName, Type.getMethodDescriptor(method$queued), false)
  }

  /**
   * Read pure structural fields using reflection
   *
   * @param mv
   *   the method visitor
   * @param ownerClass
   *   the internal name of generated class
   * @param field
   *   field name
   * @param fieldDesc
   *   field type desc
   * @param inst
   *   instance field name (as a field in ownerClass)
   * @param instDesc
   *   instance type desc
   * @param instType
   *   instance full TypeInfo
   */
  protected def readFieldUsingReflection(
      mv: MethodVisitor,
      ownerClass: String,
      field: String,
      fieldDesc: String,
      inst: String,
      instDesc: String,
      instType: TypeInfo[_]): Unit = {
    val l0 = new Label()
    val l1 = new Label()
    val l2 = new Label()
    mv.visitTryCatchBlock(l0, l1, l2, Exception)
    mv.visitLabel(l0)
    mv.visitVarInsn(ALOAD, 0)
    mv.visitFieldInsn(GETFIELD, ownerClass, inst, instDesc)
    mv.visitMethodInsn(INVOKEVIRTUAL, Object, "getClass", s"()L$Class;", false)
    mv.visitLdcInsn(field)
    mv.visitInsn(ICONST_0)
    mv.visitTypeInsn(ANEWARRAY, Class)
    mv.visitMethodInsn(INVOKEVIRTUAL, Class, "getMethod", s"(L$String;[L$Class;)L$Method;", false)
    mv.visitVarInsn(ALOAD, 0)
    mv.visitFieldInsn(GETFIELD, ownerClass, inst, instDesc)
    mv.visitInsn(ICONST_0)
    mv.visitTypeInsn(ANEWARRAY, Object)
    mv.visitMethodInsn(INVOKEVIRTUAL, Method, "invoke", s"(L$Object;[L$Object;)L$Object;", false)
    castTo(mv, fieldDesc, instType.propertyMap(field).clazz)
    mv.visitLabel(l1)
    returnFromMethod(mv, fieldDesc)
    mv.visitLabel(l2)
    mv.visitVarInsn(ASTORE, 1)
    mv.visitTypeInsn(NEW, RelationalException)
    mv.visitInsn(DUP)
    mv.visitVarInsn(ALOAD, 1)
    mv.visitMethodInsn(INVOKEVIRTUAL, Exception, "getMessage", s"()L$String;", false)
    mv.visitVarInsn(ALOAD, 1)
    mv.visitMethodInsn(INVOKESPECIAL, RelationalException, Ctor, s"(L$String;Ljava/lang/Throwable;)V", false)
    mv.visitInsn(ATHROW)
  }

  /**
   * Generate instructions to cast to 'desc' & 'targetType'
   */
  protected def castTo(mv: MethodVisitor, desc: String, targetType: Class[_]): Unit = {
    if (!targetType.isPrimitive) {
      mv.visitTypeInsn(CHECKCAST, Type.getInternalName(targetType))
    } else {
      val name = targetType.getName
      val boxedName = boxToName(desc)
      mv.visitTypeInsn(CHECKCAST, boxedName)
      mv.visitMethodInsn(INVOKEVIRTUAL, boxedName, s"${name}Value", s"()$desc", false)
    }
  }

  /**
   * make field name to field desc lookup
   */
  protected def mkFieldDescLookup(shapeType: TypeInfo[_], fields: Seq[String]): Map[String, String] = {
    fields.iterator
      .map(fn => {
        val fieldClass = shapeType.propertyMap.get(fn).map(_.clazz).getOrElse(ObjectClass)
        (fn, Type.getDescriptor(fieldClass))
      })
      .toMap
  }

  /**
   * Partition the fields into async and sync collections
   */
  protected def partitionFields(shapeType: TypeInfo[_], fields: Seq[String]): (Seq[String], Seq[String]) = {
    val allMethods: Set[String] =
      shapeType.classes.iterator.flatMap(_.getMethods.toList.filter(_.getParameterCount == 0).map(_.getName)).toSet
    fields.partition(f => allMethods.contains(s"$f$$queued"))
  }

  /**
   * Retrieve the major base class internal name from a TypeInfo[_]
   */
  protected def majorBaseFrom(shapeType: TypeInfo[_]): String = {
    if (shapeType.classes.isEmpty)
      Object
    else if (shapeType.clazz.isPrimitive)
      boxToName(Type.getDescriptor(shapeType.clazz))
    else
      Type.getInternalName(shapeType.clazz)
  }

  /**
   * primitive type desc to boxed type internal name
   */
  protected def boxToName(desc: String): String = {
    desc match {
      case "D" => Double
      case "F" => Float
      case "I" => Integer
      case "Z" => "java/lang/Boolean"
      case "J" => "java/lang/Long"
      case "B" => "java/lang/Byte"
      case "S" => "java/lang/Short"
      case "C" => "java/lang/Character"
    }
  }

  protected def boxToDesc(desc: String): String =
    if (isPrimitive(desc)) s"L${boxToName(desc)};" else desc

  /**
   * Check if a type desc is primitive
   */
  protected def isPrimitive(desc: String): Boolean = {
    desc match {
      case "B" | "C" | "I" | "S" | "Z" | "D" | "J" | "F" => true
      case _                                             => false
    }
  }

  protected def arrayType(itemDesc: String): Int = {
    itemDesc match {
      case "Z" => T_BOOLEAN
      case "B" => T_BYTE
      case "C" => T_CHAR
      case "S" => T_SHORT
      case "I" => T_INT
      case "D" => T_DOUBLE
      case "J" => T_LONG
      case "F" => T_FLOAT
    }
  }

  protected def storeIntoArray(mv: MethodVisitor, itemDesc: String): Unit = {
    itemDesc match {
      case "B" | "Z" =>
        mv.visitInsn(BASTORE)
      case "C" =>
        mv.visitInsn(CASTORE)
      case "S" =>
        mv.visitInsn(SASTORE)
      case "I" =>
        mv.visitInsn(IASTORE)
      case "D" => // double
        mv.visitInsn(DASTORE)
      case "J" => // long
        mv.visitInsn(LASTORE)
      case "F" => // float
        mv.visitInsn(FASTORE)
      case _ =>
        mv.visitInsn(AASTORE)
    }
  }

  /**
   * Generate return instruction based on the return type
   */
  protected def returnFromMethod(mv: MethodVisitor, returnTypeDesc: String): Unit = {
    returnTypeDesc match {
      case "B" | "C" | "I" | "S" | "Z" =>
        mv.visitInsn(IRETURN)
      case "D" => // double
        mv.visitInsn(DRETURN)
      case "J" => // long
        mv.visitInsn(LRETURN)
      case "F" => // float
        mv.visitInsn(FRETURN)
      case "V" =>
        mv.visitInsn(RETURN)
      case _ =>
        mv.visitInsn(ARETURN)
    }
  }

  /**
   * Generate load instruction based on the argument type, return the next argument index
   */
  protected def loadMethodArg(mv: MethodVisitor, index: Int, typeDesc: String): Int = {
    typeDesc match {
      case "B" | "C" | "I" | "S" | "Z" =>
        mv.visitVarInsn(ILOAD, index)
        index + 1
      case "D" => // double
        mv.visitVarInsn(DLOAD, index)
        index + 2
      case "J" => // long
        mv.visitVarInsn(LLOAD, index)
        index + 2
      case "F" =>
        mv.visitVarInsn(FLOAD, index)
        index + 1
      case _ =>
        mv.visitVarInsn(ALOAD, index)
        index + 1
    }
  }
}

object ASMGeneratorUtils {
  val FactoryName = "get$Factory"

  private[relational] def loadClass(name: String, byteCode: Array[Byte]): Class[_] = {
    loader.loadClass(name, byteCode)
  }

  private val loader = new Loader(classOf[ASMGeneratorUtils].getClassLoader)

  private[asm] class Loader(parent: ClassLoader) extends ClassLoader(parent) {
    def loadClass(name: String, byteCode: Array[Byte]): Class[_] = {
      getClassLoadingLock(name).synchronized {
        val c = findLoadedClass(name)
        if (c eq null) defineClass(name, byteCode, 0, byteCode.length) else c
      }
    }
  }

  /**
   * Please do not move or change the name of this method, used by ASMProxyGenerator
   */
  final def extractCtorParamMethods(clazz: Class[_], ctorParams: Array[String]): Array[Method] = {
    val methodMap: Map[String, Method] = clazz.getDeclaredMethods.iterator.map(m => (m.getName, m)).toMap
    ctorParams.iterator.map { p =>
      val method = methodMap.getOrElse(
        p,
        methodMap.getOrElse(
          s"$p$$4eq",
          throw new NoSuchMethodException(
            "Cannot create proxy for concrete non-entity type that has non-val constructor parameters."))
      )
      method.setAccessible(true)
      method
    }.toArray
  }

  /**
   * Please do not move or change the name of this method, used by ASMDynamicObjectGenerator
   */
  final def zipWithIndex(properties: Seq[String]): Map[String, Int] = {
    properties.iterator.zipWithIndex.toMap
  }

  /**
   * Please do not move or change the name of this method, used by ASMDynamicObjectGenerator
   */
  final def getAll(d: DynamicObject, properties: Map[String, Int]): Map[String, Any] = {
    properties.iterator.map(t => (t._1, d.get(t._1))).toMap
  }

  /**
   * Please do not move or change the name of this method, used by ASMDynamicObjectGenerator
   */
  final val lazyM1 = () => -1
}
