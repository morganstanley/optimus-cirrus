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
package optimus;

import static optimus.debug.InstrumentationConfig.OBJECT_CLS_NAME;

import java.lang.instrument.ClassFileTransformer;
import java.lang.instrument.IllegalClassFormatException;
import java.security.ProtectionDomain;

import org.objectweb.asm.ClassReader;
import org.objectweb.asm.ClassVisitor;
import org.objectweb.asm.ClassWriter;
import org.objectweb.asm.MethodVisitor;
import org.objectweb.asm.Opcodes;
import org.objectweb.asm.Type;

/**
 * To check how the code should looks like in ASM format, we can use: org.objectweb.asm.util.ASMifier
 * to analyze some class file, and it can tell how the ASM code look like
 */
public class CollectionInjector implements ClassFileTransformer {

  @Override
  public byte[] transform(ClassLoader loader, String className, Class<?> classBeingRedefined,
      ProtectionDomain protectionDomain, byte[] classfileBuffer) throws IllegalClassFormatException {

    ClassReader source = new ClassReader(classfileBuffer);

    ClassWriter cw = new ClassWriter(source, ClassWriter.COMPUTE_MAXS);
    ClassVisitor cv;

    if (className.startsWith("scala/collection/")) {
      // inject code for the collection classes
      cv = new CollectionClassAdapter(className, cw);
    } else {
      // inject code before invoke the collection methods
      cv = new NoneCollectionClassAdapter(className, cw);
    }

    source.accept(cv, 0);

    return cw.toByteArray();
  }
}

// Used to inject trace methods inside collection methods
class CollectionClassAdapter extends ClassVisitor implements Opcodes {
  private String cname;

  CollectionClassAdapter(String cname, final ClassVisitor cv) {
    super(ASM9, cv);
    this.cname = cname;
  }

  @Override
  public MethodVisitor visitMethod(final int access, final String name,
      final String desc, final String signature, final String[] exceptions) {

    MethodVisitor mv = super.visitMethod(access, name, desc, signature, exceptions);

    // If constructor, we handle it differently
    if ("<init>".equals(name)) {
      return mv == null ? null : new CollectionCtorVisitor(cname, name, desc, mv);
    }

    return mv == null ? null : new CollectionMethodVisitor(cname, name, access, desc, mv);
  }
}

class CollectionMethodVisitor extends MethodVisitor implements Opcodes {
  private int mAccess;
  private String mDesc;
  private String mName;
  private String cName;
  CollectionMethodVisitor(String cname, String mname, int access, String desc, MethodVisitor mv) {
    super(ASM9, mv);
    cName = cname;
    mName = mname;
    mAccess = access;
    mDesc = desc;
  }

  @Override
  public void visitCode() {
    if ((mAccess & ACC_STATIC) != 0) {
      CollectionInjectHelper.traceStaticInvokedWithParam(mv, cName, mName, mDesc);
    } else {
      CollectionInjectHelper.traceInvokedWithParam(mv, cName, mName, mDesc);
    }
    super.visitCode();
  }
}

class CollectionCtorVisitor extends MethodVisitor implements Opcodes {
  private String mDesc;
  private String mName;
  private String cName;
  CollectionCtorVisitor(String cname, String mname, String desc, MethodVisitor mv) {
    super(ASM9, mv);
    cName = cname;
    mName = mname;
    mDesc = desc;
  }

  @Override
  public void visitInsn(int opcode) {
    // we will add the injected code inside ctor before return
    if (opcode == RETURN) {
      CollectionInjectHelper.traceCtorInvokedWithParam(mv, cName, mName, mDesc);
    }
    super.visitInsn(opcode);
  }
}

// Used to inject trace method before we call collection library
class NoneCollectionClassAdapter extends ClassVisitor implements Opcodes {
  private String cname;

  NoneCollectionClassAdapter(String cname, final ClassVisitor cv) {
    super(ASM9, cv);
    this.cname = cname;
  }

  @Override
  public MethodVisitor visitMethod(final int access, final String name,
      final String desc, final String signature, final String[] exceptions) {

    MethodVisitor mv = super.visitMethod(access, name, desc, signature, exceptions);

    return mv == null ? null : new CollectionInvokerVisitor(cname, name, mv);
  }
}

class CollectionInvokerVisitor extends MethodVisitor implements Opcodes {
  private String mName;
  private String cName;
  CollectionInvokerVisitor(String cname, String mname, MethodVisitor mv) {
    super(ASM9, mv);
    cName = cname;
    mName = mname;
  }

  @Override
  public void visitMethodInsn(int opcode, String owner, String name, String desc, boolean itf) {
    if (mv != null && owner.startsWith("scala/collection/")) {
      CollectionInjectHelper.traceUserInvoked(mv, cName, mName, owner, name, desc);
    }
    super.visitMethodInsn(opcode, owner, name, desc, itf);
  }
}

/**
 * Helper method which define the ASM injection invoke to the Support methods (hooks)
 */
class CollectionInjectHelper implements Opcodes {
  private static void traceCtorInvoked(MethodVisitor mv, String colCls, String method, String desc) {
    mv.visitVarInsn(ALOAD, 0);
    mv.visitLdcInsn(colCls);
    mv.visitLdcInsn(method);
    mv.visitLdcInsn(desc);
    mv.visitMethodInsn(INVOKESTATIC, "optimus/graph/CollectionTraceSupport", "traceCtorInvoked",
        "(Ljava/lang/Object;Ljava/lang/String;Ljava/lang/String;Ljava/lang/String;)V", false);
  }

  static void traceCtorInvokedWithParam(MethodVisitor mv, String colCls, String method, String desc) {
    Type[] paramTypes = Type.getArgumentTypes(desc);
    if (paramTypes.length > 0) {
      int index = genParam(mv, paramTypes, false);
      mv.visitVarInsn(ALOAD, 0);
      mv.visitLdcInsn(colCls);
      mv.visitLdcInsn(method);
      mv.visitLdcInsn(desc);
      mv.visitVarInsn(ALOAD, index);
      mv.visitMethodInsn(INVOKESTATIC, "optimus/graph/CollectionTraceSupport", "traceCtorInvokedWithParam",
          "(Ljava/lang/Object;Ljava/lang/String;Ljava/lang/String;Ljava/lang/String;[Ljava/lang/Object;)V", false);
    } else {
      traceCtorInvoked(mv, colCls, method, desc);
    }
  }

  private static void traceInvoked(MethodVisitor mv, String colCls, String method, String desc) {
    mv.visitVarInsn(ALOAD, 0);
    mv.visitLdcInsn(colCls);
    mv.visitLdcInsn(method);
    mv.visitLdcInsn(desc);
    mv.visitMethodInsn(INVOKESTATIC, "optimus/graph/CollectionTraceSupport", "traceInvoked",
        "(Ljava/lang/Object;Ljava/lang/String;Ljava/lang/String;Ljava/lang/String;)V", false);
  }

  static void traceInvokedWithParam(MethodVisitor mv, String colCls, String method, String desc) {
    Type[] paramTypes = Type.getArgumentTypes(desc);
    if (paramTypes.length > 0) {
      int index = genParam(mv, paramTypes, false);
      mv.visitVarInsn(ALOAD, 0);
      mv.visitLdcInsn(colCls);
      mv.visitLdcInsn(method);
      mv.visitLdcInsn(desc);
      mv.visitVarInsn(ALOAD, index);
      mv.visitMethodInsn(INVOKESTATIC, "optimus/graph/CollectionTraceSupport", "traceInvokedWithParam",
          "(Ljava/lang/Object;Ljava/lang/String;Ljava/lang/String;Ljava/lang/String;[Ljava/lang/Object;)V", false);
    } else {
      traceInvoked(mv, colCls, method, desc);
    }
  }

  private static void traceStaticInvoked(MethodVisitor mv, String colCls, String method, String desc) {
    mv.visitLdcInsn(colCls);
    mv.visitLdcInsn(method);
    mv.visitLdcInsn(desc);
    mv.visitMethodInsn(INVOKESTATIC, "optimus/graph/CollectionTraceSupport", "traceStaticInvoked",
        "(Ljava/lang/String;Ljava/lang/String;Ljava/lang/String;)V", false);
  }

  static void traceStaticInvokedWithParam(MethodVisitor mv, String colCls, String method, String desc) {
    Type[] paramTypes = Type.getArgumentTypes(desc);
    if (paramTypes.length > 0) {
      int index = genParam(mv, paramTypes, true);
      mv.visitLdcInsn(colCls);
      mv.visitLdcInsn(method);
      mv.visitLdcInsn(desc);
      mv.visitVarInsn(ALOAD, index);
      mv.visitMethodInsn(INVOKESTATIC, "optimus/graph/CollectionTraceSupport", "traceStaticInvokedWithParam",
          "(Ljava/lang/String;Ljava/lang/String;Ljava/lang/String;[Ljava/lang/Object;)V", false);
    } else {
      traceStaticInvoked(mv, colCls, method, desc);
    }
  }

  private static void visitConstNum(MethodVisitor mv, int num) {
    if (num < 5) {
      mv.visitInsn(ICONST_0 + num);
    } else {
      mv.visitIntInsn(BIPUSH, num);
    }
  }
  private static int genParam(MethodVisitor mv, Type[] paramTypes, boolean isStatic) {

    visitConstNum(mv, paramTypes.length);
    mv.visitTypeInsn(ANEWARRAY, OBJECT_CLS_NAME);

    int readIndex = isStatic ? 0 : 1;
    int arrayIndex = 0;
    for (Type tp : paramTypes) {
      mv.visitInsn(DUP);
      visitConstNum(mv, arrayIndex++);
      if (tp.equals(Type.BOOLEAN_TYPE)) {
        mv.visitVarInsn(ILOAD, readIndex);
        mv.visitMethodInsn(INVOKESTATIC, "java/lang/Boolean", "valueOf", "(Z)Ljava/lang/Boolean;", false);
      } else if (tp.equals(Type.BYTE_TYPE)) {
        mv.visitVarInsn(ILOAD, readIndex);
        mv.visitMethodInsn(INVOKESTATIC, "java/lang/Byte", "valueOf", "(B)Ljava/lang/Byte;", false);
      } else if (tp.equals(Type.CHAR_TYPE)) {
        mv.visitVarInsn(ILOAD, readIndex);
        mv.visitMethodInsn(INVOKESTATIC, "java/lang/Character", "valueOf", "(C)Ljava/lang/Character;", false);
      } else if (tp.equals(Type.SHORT_TYPE)) {
        mv.visitVarInsn(ILOAD, readIndex);
        mv.visitMethodInsn(INVOKESTATIC, "java/lang/Short", "valueOf", "(S)Ljava/lang/Short;", false);
      } else if (tp.equals(Type.INT_TYPE)) {
        mv.visitVarInsn(ILOAD, readIndex);
        mv.visitMethodInsn(INVOKESTATIC, "java/lang/Integer", "valueOf", "(I)Ljava/lang/Integer;", false);
      } else if (tp.equals(Type.LONG_TYPE)) {
        mv.visitVarInsn(LLOAD, readIndex);
        mv.visitMethodInsn(INVOKESTATIC, "java/lang/Long", "valueOf", "(J)Ljava/lang/Long;", false);
        ++readIndex;
      } else if (tp.equals(Type.FLOAT_TYPE)) {
        mv.visitVarInsn(FLOAD, readIndex);
        mv.visitMethodInsn(INVOKESTATIC, "java/lang/Float", "valueOf", "(F)Ljava/lang/Float;", false);
      } else if (tp.equals(Type.DOUBLE_TYPE)) {
        mv.visitVarInsn(DLOAD, readIndex);
        mv.visitMethodInsn(INVOKESTATIC, "java/lang/Double", "valueOf", "(D)Ljava/lang/Double;", false);
        ++readIndex;
      } else
        mv.visitVarInsn(ALOAD, readIndex);

      mv.visitInsn(AASTORE);
      ++readIndex;
    }

    mv.visitVarInsn(ASTORE, readIndex);

    return readIndex;
  }

  static void traceUserInvoked(MethodVisitor mv, String invoker, String callerMethod, String colCls, String method, String desc) {
    mv.visitLdcInsn(invoker);
    mv.visitLdcInsn(callerMethod);
    mv.visitLdcInsn(colCls);
    mv.visitLdcInsn(method);
    mv.visitLdcInsn(desc);
    mv.visitMethodInsn(INVOKESTATIC, "optimus/graph/CollectionTraceSupport", "traceUserInvoked",
        "(Ljava/lang/String;Ljava/lang/String;Ljava/lang/String;Ljava/lang/String;Ljava/lang/String;)V", false);
  }
}