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

import java.lang.instrument.ClassFileTransformer;
import java.lang.instrument.IllegalClassFormatException;
import java.security.ProtectionDomain;

import optimus.debug.CommonAdapter;
import org.objectweb.asm.ClassReader;
import org.objectweb.asm.ClassVisitor;
import org.objectweb.asm.ClassWriter;
import org.objectweb.asm.FieldVisitor;
import org.objectweb.asm.MethodVisitor;
import org.objectweb.asm.Opcodes;
import org.objectweb.asm.Type;

public class NodeMethodsInjector implements ClassFileTransformer {

  @Override
  public byte[] transform(
      ClassLoader loader,
      String className,
      Class<?> classBeingRedefined,
      ProtectionDomain protectionDomain,
      byte[] classfileBuffer)
      throws IllegalClassFormatException {

    ClassReader source = new ClassReader(classfileBuffer);
    String superName = source.getSuperName();

    if ("optimus/graph/CompletableNodeM".equals(superName)
        || (superName != null && superName.startsWith("optimus/graph/profiled/"))) {
      ClassReader crSource = new ClassReader(classfileBuffer);
      ClassWriter cw = new ClassWriter(crSource, 0);
      NodeMethodsInjectorAdapter cv = new NodeMethodsInjectorAdapter(className, cw);
      crSource.accept(cv, 0);
      return cw.toByteArray();
    }
    return null;
  }
}

class NodeMethodsInjectorAdapter extends ClassVisitor implements Opcodes {
  public static final String fieldName = "__profileId";
  public static final String fieldType = "I";
  static final String nodeTraceCls = "optimus/graph/NodeTrace";

  private boolean seenCCtor;
  private String className;

  NodeMethodsInjectorAdapter(String className, ClassVisitor cv) {
    super(ASM9, cv);
    this.className = className;
  }

  @Override
  public void visit(
      int version,
      int access,
      String name,
      String signature,
      String superName,
      String[] interfaces) {
    super.visit(version, access, name, signature, superName, interfaces);
    {
      FieldVisitor fv = cv.visitField(ACC_FINAL + ACC_STATIC, fieldName, fieldType, null, null);
      fv.visitEnd();
    }
  }

  private void writeGetProfileId() {
    {
      MethodVisitor mv = cv.visitMethod(ACC_PUBLIC, "getProfileId", "()I", null, null);
      mv.visitCode();
      mv.visitFieldInsn(GETSTATIC, className, fieldName, fieldType);
      mv.visitInsn(IRETURN);
      mv.visitMaxs(1, 1);
      mv.visitEnd();
    }
  }

  private void writeStaticCtor() {
    MethodVisitor mv = cv.visitMethod(ACC_STATIC, "<clinit>", "()V", null, null);
    mv.visitCode();
    CtorMethodVisitor.writeCtorBody(mv, className);
    mv.visitInsn(RETURN);
    mv.visitMaxs(1, 0);
    mv.visitEnd();
  }

  @Override
  public MethodVisitor visitMethod(
      int access, String name, String desc, String signature, String[] exceptions) {
    MethodVisitor mv = super.visitMethod(access, name, desc, signature, exceptions);
    if (CommonAdapter.isCCtor(access, name, desc)) {
      seenCCtor = true;
      return new CtorMethodVisitor(className, mv);
    } else return mv;
  }

  public void visitEnd() {
    writeGetProfileId();
    if (!seenCCtor) {
      writeStaticCtor();
    }
    super.visitEnd();
  }
}

class CtorMethodVisitor extends MethodVisitor implements Opcodes {
  private String className;

  CtorMethodVisitor(String className, MethodVisitor mv) {
    super(ASM9, mv);
    this.className = className;
  }

  @Override
  public void visitCode() {
    super.visitCode();
    writeCtorBody(mv, className);
  }

  static void writeCtorBody(MethodVisitor mv, String className) {
    mv.visitLdcInsn(Type.getObjectType(className));
    mv.visitMethodInsn(
        INVOKESTATIC,
        NodeMethodsInjectorAdapter.nodeTraceCls,
        "allocateAnonPNodeTaskInfo",
        "(Ljava/lang/Class;)" + NodeMethodsInjectorAdapter.fieldType,
        false);
    mv.visitFieldInsn(
        PUTSTATIC,
        className,
        NodeMethodsInjectorAdapter.fieldName,
        NodeMethodsInjectorAdapter.fieldType);
  }
}
