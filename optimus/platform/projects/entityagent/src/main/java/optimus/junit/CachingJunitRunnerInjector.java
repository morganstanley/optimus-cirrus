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
package optimus.junit;

import static org.objectweb.asm.Opcodes.ACC_ABSTRACT;
import static org.objectweb.asm.Opcodes.ACC_NATIVE;
import static org.objectweb.asm.Opcodes.ACC_PUBLIC;
import static org.objectweb.asm.Opcodes.ALOAD;
import static org.objectweb.asm.Opcodes.INVOKESTATIC;

import java.io.IOException;
import java.io.InputStream;
import java.lang.instrument.ClassFileTransformer;
import java.lang.instrument.IllegalClassFormatException;
import java.security.ProtectionDomain;

import org.objectweb.asm.ClassReader;
import org.objectweb.asm.ClassVisitor;
import org.objectweb.asm.ClassWriter;
import org.objectweb.asm.MethodVisitor;
import org.objectweb.asm.Opcodes;

public class CachingJunitRunnerInjector implements ClassFileTransformer {
  public static final String JUNIT_RUNNER_CLASS = "org/junit/runner/Runner";
  public static final String RUN_INTERNAL_METHOD_NAME = "runInternalDontCollide";
  private static final Class baseClass;
  private static final Class dtcHelperClass;

  static {
    Class baseClass_ = null;
    Class dtcHelperClass_ = null;
    try {
      baseClass_ = Class.forName("org.junit.runner.Runner");
    } catch (ClassNotFoundException e) {
      //baseClass will be null, usage later is guarded
    }
    try {
      dtcHelperClass_ = Class.forName("optimus.dtc.runners.DTCDynamicRunnerHelper");
    } catch (ClassNotFoundException e) {
      //dtcHelperClass will be null, usage later is guarded
    }
    baseClass = baseClass_;
    dtcHelperClass = dtcHelperClass_;
  }

  private boolean extendsRunner(ClassLoader loader, String className) {
    if (className == null || skipName(className))
      return false;
    else if (className.equals(JUNIT_RUNNER_CLASS))
      return true;

    InputStream is = loader.getResourceAsStream(className + ".class");
    ClassReader reader;
    try {
      reader = new ClassReader(is);
    } catch (IOException e) {
      // throwing here will be logged by safeTransform
      throw new RuntimeException(e);
    } finally {
      try {
        is.close();
      } catch (IOException e) {
        //ignore
      }

    }
    return extendsRunner(loader, reader.getSuperName());
  }

  static boolean skipName(String name) {
    return name.startsWith("com.sun")
        || name.startsWith("java.")
        || name.startsWith("jdk.") // java 9+
        || name.startsWith("scala.")
        || name.startsWith("sun.")
        ;
  }

  @Override
  public byte[] transform(ClassLoader loader, String className, Class<?> classBeingRedefined,
      ProtectionDomain protectionDomain, byte[] classfileBuffer) throws IllegalClassFormatException {

    // if we've got the instrumentation system property but no dtc on classpath, will get logged by safeTransform
    if (dtcHelperClass == null) {
      throw new RuntimeException(new ClassNotFoundException(
          "optimus.dtc.runners.DTCDynamicRunnerHelper not found on classpath, cannot dynamically instrument junit " + "tests for DTC"));
    }

    // if we have the instrumentation system property but not junit on the classpath...
    if (baseClass == null) {
      throw new RuntimeException(new ClassNotFoundException(
          "org.junit.runner.Runner not found on classpath, cannot dynamically instrument junit tests for DTC"));
    }

    // not interested in bootstrap classpath or core java/scala classes
    if (loader == null || skipName(className)) {
      return null;
    }

    ClassReader classReader = new ClassReader(classfileBuffer);

    // is className assignableFrom junit runner base - without loading any new classes, as they won't be transformed if
    // we do it from here
    if (!extendsRunner(loader, classReader.getSuperName())) {
      return null;
    }

    ClassWriter classWriter = new ClassWriter(classReader, ClassWriter.COMPUTE_MAXS);

    ClassVisitor visitor = new ClassVisitor(Opcodes.ASM9, classWriter) {
      private boolean inVisitEnd = false;
      private boolean visitedRunMethod = false;

      @Override
      public void visitEnd() {
        //Target ASM Byte Code:
        // 0: aload_0
        // 1: aload_1
        // 2: invokestatic  #8 // Method runStatic:(Lorg/junit/runner/Runner;
        // Lorg/junit/runner/notification/RunNotifier;)V
        // 5: return
        //
        if (visitedRunMethod) {
          inVisitEnd = true; //flag so we can avoid re-writing the 'new' run method
          MethodVisitor mv = visitMethod(ACC_PUBLIC, "run", "(Lorg/junit/runner/notification/RunNotifier;)V", null,
              null);
          mv.visitCode();
          mv.visitVarInsn(ALOAD, 0);
          mv.visitVarInsn(ALOAD, 1);
          mv.visitMethodInsn(INVOKESTATIC, "optimus/dtc/runners/DTCRunnerHelper", "runStatic",
              "(Lorg/junit/runner/Runner;Lorg/junit/runner/notification/RunNotifier;)V", false);
          mv.visitInsn(Opcodes.RETURN);
          mv.visitMaxs(2, 2);
          mv.visitEnd();

        }
      }

      @Override
      public MethodVisitor visitMethod(int access, String name, String desc, String signature, String[] exceptions) {
        if (!inVisitEnd && name.equals("run") && desc.equals(
            "(Lorg/junit/runner/notification/RunNotifier;)V") && (access & (ACC_NATIVE | ACC_ABSTRACT)) == 0) {
          visitedRunMethod = true;
          return new MethodVisitor(Opcodes.ASM9,
              super.visitMethod(access, RUN_INTERNAL_METHOD_NAME, desc, signature, exceptions)) {
            @Override
            public void visitMethodInsn(int opcode, String owner, String name, String descriptor, boolean isInterface) {
              if (owner.equals(classReader.getSuperName()) && "run".equals(
                  name) && "(Lorg/junit/runner/notification/RunNotifier;)V".equals(descriptor))
                super.visitMethodInsn(opcode, owner, RUN_INTERNAL_METHOD_NAME, descriptor, isInterface);
              else
                super.visitMethodInsn(opcode, owner, name, descriptor, isInterface);
            }
          };
        } else {
          return super.visitMethod(access, name, desc, signature, exceptions);
        }
      }
    };
    classReader.accept(visitor, 0);

    return classWriter.toByteArray();
  }
}
