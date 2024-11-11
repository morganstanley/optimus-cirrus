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

import static optimus.EntityAgent.*;
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

import optimus.graph.DiagnosticSettings;
import org.objectweb.asm.ClassReader;
import org.objectweb.asm.ClassVisitor;
import org.objectweb.asm.ClassWriter;
import org.objectweb.asm.MethodVisitor;
import org.objectweb.asm.Opcodes;

public class CachingJunitRunnerInjector implements ClassFileTransformer {
  public static final String JUNIT_RUNNER_CLASS = "org/junit/runner/Runner";
  public static final String RUN_INTERNAL_METHOD_NAME = "runInternalDontCollide";

  private static final boolean baseClassFound;
  private static final boolean dtcHelperClassFound;

  private final boolean failedState;

  static {
    ClassLoader loader = Thread.currentThread().getContextClassLoader();
    // WARNING!
    //    We must absolutely avoid doing class loading here before transformation can begin!
    //    'getResourceAsStream' does not cause class loading, so we are safe with it.
    baseClassFound = loader.getResource("org/junit/runner/Runner.class") != null;
    dtcHelperClassFound =
        loader.getResource("optimus/dtc/runners/DTCDynamicRunnerHelper.class") != null;
  }

  public CachingJunitRunnerInjector() {
    super();

    this.failedState = !dtcHelperClassFound || !baseClassFound;

    if (DiagnosticSettings.isClassMonitorEnabled) {
      final String state;
      if (DiagnosticSettings.enableJunitRunnerMonitorInjection) {
        if (!dtcHelperClassFound) {
          // we've got the instrumentation system property but no dtc on classpath!
          // we will report only one, if we are asked to instrument
          logErrMsg(
              "optimus.dtc.runners.DTCDynamicRunnerHelper not found on classpath, cannot dynamically instrument junit tests for DTC");
          state = "OFF";
        } else if (!baseClassFound) {
          // if we have the instrumentation system property but not junit on the classpath...
          logErrMsg(
              "org.junit.runner.Runner not found on classpath, cannot dynamically instrument junit tests for DTC");
          state = "OFF";
        } else {
          state = "ON";
        }
      } else {
        state = "OFF";
      }
      logMsg(String.format("[DTC] Dynamic instrumentation of JUnit runners is %s!", state));
    }
  }

  private boolean extendsRunner(ClassLoader loader, String className, String superClassName) {
    if (superClassName == null || skipName(superClassName)) return false;
    else if (superClassName.equals(JUNIT_RUNNER_CLASS)) return true;

    // getResourceAsStream does not cause class loading, which we must absolutely avoid
    InputStream is = loader.getResourceAsStream(superClassName + ".class");
    ClassReader reader;
    try (is) {
      if (is == null)
        throw new RuntimeException(
            String.format("Parent class of %s not found: %s", className, superClassName));
      reader = new ClassReader(is);
    } catch (IOException e) {
      // throwing here will be logged by safeTransform
      throw new RuntimeException(e);
    }
    return extendsRunner(loader, superClassName, reader.getSuperName());
  }

  private static boolean skipName(String name) {
    return name.startsWith("com/sun")
        || name.startsWith("java/")
        || name.startsWith("jdk/") // java 9+
        || name.startsWith("scala/")
        || name.startsWith("sun/")
        || name.startsWith("ch/qos/")
        || name.startsWith("org/slf4j/");
  }

  @Override
  public byte[] transform(
      ClassLoader loader,
      String className,
      Class<?> classBeingRedefined,
      ProtectionDomain protectionDomain,
      byte[] classfileBuffer)
      throws IllegalClassFormatException {

    // not interested in bootstrap classpath or core java/scala classes or being in a bad state
    if (failedState || loader == null || skipName(className)) {
      return null;
    }

    ClassReader classReader = new ClassReader(classfileBuffer);

    // is className assignableFrom junit runner base - without loading any new classes, as they
    // won't be transformed if
    // we do it from here
    if (!extendsRunner(loader, className, classReader.getSuperName())) {
      return null;
    }

    ClassWriter classWriter = new ClassWriter(classReader, ClassWriter.COMPUTE_MAXS);

    ClassVisitor visitor =
        new ClassVisitor(Opcodes.ASM9, classWriter) {
          private boolean inVisitEnd = false;
          private boolean visitedRunMethod = false;

          @Override
          public void visitEnd() {
            // Target ASM Byte Code:
            // 0: aload_0
            // 1: aload_1
            // 2: invokestatic  #8 // Method runStatic:(Lorg/junit/runner/Runner;
            // Lorg/junit/runner/notification/RunNotifier;)V
            // 5: return
            //
            if (visitedRunMethod) {
              inVisitEnd = true; // flag so we can avoid re-writing the 'new' run method

              // We had renamed the 'run' method into 'runInternalDontCollide` in 'visitMethod',
              // leaving a gap.
              // We thus inject a new 'run' method that points to DTCRunnerHelper.runStatic(Runner,
              // RunNotifier).
              MethodVisitor mv =
                  visitMethod(
                      ACC_PUBLIC,
                      "run",
                      "(Lorg/junit/runner/notification/RunNotifier;)V",
                      null,
                      null);
              mv.visitCode();
              mv.visitVarInsn(ALOAD, 0);
              mv.visitVarInsn(ALOAD, 1);
              mv.visitMethodInsn(
                  INVOKESTATIC,
                  "optimus/dtc/runners/DTCRunnerHelper",
                  "runStatic",
                  "(Lorg/junit/runner/Runner;Lorg/junit/runner/notification/RunNotifier;)V",
                  false);
              mv.visitInsn(Opcodes.RETURN);
              mv.visitMaxs(2, 2);
              mv.visitEnd();
            }
          }

          @Override
          public MethodVisitor visitMethod(
              int access, String name, String desc, String signature, String[] exceptions) {
            if (!inVisitEnd
                && name.equals("run")
                && desc.equals("(Lorg/junit/runner/notification/RunNotifier;)V")
                && (access & (ACC_NATIVE | ACC_ABSTRACT)) == 0) {
              visitedRunMethod = true;

              // Rename the 'run' method as 'runInternalDontCollide', which will leave a gap that is
              // filled in visitEnd.
              return new MethodVisitor(
                  Opcodes.ASM9,
                  super.visitMethod(
                      access, RUN_INTERNAL_METHOD_NAME, desc, signature, exceptions)) {
                @Override
                public void visitMethodInsn(
                    int opcode, String owner, String name, String descriptor, boolean isInterface) {
                  if (owner.equals(classReader.getSuperName())
                      && "run".equals(name)
                      && "(Lorg/junit/runner/notification/RunNotifier;)V".equals(descriptor)) {
                    // Let's rewire to point to the internal method we injected in the parent so we
                    // do not land
                    // back into DTCRunnerHelper in a nested way.
                    super.visitMethodInsn(
                        opcode, owner, RUN_INTERNAL_METHOD_NAME, descriptor, isInterface);
                  } else {
                    super.visitMethodInsn(opcode, owner, name, descriptor, isInterface);
                  }
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
