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
import static org.objectweb.asm.Opcodes.INVOKESTATIC;

import java.lang.instrument.ClassFileTransformer;
import java.lang.instrument.IllegalClassFormatException;
import java.security.ProtectionDomain;

import optimus.graph.DiagnosticSettings;
import org.objectweb.asm.ClassReader;
import org.objectweb.asm.ClassVisitor;
import org.objectweb.asm.ClassWriter;
import org.objectweb.asm.MethodVisitor;
import org.objectweb.asm.Opcodes;

public class OptimusTestWorkerClientInjector implements ClassFileTransformer {
  public static final String DTC_RUNNER_HELPER_CLASS = "optimus/dtc/runners/DTCRunnerHelper";
  public static final String CACHED_RUNS_MANAGER_CLASS = "optimus/dtc/cache/CachedRunsManager";
  public static final String OPTIMUS_TEST_WORKER_CLIENT_CLASS =
      "optimus/buildtool/testrunner/worker/OptimusTestWorkerClient";
  public static final String BREADCRUMBS_CLASS = "optimus/breadcrumbs/Breadcrumbs";

  private static final boolean dtcRunnerHelperClassFound;
  private static final boolean dtcCachedRunsManagerClassFound;
  private static final boolean optimusTestWorkerClientClassFound;
  private static final boolean breadcrumbsClassFound;

  private final boolean failedState;

  static {
    ClassLoader loader = Thread.currentThread().getContextClassLoader();
    // WARNING!
    //    We must absolutely avoid doing class loading here before transformation can begin!
    //    'getResourceAsStream' does not cause class loading, so we are safe with it.
    dtcRunnerHelperClassFound =
        loader.getResource("optimus/dtc/runners/DTCRunnerHelper.class") != null;
    dtcCachedRunsManagerClassFound =
        loader.getResource("optimus/dtc/cache/CachedRunsManager.class") != null;
    optimusTestWorkerClientClassFound =
        loader.getResource("optimus/buildtool/testrunner/worker/OptimusTestWorkerClient.class")
            != null;
    breadcrumbsClassFound = loader.getResource("optimus/breadcrumbs/Breadcrumbs.class") != null;
  }

  public OptimusTestWorkerClientInjector() {
    super();

    this.failedState =
        !dtcRunnerHelperClassFound
            || !dtcCachedRunsManagerClassFound
            || !optimusTestWorkerClientClassFound
            || !breadcrumbsClassFound;

    if (DiagnosticSettings.isClassMonitorEnabled
        && DiagnosticSettings.enableJunitRunnerMonitorInjection) {
      // we've got the instrumentation system property but no dtc on classpath!
      // we will report only one, if we are asked to instrument
      if (!dtcRunnerHelperClassFound) {
        logErrMsg(
            "optimus.dtc.runners.DTCRunnerHelper not found on classpath, cannot dynamically instrument OptimusTestWorkerClient to reduce DTC overhead");
      }

      // if we have the instrumentation system property but no junit on the classpath...
      if (!dtcCachedRunsManagerClassFound) {
        logErrMsg(
            "optimus.dtc.cache.CachedRunsManager not found on classpath, cannot dynamically instrument OptimusTestWorkerClient to reduce DTC overhead");
      }

      // if we have the instrumentation system property but no Optimus test worker client on the
      // classpath...
      if (!optimusTestWorkerClientClassFound) {
        logErrMsg(
            "optimus.buildtool.testrunner.worker.OptimusTestWorkerClient not found on classpath, cannot dynamically instrument OptimusTestWorkerClient to reduce DTC overhead");
      }

      // if we have the instrumentation system property but no breadcrumb infrastructure on the
      // classpath...
      if (!breadcrumbsClassFound) {
        logErrMsg(
            "optimus.breadcrumbs.Breadcrumbs not found on classpath, cannot dynamically instrument OptimusTestWorkerClient to reduce DTC overhead");
      }
    }
  }

  private static boolean skipName(String name) {
    return !name.equals(OPTIMUS_TEST_WORKER_CLIENT_CLASS);
  }

  @Override
  public byte[] transform(
      ClassLoader loader,
      String className,
      Class<?> classBeingRedefined,
      ProtectionDomain protectionDomain,
      byte[] classfileBuffer)
      throws IllegalClassFormatException {

    // not interested in any classes but OptimusTestWorkerClient or being in a bad state
    if (failedState || loader == null || skipName(className)) {
      return null;
    }

    ClassReader classReader = new ClassReader(classfileBuffer);

    ClassWriter classWriter = new ClassWriter(classReader, ClassWriter.COMPUTE_MAXS);
    ClassVisitor visitor =
        new ClassVisitor(Opcodes.ASM9, classWriter) {
          @Override
          public MethodVisitor visitMethod(
              int access, String name, String desc, String signature, String[] exceptions) {
            if (name.equals("main")
                && desc.equals("([Ljava/lang/String;)V")
                && (access & (ACC_NATIVE | ACC_ABSTRACT)) == 0) {

              return new MethodVisitor(
                  Opcodes.ASM9, super.visitMethod(access, name, desc, signature, exceptions)) {
                @Override
                public void visitCode() {
                  visitMethodInsn(
                      INVOKESTATIC,
                      DTC_RUNNER_HELPER_CLASS,
                      "disableWaitingAllOperationsCompletion",
                      "()V",
                      false);
                  super.visitCode();
                }

                @Override
                public void visitMethodInsn(
                    int opcode, String owner, String name, String descriptor, boolean isInterface) {
                  if (owner.equals("java/lang/System")
                      && "exit".equals(name)
                      && "(I)V".equals(descriptor)) {
                    visitMethodInsn(
                        INVOKESTATIC,
                        CACHED_RUNS_MANAGER_CLASS,
                        "awaitOnAllOperationsCompletion",
                        "()V",
                        false);
                    visitMethodInsn(INVOKESTATIC, BREADCRUMBS_CLASS, "flush", "()V", false);
                  }
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
