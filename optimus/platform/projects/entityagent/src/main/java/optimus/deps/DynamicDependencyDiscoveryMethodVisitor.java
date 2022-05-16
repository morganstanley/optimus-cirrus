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
package optimus.deps;

import static optimus.deps.BaseClassVisitor.ASM_API;

import optimus.ClassMonitorInjector;
import org.objectweb.asm.MethodVisitor;

// == Dynamic dependency discovery ============================
//
public class DynamicDependencyDiscoveryMethodVisitor extends MethodVisitor {
  private final VisitContext context;
  private final DynamicDependencyDiscoveryInstrumentation instrumentation;

  DynamicDependencyDiscoveryMethodVisitor(VisitContext context, MethodVisitor mv) {
    super(ASM_API, mv);

    this.context = context;
    this.instrumentation = new DynamicDependencyDiscoveryInstrumentation(context);
  }

  // Eventually consider monitoring
  // - JDBC
  // - ProcessBuilder
  // - scala.io.Source


  // Handle methods which returns Class, resource URL, or new class instance
  public void visitMethodInsn(int opcode, String owner, String name, String desc, boolean itf) {
    switch (owner) {
    case ClassMonitorInjector.javaLangClass:
    case ClassMonitorInjector.javaLangClassLoader:
      switch (name) {
      case "getResource":
        super.visitMethodInsn(opcode, owner, name, desc, itf);
        instrumentation.addCallToLoggerForUrlResource(mv); // Logging the return type
        break;
      case "getResourceAsStream":
        switch (owner) {
        case ClassMonitorInjector.javaLangClass:
          instrumentation.addCallToLoggerForStreamResourceOnClass(mv);
          break;
        case ClassMonitorInjector.javaLangClassLoader:
          instrumentation.addCallToLoggerForStreamResourceOnClassLoader(mv);
          break;
        default:
          throw new IllegalStateException(String.format("We do not know how to handle this case with '%s'", owner));
        }
        super.visitMethodInsn(opcode, owner, name, desc, itf);
        break;
      case "getResources":
        instrumentation.addCallToLoggerForResources(mv);
        super.visitMethodInsn(opcode, owner, name, desc, itf);
        break;
      case "forName":
      case "loadClass":
        super.visitMethodInsn(opcode, owner, name, desc, itf);
        instrumentation.addCallToLoggerForClassUsage(mv); // Logging the return type
        break;
      default:
        super.visitMethodInsn(opcode, owner, name, desc, itf);
      }
      break;
    default:
      if (name.equals("<init>")) {
        switch (owner) {
        case "java/io/FileInputStream":
          // Skip very heaving SessionUtils for which there is no benefit to track (access to .class)
          if (!context.classResourceName.startsWith("optimus/session/utils/SessionUtils")) {
            instrumentation.addCallToLoggerForFileInputResource(mv, context.className, owner, name, desc);
          }
          super.visitMethodInsn(opcode, owner, name, desc, itf);
          break;
        case "java/io/FileOutputStream":
          instrumentation.addCallToLoggerForFileOutputResource(mv, context.className, owner, name, desc);
          super.visitMethodInsn(opcode, owner, name, desc, itf);
          break;
        case "java/io/RandomAccessFile":
          instrumentation.addCallToLoggerForFileRandomAccessResource(mv, context.className, owner, name, desc);
          super.visitMethodInsn(opcode, owner, name, desc, itf);
          break;
        case "java/net/URI":
          super.visitMethodInsn(opcode, owner, name, desc, itf);
          instrumentation.addCallToLoggerForUriResource(mv);
          break;
        case "java/net/URL":
          super.visitMethodInsn(opcode, owner, name, desc, itf);
          instrumentation.addCallToLoggerForUrlResource(mv);
          break;
        case "msjava/msnet/MSNetInetAddress":
          instrumentation.addCallToLoggerForMSInetAddressResource(mv, desc);
          super.visitMethodInsn(opcode, owner, name, desc, itf);
          break;
        default:
          super.visitMethodInsn(opcode, owner, name, desc, itf);
        }
      }
      else {
        super.visitMethodInsn(opcode, owner, name, desc, itf);
      }
    }
  }

  @Override
  public void visitMaxs(int maxStack, int maxLocals) {
    // We add up to three arguments
    super.visitMaxs(maxStack + 3, maxLocals);
  }
}
