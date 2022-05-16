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

import static org.objectweb.asm.Opcodes.INVOKESTATIC;

import optimus.ClassMonitorInjector;
import org.objectweb.asm.MethodVisitor;
import org.objectweb.asm.Opcodes;
import org.objectweb.asm.Type;

class DynamicDependencyDiscoveryInstrumentation {
  private final String classResourceName;

  DynamicDependencyDiscoveryInstrumentation(VisitContext context) {
    super();

    this.classResourceName = context.classResourceName;
  }

  void addCallToLoggerForClassUsage(MethodVisitor mv) {
    // First param is always already loaded and is either Class or new class instance
    mv.visitInsn(Opcodes.DUP);
    mv.visitMethodInsn(INVOKESTATIC, "optimus/ClassMonitorInjector", "logClassUsage", "(Ljava/lang/Class;)V",
        false);
  }

  void addCallToLoggerForUrlResource(MethodVisitor mv) {
    mv.visitInsn(Opcodes.DUP);
    mv.visitLdcInsn(classResourceName);
    mv.visitMethodInsn(INVOKESTATIC, "optimus/ClassMonitorInjector", "logUrlResourceUsage",
        "(Ljava/net/URL;Ljava/lang/String;)V", false);
  }

  void addCallToLoggerForStreamResourceOnClassLoader(MethodVisitor mv) {
    // First param is always already loaded and is either Class or new class instance
    mv.visitInsn(Opcodes.DUP2);
    mv.visitLdcInsn(classResourceName);
    mv.visitMethodInsn(INVOKESTATIC, "optimus/ClassMonitorInjector", "logClassAndStreamResourceByNameUsage",
        "(Ljava/lang/ClassLoader;Ljava/lang/String;Ljava/lang/String;)V", false);
  }

  void addCallToLoggerForStreamResourceOnClass(MethodVisitor mv) {
    // First param is always already loaded and is either Class or new class instance
    mv.visitInsn(Opcodes.DUP2);
    mv.visitLdcInsn(classResourceName);
    mv.visitMethodInsn(INVOKESTATIC, "optimus/ClassMonitorInjector", "logClassAndStreamResourceByNameUsage",
        "(Ljava/lang/Class;Ljava/lang/String;Ljava/lang/String;)V", false);
  }

  void addCallToLoggerForResources(MethodVisitor mv) {
    // First param is always already loaded and is either Class or new class instance
    mv.visitInsn(Opcodes.DUP2);
    mv.visitLdcInsn(classResourceName);
    mv.visitMethodInsn(INVOKESTATIC, "optimus/ClassMonitorInjector", "logClassAndResourcesByNameUsage",
        "(Ljava/lang/ClassLoader;Ljava/lang/String;Ljava/lang/String;)V", false);
  }

  private final static String fileDescriptorTypeDesc = "Ljava/io/FileDescriptor;";
  private final static String fileTypeDesc = "Ljava/io/File;";
  private final static String stringTypeDesc = "Ljava/lang/String;";

  private boolean isOneOf(String argDesc, String... choices) {
    for (String choice : choices) {
      if (argDesc.equals(choice))
        return true;
    }
    return false;
  }

  void addCallToLoggerForFileInputResource(MethodVisitor mv, String className, String owner, String name, String desc) {
    Type[] argTypes = Type.getType(desc).getArgumentTypes();
    if (argTypes.length == 1 && isOneOf(argTypes[0].getDescriptor(), fileTypeDesc, stringTypeDesc, fileDescriptorTypeDesc)) {
      // File descriptors represents the console in, which we do not care about
      if (!argTypes[0].getDescriptor().equals(fileDescriptorTypeDesc)) {
        mv.visitInsn(Opcodes.DUP);
        mv.visitLdcInsn("r"); // Add read access
        mv.visitLdcInsn(classResourceName);
        mv.visitMethodInsn(INVOKESTATIC, "optimus/ClassMonitorInjector", "logFileResourceUsage",
            String.format("(%sLjava/lang/String;Ljava/lang/String;)V", argTypes[0]), false);
      }
    } else {
      ClassMonitorInjector.recordInternalEvent(
          String.format("%s Unmanaged call to %s.%s%s from %s: check for API change", ClassMonitorInjector.CMI_ERROR, owner, name,
              desc, className));
    }
  }

  void addCallToLoggerForFileOutputResource(MethodVisitor mv, String className, String owner, String name,
      String desc) {
    Type[] argTypes = Type.getType(desc).getArgumentTypes();
    if ((argTypes.length == 2 || argTypes.length == 1) && isOneOf(argTypes[0].getDescriptor(), fileTypeDesc,
        stringTypeDesc, fileDescriptorTypeDesc)) {
      // File descriptors represents the console out/err, which we do not care about
      if (!argTypes[0].getDescriptor().equals(fileDescriptorTypeDesc)) {
        if (argTypes.length == 2) {
          mv.visitInsn(Opcodes.DUP2);
          mv.visitInsn(Opcodes.POP);
        } else {
          mv.visitInsn(Opcodes.DUP);
        }
        mv.visitLdcInsn("w"); // Replace with write access
        mv.visitLdcInsn(classResourceName);
        mv.visitMethodInsn(INVOKESTATIC, "optimus/ClassMonitorInjector", "logFileResourceUsage",
            String.format("(%sLjava/lang/String;Ljava/lang/String;)V", argTypes[0]), false);
      }
    } else {
      ClassMonitorInjector.recordInternalEvent(
          String.format("%s Unmanaged call to %s.%s%s from %s: check for API change", ClassMonitorInjector.CMI_ERROR, owner, name,
              desc, className));
    }
  }

  void addCallToLoggerForFileRandomAccessResource(MethodVisitor mv, String className, String owner, String name,
      String desc) {
    Type[] argTypes = Type.getType(desc).getArgumentTypes();
    if (argTypes.length == 2 && isOneOf(argTypes[0].getDescriptor(), fileTypeDesc, stringTypeDesc)) {
      mv.visitInsn(Opcodes.DUP2);
      mv.visitLdcInsn(classResourceName);
      mv.visitMethodInsn(INVOKESTATIC, "optimus/ClassMonitorInjector", "logFileResourceUsage",
          String.format("(%sLjava/lang/String;Ljava/lang/String;)V", argTypes[0]), false);
    } else {
      ClassMonitorInjector.recordInternalEvent(
          String.format("%s Unmanaged call to %s.%s%s from %s: check for API change", ClassMonitorInjector.CMI_ERROR, owner, name,
              desc, className));
    }
  }

  void addCallToLoggerForUriResource(MethodVisitor mv) {
    mv.visitInsn(Opcodes.DUP); // Dup new object
    mv.visitLdcInsn(classResourceName);
    mv.visitMethodInsn(INVOKESTATIC, "optimus/ClassMonitorInjector", "logUriResourceUsage",
        "(Ljava/net/URI;Ljava/lang/String;)V", false);
  }

  void addCallToLoggerForMSInetAddressResource(MethodVisitor mv, String desc) {
    switch (desc) {
    case "(Ljava/net/InetAddress;I)V":
      mv.visitInsn(Opcodes.DUP2);
      mv.visitLdcInsn(classResourceName);
      mv.visitMethodInsn(INVOKESTATIC, "optimus/ClassMonitorInjector", "logIpPortResourceUsage",
          "(Ljava/net/InetAddress;ILjava/lang/String;)V", false);
      break;
    case "(Ljava/lang/String;I)V":
      mv.visitInsn(Opcodes.DUP2);
      mv.visitLdcInsn(classResourceName);
      mv.visitMethodInsn(INVOKESTATIC, "optimus/ClassMonitorInjector", "logIpPortResourceUsage",
          "(Ljava/lang/String;ILjava/lang/String;)V", false);
      break;
    case "(Ljava/lang/String;)V":
      mv.visitInsn(Opcodes.DUP);
      mv.visitLdcInsn(classResourceName);
      mv.visitMethodInsn(INVOKESTATIC, "optimus/ClassMonitorInjector", "logIpResourceUsage",
          "(Ljava/lang/String;Ljava/lang/String;)V", false);
      break;
    }
  }
}
