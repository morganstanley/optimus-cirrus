/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * Licensed under the Apache License, Version 2.0 (the "License").
 * You may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *     http://www.apache.org/licenses/LICENSE-2.0
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
/*
 * The above license is included because this file was heavily influenced by the more expansive https://github.com/corretto/hotpatch-for-apache-log4j2
 * For the parts of this file which vary from that version:
 *
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

import java.security.ProtectionDomain;

import org.objectweb.asm.*;

// We allow the JndiLookup class to load, but we render its lookup method harmless.
public class JndiDefanger implements ClassFileTransformer {

  public static final String goAway = "JndiLookup#lookup now returns this sentence unconditionally";

  public byte[] transform(
      ClassLoader loader,
      String className,
      Class<?> classBeingRedefined,
      ProtectionDomain protectionDomain,
      byte[] classfileBuffer) {

    if (className != null && className.endsWith("/JndiLookup")) {
      String msg =
          "Not an error; harmless but necessary: Defanging " + className + " (" + loader + ")";
      JndiDefangerClassLoadingPoint t = new JndiDefangerClassLoadingPoint(msg);
      t.printStackTrace(System.err);
      ClassWriter cw = new ClassWriter(ClassWriter.COMPUTE_FRAMES | ClassWriter.COMPUTE_MAXS);
      ClassVisitor cv =
          new ClassVisitor(Opcodes.ASM9, cw) {
            @Override
            public MethodVisitor visitMethod(
                int access, String name, String desc, String signature, String[] exceptions) {
              MethodVisitor mv = cv.visitMethod(access, name, desc, signature, exceptions);
              if ("lookup".equals(name)) {
                mv =
                    new MethodVisitor(Opcodes.ASM9, mv) {
                      @Override
                      public void visitCode() {
                        mv.visitCode();
                        mv.visitLdcInsn(goAway);
                        mv.visitInsn(Opcodes.ARETURN);
                      }
                    };
              }
              return mv;
            }
          };
      ClassReader cr = new ClassReader(classfileBuffer);
      cr.accept(cv, 0);
      return cw.toByteArray();
    } else {
      return null;
    }
  }
}
