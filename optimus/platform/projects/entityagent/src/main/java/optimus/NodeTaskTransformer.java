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

import static optimus.NodeTaskTransformer.nodeTaskDesc;
import static optimus.NodeTaskTransformer.nodeTaskTpe;
import static optimus.NodeTaskTransformer.tpdMaskDesc;
import static optimus.NodeTaskTransformer.tpdMaskTpe;

import java.lang.instrument.ClassFileTransformer;
import java.security.ProtectionDomain;
import java.util.ArrayList;
import java.util.List;

import optimus.graph.DiagnosticSettings;
import org.objectweb.asm.ClassReader;
import org.objectweb.asm.ClassVisitor;
import org.objectweb.asm.ClassWriter;
import org.objectweb.asm.FieldVisitor;
import org.objectweb.asm.Label;
import org.objectweb.asm.MethodVisitor;
import org.objectweb.asm.Opcodes;

public class NodeTaskTransformer implements ClassFileTransformer {
  static final String nodeTaskTpe = "optimus/graph/NodeTask";
  static final String tpdMaskTpe = "optimus/core/TPDMask";
  static final String nodeTaskDesc = "Loptimus/graph/NodeTask;";
  static final String tpdMaskDesc = "Loptimus/core/TPDMask;";

  @Override
  public byte[] transform(
      ClassLoader loader, String name, Class<?> redef, ProtectionDomain pd, byte[] bytes) {
    List<String> properties = new ArrayList<>();
    if (DiagnosticSettings.traceAvailable) {
      properties.add("Profile");
      properties.add("Id");
      properties.add("SelfPlusANCTime");
    }
    if (DiagnosticSettings.traceTweaksEnabled) properties.add("TweakInfection");
    if (DiagnosticSettings.traceEnqueuer) properties.add("Enqueuer");
    if (DiagnosticSettings.awaitStacks) properties.add("EnqueuerStackHash");

    ClassReader crSource = new ClassReader(bytes);
    ClassWriter cw = new ClassWriter(crSource, 0);
    ClassVisitor cv = new NodeTaskAdapter(properties, cw);
    crSource.accept(cv, 0);
    return cw.toByteArray();
  }
}

class TPDMaskTransfomer implements ClassFileTransformer {
  @Override
  public byte[] transform(
      ClassLoader loader, String name, Class<?> redef, ProtectionDomain pd, byte[] bytes) {
    ClassReader crSource = new ClassReader(bytes);
    ClassWriter cw = new ClassWriter(crSource, 0);
    ClassVisitor cv = new TPDMaskAdapter(cw);
    crSource.accept(cv, 0);
    return cw.toByteArray();
  }
}

class TPDMaskAdapter extends ClassVisitor implements Opcodes {
  private static final String removeField0 = "m0";
  private static final String removeField1 = "m1";

  TPDMaskAdapter(ClassVisitor cv) {
    super(ASM9, cv);
  }

  @Override
  public FieldVisitor visitField(
      int access, String name, String descriptor, String signature, Object value) {
    if (removeField0.equals(name) || removeField1.equals(name)) return null;
    return super.visitField(access, name, descriptor, signature, value);
  }

  @Override
  public MethodVisitor visitMethod(
      int access, String name, String desc, String signature, String[] exceptions) {
    MethodVisitor mv = super.visitMethod(access, name, desc, signature, exceptions);
    if ("fillFromArray".equals(name)) {
      return new MethodVisitor(ASM9, mv) {
        @Override
        public void visitLineNumber(int line, Label start) {
          super.visitLineNumber(line, start);
          if (mv == null) return;
          for (int i = 0; i < DiagnosticSettings.tweakUsageQWords; ++i) {
            mv.visitVarInsn(ALOAD, 0);
            mv.visitVarInsn(ALOAD, 1);
            mv.visitIntInsn(BIPUSH, i);
            mv.visitInsn(LALOAD);
            mv.visitFieldInsn(PUTFIELD, tpdMaskTpe, "m" + i, "J");
          }
          mv.visitInsn(RETURN);
          Label endLabel = new Label();
          mv.visitLabel(endLabel);
          mv.visitLocalVariable("mask", tpdMaskDesc, null, start, endLabel, 0);
          mv.visitLocalVariable("maskArray", "[J", null, start, endLabel, 1);
          mv.visitMaxs(3, 2);
          mv.visitEnd();
          this.mv = null; // Stop writing all the previous code....
        }
      };
    }
    if ("toArray".equals(name)) {
      return new MethodVisitor(ASM9, mv) {
        @Override
        public void visitLineNumber(int line, Label start) {
          super.visitLineNumber(line, start);
          if (mv == null) return;

          // new long[DiagnosticSettings.tweakUsageQWords]
          mv.visitIntInsn(BIPUSH, DiagnosticSettings.tweakUsageQWords);
          mv.visitIntInsn(NEWARRAY, T_LONG);

          for (int i = 0; i < DiagnosticSettings.tweakUsageQWords; ++i) {
            mv.visitInsn(DUP);
            mv.visitIntInsn(BIPUSH, i);
            mv.visitVarInsn(ALOAD, 0);
            mv.visitFieldInsn(GETFIELD, tpdMaskTpe, "m" + i, "J");
            mv.visitInsn(LASTORE);
          }
          mv.visitInsn(ARETURN);
          Label endLabel = new Label();
          mv.visitLabel(endLabel);
          mv.visitLocalVariable("this", tpdMaskDesc, null, start, endLabel, 0);
          mv.visitMaxs(5, 1);
          mv.visitEnd();
          this.mv = null; // Stop writing all the previous code....
        }
      };
    }
    if ("merge".equals(name)) {
      return new MethodVisitor(ASM9, mv) {
        @Override
        public void visitLineNumber(int line, Label start) {
          super.visitLineNumber(line, start);
          if (mv == null) return;

          for (int i = 0; i < DiagnosticSettings.tweakUsageQWords; ++i) {
            mv.visitVarInsn(ALOAD, 0);
            mv.visitInsn(DUP);
            mv.visitFieldInsn(GETFIELD, tpdMaskTpe, "m" + i, "J");
            mv.visitVarInsn(ALOAD, 1);
            mv.visitFieldInsn(GETFIELD, tpdMaskTpe, "m" + i, "J");
            mv.visitInsn(LOR);
            mv.visitFieldInsn(PUTFIELD, tpdMaskTpe, "m" + i, "J");
          }
          mv.visitInsn(RETURN);
          Label endLabel = new Label();
          mv.visitLabel(endLabel);
          mv.visitLocalVariable("this", tpdMaskDesc, null, start, endLabel, 0);
          mv.visitLocalVariable("mask", tpdMaskDesc, null, start, endLabel, 1);
          mv.visitMaxs(5, 2);
          mv.visitEnd();
          this.mv = null; // Stop writing all the previous code....
        }
      };
    }
    if ("intersects".equals(name)) {
      return new MethodVisitor(ASM9, mv) {
        @Override
        public void visitLineNumber(int line, Label start) {
          super.visitLineNumber(line, start);
          if (mv == null) return;

          Label retTrue = new Label();
          Label retLabel = new Label();
          for (int i = 0; i < DiagnosticSettings.tweakUsageQWords; ++i) {
            mv.visitVarInsn(ALOAD, 0);
            mv.visitFieldInsn(GETFIELD, tpdMaskTpe, "m" + i, "J");
            mv.visitVarInsn(ALOAD, 1);
            mv.visitFieldInsn(GETFIELD, tpdMaskTpe, "m" + i, "J");
            mv.visitInsn(LAND);
            mv.visitInsn(LCONST_0);
            mv.visitInsn(LCMP);
            mv.visitJumpInsn(IFNE, retTrue);
          }
          mv.visitInsn(ICONST_0);
          mv.visitJumpInsn(GOTO, retLabel);

          mv.visitLabel(retTrue);
          mv.visitFrame(Opcodes.F_SAME, 0, null, 0, null);
          mv.visitInsn(ICONST_1);

          mv.visitLabel(retLabel);
          mv.visitFrame(Opcodes.F_SAME1, 0, null, 1, new Object[] {Opcodes.INTEGER});
          mv.visitInsn(IRETURN);
          Label endLabel = new Label();
          mv.visitLabel(endLabel);
          mv.visitLocalVariable("this", tpdMaskDesc, null, start, endLabel, 0);
          mv.visitLocalVariable("mask", tpdMaskDesc, null, start, endLabel, 1);
          mv.visitMaxs(4, 2);
          mv.visitEnd();
          this.mv = null; // Stop writing all the previous code....
        }
      };
    }
    if ("subsetOf".equals(name)) {
      return new MethodVisitor(ASM9, mv) {
        @Override
        public void visitLineNumber(int line, Label start) {
          super.visitLineNumber(line, start);
          if (mv == null) return;

          Label retFalse = new Label();
          Label retLabel = new Label();

          for (int i = 0; i < DiagnosticSettings.tweakUsageQWords; ++i) {
            mv.visitVarInsn(ALOAD, 0);
            mv.visitFieldInsn(GETFIELD, tpdMaskTpe, "m" + i, "J");
            mv.visitVarInsn(ALOAD, 1);
            mv.visitFieldInsn(GETFIELD, tpdMaskTpe, "m" + i, "J");
            mv.visitInsn(LOR);
            mv.visitVarInsn(ALOAD, 1);
            mv.visitFieldInsn(GETFIELD, tpdMaskTpe, "m" + i, "J");
            mv.visitInsn(LCMP);
            mv.visitJumpInsn(IFNE, retFalse);
          }

          mv.visitInsn(ICONST_1);
          mv.visitJumpInsn(GOTO, retLabel);
          mv.visitLabel(retFalse);
          mv.visitFrame(Opcodes.F_SAME, 0, null, 0, null);
          mv.visitInsn(ICONST_0);
          mv.visitLabel(retLabel);
          mv.visitFrame(Opcodes.F_SAME1, 0, null, 1, new Object[] {Opcodes.INTEGER});
          mv.visitInsn(IRETURN);
          Label endLabel = new Label();
          mv.visitLabel(endLabel);
          mv.visitLocalVariable("this", tpdMaskDesc, null, start, endLabel, 0);
          mv.visitLocalVariable("mask", tpdMaskDesc, null, start, endLabel, 1);
          mv.visitMaxs(4, 2);
          mv.visitEnd();
          this.mv = null; // Stop writing all the previous code....
        }
      };
    }
    return mv;
  }

  @Override
  public void visitEnd() {
    for (int i = 0; i < DiagnosticSettings.tweakUsageQWords; ++i) {
      cv.visitField(ACC_PUBLIC, "m" + i, "J", null, null);
    }
    super.visitEnd();
  }
}

/** [SEE_MASK_SUPPORT_GENERATION] */
class NodeTaskAdapter extends AddFieldsAdapter {
  private static final String removeField0 = "_tpd0";
  private static final String removeField1 = "_tpd1";

  NodeTaskAdapter(List<String> properties, ClassVisitor cv) {
    super(properties, cv);
  }

  @Override
  public FieldVisitor visitField(
      int access, String name, String descriptor, String signature, Object value) {
    if (removeField0.equals(name) || removeField1.equals(name)) return null;
    return super.visitField(access, name, descriptor, signature, value);
  }

  @Override
  public MethodVisitor visitMethod(
      int access, String name, String desc, String signature, String[] exceptions) {
    MethodVisitor mv = super.visitMethod(access, name, desc, signature, exceptions);
    if ("setTweakPropertyDependency".equals(name)) {
      return new MethodVisitor(ASM9, mv) {
        @Override
        public void visitLineNumber(int line, Label start) {
          super.visitLineNumber(line, start);
          if (mv == null) return;
          for (int i = 0; i < DiagnosticSettings.tweakUsageQWords; ++i) {
            mv.visitVarInsn(ALOAD, 0);
            mv.visitVarInsn(ALOAD, 1);
            mv.visitFieldInsn(GETFIELD, tpdMaskTpe, "m" + i, "J");
            mv.visitFieldInsn(PUTFIELD, nodeTaskTpe, "_tpd" + i, "J");
          }
          mv.visitInsn(RETURN);
          Label endLabel = new Label();
          mv.visitLabel(endLabel);
          mv.visitLocalVariable("this", nodeTaskDesc, null, start, endLabel, 0);
          mv.visitLocalVariable("mask", tpdMaskDesc, null, start, endLabel, 1);
          mv.visitMaxs(3, 2);
          mv.visitEnd();
          this.mv = null; // Stop writing all the previous code....
        }
      };
    }
    if ("mergeTweakPropertyDependency".equals(name)) {
      return new MethodVisitor(ASM9, mv) {
        @Override
        public void visitLineNumber(int line, Label start) {
          super.visitLineNumber(line, start);
          if (mv == null) return;
          for (int i = 0; i < DiagnosticSettings.tweakUsageQWords; ++i) {
            mv.visitVarInsn(ALOAD, 0);
            mv.visitInsn(DUP); // <-- Extra 'this' used with PUTFIELD
            mv.visitFieldInsn(GETFIELD, nodeTaskTpe, "_tpd" + i, "J");
            mv.visitVarInsn(ALOAD, 1);
            mv.visitFieldInsn(GETFIELD, nodeTaskTpe, "_tpd" + i, "J");
            mv.visitInsn(LOR);
            mv.visitFieldInsn(PUTFIELD, nodeTaskTpe, "_tpd" + i, "J");
          }
          mv.visitInsn(RETURN);
          Label endLabel = new Label();
          mv.visitLabel(endLabel);
          mv.visitLocalVariable("this", nodeTaskDesc, null, start, endLabel, 0);
          mv.visitLocalVariable("child", nodeTaskDesc, null, start, endLabel, 1);
          mv.visitMaxs(5, 2);
          mv.visitEnd();
          this.mv = null; // Stop writing all the previous code....
        }
      };
    }
    if ("mergeTweakPropertyDependenciesInto".equals(name)) {
      return new MethodVisitor(ASM9, mv) {
        @Override
        public void visitLineNumber(int line, Label start) {
          super.visitLineNumber(line, start);
          if (mv == null) return;
          for (int i = 0; i < DiagnosticSettings.tweakUsageQWords; ++i) {
            mv.visitVarInsn(ALOAD, 1);
            mv.visitInsn(DUP); // <-- Extra 'mask' used with PUTFIELD
            mv.visitFieldInsn(GETFIELD, tpdMaskTpe, "m" + i, "J");
            mv.visitVarInsn(ALOAD, 0);
            mv.visitFieldInsn(GETFIELD, nodeTaskTpe, "_tpd" + i, "J");
            mv.visitInsn(LOR);
            mv.visitFieldInsn(PUTFIELD, tpdMaskTpe, "m" + i, "J");
          }
          mv.visitInsn(RETURN);
          Label endLabel = new Label();
          mv.visitLabel(endLabel);
          mv.visitLocalVariable("this", nodeTaskDesc, null, start, endLabel, 0);
          mv.visitLocalVariable("mask", tpdMaskDesc, null, start, endLabel, 1);
          mv.visitMaxs(5, 2);
          mv.visitEnd();
          this.mv = null; // Stop writing all the previous code....
        }
      };
    }
    if ("isTweakPropertyDependencySubsetOf".equals(name)) {
      return new MethodVisitor(ASM9, mv) {
        @Override
        public void visitLineNumber(int line, Label start) {
          super.visitLineNumber(line, start);
          if (mv == null) return;
          Label falseLabel = new Label();
          Label retLabel = new Label();
          for (int i = 0; i < DiagnosticSettings.tweakUsageQWords; ++i) {
            mv.visitVarInsn(ALOAD, 0);
            mv.visitFieldInsn(GETFIELD, nodeTaskTpe, "_tpd" + i, "J");
            mv.visitVarInsn(ALOAD, 1);
            mv.visitFieldInsn(GETFIELD, tpdMaskTpe, "m" + i, "J");
            mv.visitInsn(LOR);
            mv.visitVarInsn(ALOAD, 1);
            mv.visitFieldInsn(GETFIELD, tpdMaskTpe, "m" + i, "J");
            mv.visitInsn(LCMP);
            mv.visitJumpInsn(IFNE, falseLabel);
          }
          if (DiagnosticSettings.enablePerNodeTPDMask) {
            mv.visitInsn(ICONST_1); // return true
            mv.visitJumpInsn(GOTO, retLabel);

            mv.visitLabel(falseLabel);
            mv.visitFrame(F_SAME, 0, null, 0, null);
            mv.visitInsn(ICONST_0);

            mv.visitLabel(retLabel);
            mv.visitFrame(F_SAME1, 0, null, 1, new Object[] {INTEGER});
            mv.visitInsn(IRETURN);
          } else {
            mv.visitInsn(ICONST_1); // return true
            mv.visitInsn(IRETURN);
          }

          Label endLabel = new Label();
          mv.visitLabel(endLabel);
          mv.visitLocalVariable("this", nodeTaskDesc, null, start, endLabel, 0);
          mv.visitLocalVariable("mask", tpdMaskDesc, null, start, endLabel, 1);
          mv.visitMaxs(4, 2);
          mv.visitEnd();
          this.mv = null; // Stop writing all the previous code....
        }
      };
    }
    if ("tweakPropertyDependenciesIntersect".equals(name)) {
      return new MethodVisitor(ASM9, mv) {
        @Override
        public void visitLineNumber(int line, Label start) {
          super.visitLineNumber(line, start);
          if (mv == null) return;
          Label retTrue = new Label();
          Label retLabel = new Label();
          for (int i = 0; i < DiagnosticSettings.tweakUsageQWords; ++i) {
            mv.visitVarInsn(ALOAD, 0);
            mv.visitFieldInsn(GETFIELD, nodeTaskTpe, "_tpd" + i, "J");
            mv.visitVarInsn(ALOAD, 1);
            mv.visitFieldInsn(GETFIELD, tpdMaskTpe, "m" + i, "J");
            mv.visitInsn(LAND);
            mv.visitInsn(LCONST_0);
            mv.visitInsn(LCMP);
            mv.visitJumpInsn(IFNE, retTrue);
          }
          if (DiagnosticSettings.enablePerNodeTPDMask) {
            mv.visitInsn(ICONST_0); // return false
            mv.visitJumpInsn(GOTO, retLabel);

            mv.visitLabel(retTrue);
            mv.visitFrame(F_SAME, 0, null, 0, null);
            mv.visitInsn(ICONST_1);

            mv.visitLabel(retLabel);
            mv.visitFrame(F_SAME1, 0, null, 1, new Object[] {INTEGER});
            mv.visitInsn(IRETURN);
          } else {
            mv.visitInsn(ICONST_1); // return true
            mv.visitInsn(IRETURN);
          }

          Label endLabel = new Label();
          mv.visitLabel(endLabel);
          mv.visitLocalVariable("this", nodeTaskDesc, null, start, endLabel, 0);
          mv.visitLocalVariable("mask", tpdMaskDesc, null, start, endLabel, 1);
          mv.visitMaxs(4, 2);
          mv.visitEnd();
          this.mv = null; // Stop writing all the previous code....
        }
      };
    }
    return mv;
  }

  @Override
  public void visitEnd() {
    for (int i = 0; i < DiagnosticSettings.tweakUsageQWords; ++i) {
      cv.visitField(ACC_PRIVATE | ACC_TRANSIENT, "_tpd" + i, "J", null, null);
    }
    super.visitEnd();
  }
}
