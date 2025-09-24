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
package optimus.graph.loom.compiler;

import static optimus.debug.CommonAdapter.isStatic;
import static optimus.debug.CommonAdapter.makePrivate;
import static optimus.debug.InstrumentationConfig.OGSC_TYPE;
import static optimus.graph.loom.NameMangler.mkLoomName;
import static optimus.graph.loom.NameMangler.mkPlainName;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import optimus.BiopsyLab;
import optimus.graph.DiagnosticSettings;
import optimus.graph.loom.LoomAdapter;
import optimus.graph.loom.TransformableMethod;
import org.objectweb.asm.Opcodes;
import org.objectweb.asm.Type;
import org.objectweb.asm.tree.AbstractInsnNodeEx;
import org.objectweb.asm.tree.InsnList;
import org.objectweb.asm.tree.InsnNode;
import org.objectweb.asm.tree.JumpInsnNode;
import org.objectweb.asm.tree.LabelNode;
import org.objectweb.asm.tree.MethodInsnNode;
import org.objectweb.asm.tree.MethodNode;
import org.objectweb.asm.tree.VarInsnNode;

/**
 * The Loom Compiler, organized in several phases. It also contain code to easy code debugging,
 * which are triggered by manually setting @compiler(debug = true).
 *
 * <p>For some ideas see org.objectweb.asm.MethodWriter
 */
public class LCompiler implements Opcodes {

  private static final String lcompilerConfig = "lcompiler.config";
  private static final HashMap<String, String> config = new HashMap<>();
  private static final String searchMode;

  /** Pops nothing but not available! */
  static final int POP_NOTHING_NA = -1;

  static final int POP_NOTHING_BUT_READS = -2;
  private final LoomAdapter adapter;

  private final TransformableMethod tmethod;

  static {
    searchMode = DiagnosticSettings.getStringProperty("lcompiler.search", "");
    try {
      if (!searchMode.isEmpty()) {
        Runtime.getRuntime()
            .addShutdownHook(
                new Thread(
                    () -> {
                      System.err.println("Saving config....");
                      var sb = new StringBuilder();
                      var cfgSorted =
                          config.entrySet().stream()
                              .sorted(Map.Entry.comparingByKey())
                              .collect(Collectors.toList());
                      for (var e : cfgSorted) {
                        sb.append(e.getValue());
                        sb.append(" ");
                        sb.append(e.getKey());
                        sb.append("\n");
                      }
                      dumpToFile(lcompilerConfig, sb.toString());
                    }));
      }
      var file = Paths.get(lcompilerConfig);
      if (Files.exists(file)) {
        var lines = Files.readAllLines(file);
        int enabled = 0;
        int cleared = 0; // Anything that was enabled and passed is 'clear'
        int disabled = 0;
        for (var l : lines)
          if (l.startsWith("E ")) enabled++;
          else if (l.startsWith("D ")) disabled++;
          else if (l.startsWith("C ")) cleared++;
          else System.err.println("Just reading the config...");

        System.err.println(
            "Last run: Cleared=" + cleared + " Enabled=" + enabled + " Disabled=" + disabled);
        if (searchMode.equals("F")) {
          // Last run was a failure disable 1/2 of failed entries
          var disable = enabled / 2;
          System.err.println("Disabling " + disable + " methods, enabled: " + (enabled - disable));
          loadConfigFrom(lines, disable, searchMode);
        } else if (searchMode.equals("S")) {
          // Last run was a success enable 1/2 of disabled entries
          var enable = disabled / 2;
          System.err.println("Remaining disabled " + (disabled - enable) + " methods");
          loadConfigFrom(lines, enable, searchMode);
        } else {
          loadConfigFrom(lines, 0, searchMode);
        }
      }
    } catch (IOException e) {
      //noinspection CallToPrintStackTrace
      e.printStackTrace();
    }
  }

  private static void loadConfigFrom(List<String> lines, int change, String mode) {
    for (var l : lines) {
      var values = l.split(" ");
      var flag = values[0];
      var method = values[1];
      if (mode.equals("S")) {
        if (flag.equals("E")) config.putIfAbsent(method, "C");
        else if (flag.equals("D") && change > 0) {
          change--;
          config.putIfAbsent(values[1], "E");
        } else config.putIfAbsent(method, flag);
      } else if (mode.equals("F")) {
        if (flag.equals("D")) config.putIfAbsent(method, "C");
        if (flag.equals("E") && change > 0) {
          change--;
          config.putIfAbsent(method, "D");
        } else config.putIfAbsent(method, flag);
      } else {
        config.putIfAbsent(method, flag);
      }
    }
  }

  public LCompiler(TransformableMethod tmethod, LoomAdapter adapter) {
    this.tmethod = tmethod;
    this.adapter = adapter;
  }

  public static void transform(TransformableMethod method, LoomAdapter adapter, boolean suffix) {
    /*
    // example on how to debug a method by name (without re-compiling):
    if (adapter.cls.name.endsWith("MapLikeDSI")) {
      if (between(method.id, 26, 27)) {
        System.err.println("Skipping: " + method.method.name);
        method.compilerArgs.debug = true;
        method.compilerArgs.skipFoldingBlocks = true;
        return;
      } else {
        System.err.println("Transforming: " + method.method.name);
      }
    }*/

    // Short circuit re-compile
    if (method.compilerArgs.level <= 0 || method.asyncOnly) return;
    // Previously detected in optimus.graph.loom.LoomAdapter.enrichMethod
    if (method.unsafeToReorder || !method.hasNodeCalls) return;
    // we cannot rearrange node methods using try catches!
    if (!method.method.tryCatchBlocks.isEmpty()) return;

    var fullMethodName = adapter.cls.name + "." + method.method.name;
    var configFlag = config.getOrDefault(fullMethodName, "N");
    if (configFlag.equals("D") || configFlag.equals("C")) return;
    else {
      // LMessage.info("Enabled: ", method, adapter.cls);
      if (!searchMode.isEmpty()) config.putIfAbsent(fullMethodName, "E");
    }

    var lc = new LCompiler(method, adapter);
    if (method.compilerArgs.queueSizeSensitive) lc.writePlainFunc();
    dumpToFile("1-before.txt", method);

    try {
      var state = lc.parse();

      // parser decided not to transform?
      if (state.isResultAvailable()) {
        lc.dumpDotGraphToFile(state, "dotgraph.md");
        lc.dumpDotGraphToFile(state, "control-flow.md", true);
        // The order here is important!

        // if CompilerArgs.usePostOrder, it also rearranges the blocks in reverse rpo order
        lc.computeDOM(state);
        lc.reduce(state);
        lc.dumpDotGraphToFile(state, "dotgraph-reduced.md");
        lc.regen(state);
      }

      // regen failed?
      if (state.isResultAvailable()) {
        var instructions = new InsnList();
        var result = state.getResult();

        LabelNode labelNeedsFrame;
        if (method.compilerArgs.queueSizeSensitive) {
          labelNeedsFrame = (LabelNode) result.get(0);
          lc.writeSwitchToPlain(instructions, labelNeedsFrame);
        }

        for (var insn : result) {
          AbstractInsnNodeEx.clear(insn);
          instructions.add(insn);
        }
        instructionsInvariantsCheck(state, instructions);
        method.method.instructions = instructions;
        var mangledN = adapter.mangledClsName;
        var eventualName = suffix ? mkLoomName(mangledN, method.method.name) : method.method.name;
        adapter.registerForFrameCompute(eventualName, method.method.desc);
        dumpToFile("2-after.txt", method);
      }
    } catch (Throwable ex) {
      ex.printStackTrace();
    }
  }

  private static boolean between(int v, int low, int top) {
    return v >= low && v < top;
  }

  DCFlowGraph parse() {
    var parser = new Parser(tmethod, adapter);
    return parser.parse();
  }

  void computeDOM(DCFlowGraph state) {
    var calculator = new DOMCalculator(state);
    calculator.computeDOM();
  }

  void reduce(DCFlowGraph state) {
    var reducer = new BlockFolding(state);
    reducer.reduce();
  }

  void regen(DCFlowGraph state) {
    var regenerator = new Regenerator(state);
    regenerator.regen();
  }

  private static void instructionsInvariantsCheck(DCFlowGraph state, InsnList instructions) {
    if (state.compilerArgs.debug) {
      var insnsArr = instructions.toArray();
      var result = state.getResult();
      for (int i = 0; i < insnsArr.length; i++) {
        if (result.get(i) != insnsArr[i]) LMessage.fatal("Instruction changed @" + i);
      }
      if (result.size() != insnsArr.length) LMessage.fatal("Lost instructions!");
    }
  }

  private void writeSwitchToPlain(InsnList instructions, LabelNode firstLabel) {
    var method = tmethod.method;
    instructions.add(new MethodInsnNode(INVOKESTATIC, OGSC_TYPE, "hasEnoughWork", "()Z"));
    instructions.add(new JumpInsnNode(IFEQ, firstLabel));
    var hasThis = !isStatic(method.access);
    if (hasThis) instructions.add(new VarInsnNode(ALOAD, 0));
    var paramTypes = Type.getArgumentTypes(method.desc);
    var offset = hasThis ? 1 : 0;
    for (Type paramType : paramTypes) {
      instructions.add(new VarInsnNode(paramType.getOpcode(ILOAD), offset));
      offset += paramType.getSize();
    }
    var name = mkPlainName(adapter.mangledClsName, method.name);
    instructions.add(new MethodInsnNode(INVOKESPECIAL, adapter.cls.name, name, method.desc));
    var returnType = Type.getReturnType(method.desc);
    instructions.add(new InsnNode(returnType.getOpcode(IRETURN)));
  }

  private void writePlainFunc() {
    var method = tmethod.method;
    var access = makePrivate(method.access);
    var plainName = mkPlainName(adapter.mangledClsName, method.name);
    String[] exceptions = method.exceptions.toArray(new String[0]);
    var plainMethod = new MethodNode(access, plainName, method.desc, method.signature, exceptions);
    method.accept(plainMethod);
    for (var op : plainMethod.instructions) {
      if (op instanceof MethodInsnNode) {
        var call = (MethodInsnNode) op;
        if (call.name.equals(method.name) && call.owner.equals(adapter.cls.name))
          call.name = plainName;
      }
    }
    plainMethod.visibleAnnotations = null;
    plainMethod.invisibleAnnotations = null;
    plainMethod.parameters = null;
    adapter.cls.methods.add(plainMethod);
  }

  /**
   * It dumps the bytecode in a file in the current directory, which is extremely useful when
   * debugging.
   */
  private static void dumpToFile(String file, TransformableMethod method) {
    if (method.compilerArgs.debug) dumpToFile(file, BiopsyLab.byteCodeAsString(method.method));
  }

  private static void dumpToFile(String file, String content) {
    try {
      Files.writeString(Paths.get(file), content);
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  private void dumpDotGraphToFile(DCFlowGraph state, String fileName) {
    dumpDotGraphToFile(state, fileName, false);
  }

  /**
   * It dumps the dependencies representation in a file in the current directory, which is extremely
   * useful when debugging.
   */
  private void dumpDotGraphToFile(DCFlowGraph state, String fileName, boolean minimal) {
    if (state.compilerArgs.debug) {
      var newLine = System.lineSeparator();
      var methodId = adapter.cls.name + "." + tmethod.method.name;
      var graph = LInsnDiGraphWriter.toString(state.getStartBlock(), methodId, minimal);
      var dotgraph = String.join(newLine, "```dot", graph, "```");
      dumpToFile(fileName, dotgraph);
    }
  }
}
