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
package optimus.graph.diagnostics.messages;

import static org.objectweb.asm.ClassWriter.COMPUTE_FRAMES;

import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.lang.reflect.Parameter;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.HashMap;

import optimus.DynamicClassLoader;
import org.objectweb.asm.ClassWriter;
import org.objectweb.asm.Label;
import org.objectweb.asm.MethodVisitor;
import org.objectweb.asm.Opcodes;
import org.objectweb.asm.Type;

/**
 * Used to generate writer and reader for profiler events. The API for events is defined in
 * optimus.graph.diagnostics.messages.ProfilerMessages. These APIs are called in profiling code to
 * write events to byte buffers (we have one OGTraceStore.Table per thread, and this is a wrapper
 * around ByteBuffer).
 *
 * <p>==== WRITER ==== The writer directly extends ProfilerMessages and the generator provides
 * implementations for each event type based on the signature defined in the API. We generate a
 * class called ProfilerMessagesWriter_GEN, where we override APIs.
 *
 * <p>For example, for the following API: public void enterGraph(long time) {}
 *
 * <p>We define bytecode, based on the parameter list, for:
 *
 * <p>override public void enterGraph(long time) { prefix(size, eventId); putLong(time); suffix(); }
 *
 * <p>Where "prefix", "putLong" (and other "put" methods for each type required) and "suffix" are
 * defined as final methods and implemented in
 * optimus.graph.diagnostics.messages.ProfilerMessagesWriter. The method signature to eventId
 * mapping is defined in the generator itself, and used again in the reader for parsing events.
 *
 * <p>Note - future work will support different versions of the writer and reader, but the
 * implementation currently depends on the same APIs for events defined by both the writer and
 * reader.
 *
 * <p>==== READER ==== The reader has a ProfilerMessages member (processor), which we use to
 * generate a switch statement on eventId in the dispatchEvent method. We generate a class
 * ProfilerMessagesReader_GEN, and for each of the eventIds, we generate bytecode for the following,
 * based on the ProfilerMessages APIs:
 *
 * <p>switch (eventId) { case 1: readJ(); // enterGraph(long time) break; // other eventIds go here
 * default: break; }
 *
 * <p>Implementation notes (mostly for my own reference): [1] COMPUTE_FRAMES is required for Java
 * 1.7+ since we don't want compute that -- note that this implies COMPUTE_MAXS, so doesn't matter
 * what parameters we pass to visitMaxs at the end of the method visit (they'll be ignored and
 * recomputed)
 *
 * <p>[2] Special handling for readString (so we don't generate a method called
 * readLjava/lang/String, but readString) [3] ProfilerMessages is NOT actually an interface, it's an
 * abstract class, but it defines the APIs [4] See https://asm.ow2.io/asm4-guide.pdf - the call to
 * visitMaxs must be done after all the instructions have been visited. Used to define sizes of
 * local variables and operand stack parts for the execution frame of this method.
 */
public class ProfilerMessagesGenerator implements Opcodes {
  private final Signature signature = new Signature();
  private final Class<?> interfaceClass;
  private final DynamicClassLoader clsLoader;
  private String baseClassName;
  private final String interfaceClassName;
  // only needs to change if new bytecode instructions introduced
  private static final int JAVA_VERSION = V11;

  private static final HashMap<Signature, ProfilerMessagesGenerator> cacheGen = new HashMap<>();
  private static final HashMap<ProfilerMessagesGenerator, ProfilerMessagesReader> cacheReaders =
      new HashMap<>();
  private static final HashMap<ProfilerMessagesGenerator, ProfilerEventsWriter> cacheWriters =
      new HashMap<>();

  public Signature getSignature() {
    return signature;
  }

  public String[] getNames() {
    return this.signature.getNames();
  }

  public String[] getDescriptors() {
    return this.signature.getDescriptors();
  }

  private ProfilerMessagesGenerator(Class<?> interfaceClass) {
    this.interfaceClass = interfaceClass;
    this.interfaceClassName = Type.getInternalName(interfaceClass);
    this.clsLoader = new DynamicClassLoader(interfaceClass);
  }

  public static ProfilerMessagesGenerator create(Class<?> interfaceClass) {
    return new ProfilerMessagesGenerator(interfaceClass);
  }

  public static ProfilerMessagesGenerator createFromBytes(
      Signature signature, Class<?> interfaceClass) {
    return cacheGen.computeIfAbsent(
        signature,
        signature1 -> {
          var gen = new ProfilerMessagesGenerator(interfaceClass);
          gen.signature.setNames(signature.getNames());
          gen.signature.setDescriptors(signature.getDescriptors());
          return gen;
        });
  }

  public Object createWriterObject(Class<?> baseClass) {
    return cacheWriters.computeIfAbsent(
        this,
        _this -> {
          this.baseClassName = Type.getInternalName(baseClass);
          return (ProfilerEventsWriter) clsLoader.createInstance(createWriter());
        });
  }

  public Object createReaderObject(Class<?> baseClass) {
    return cacheReaders.computeIfAbsent(
        this,
        _this -> {
          this.baseClassName = Type.getInternalName(baseClass);
          var reader = (ProfilerMessagesReader) clsLoader.createInstance(createReader());
          if (reader != null) reader.eventCount = getEventCount();
          return reader;
        });
  }

  public int getEventCount() {
    return signature.getNames().length;
  }

  private ClassWriter createClassWriter() {
    ClassWriter cw = new ClassWriter(COMPUTE_FRAMES); // see note [1]
    cw.visit(
        JAVA_VERSION, ACC_FINAL | ACC_PUBLIC, baseClassName + "_GEN", null, baseClassName, null);
    generateCtor(cw);
    return cw;
  }

  private void generateCtor(ClassWriter cw) {
    MethodVisitor mv = cw.visitMethod(ACC_PUBLIC, "<init>", "()V", null, null);
    mv.visitCode();
    mv.visitVarInsn(ALOAD, 0);
    mv.visitMethodInsn(INVOKESPECIAL, baseClassName, "<init>", "()V", false);
    mv.visitInsn(RETURN);
    mv.visitMaxs(1, 1);
    mv.visitEnd();
  }

  private byte[] createReader() {
    ClassWriter cw = createClassWriter();
    MethodVisitor mv = cw.visitMethod(ACC_PUBLIC, "dispatchEvent", "(I)V", null, null);
    mv.visitCode();
    mv.visitVarInsn(ILOAD, 1);
    String[] names = signature.getNames();
    String[] descriptors = signature.getDescriptors();

    Label[] labels = new Label[names.length];
    for (int i = 0; i < names.length; i++) labels[i] = new Label();
    Label defLabel = new Label();
    mv.visitTableSwitchInsn(1, names.length, defLabel, labels);

    for (int i = 0; i < names.length; i++)
      generateReaderMethod(mv, names[i], descriptors[i], labels[i], defLabel);

    mv.visitLabel(defLabel);
    mv.visitInsn(RETURN);
    mv.visitMaxs(1, 1);
    mv.visitEnd();

    generateEventToStringMethod(cw);

    cw.visitEnd();
    return cw.toByteArray();
  }

  private void generateEventToStringMethod(ClassWriter cw) {
    Type tpeString = Type.getType(String.class);
    Type tpeInt = Type.getType(int.class);
    String descriptor = Type.getMethodDescriptor(tpeString, tpeInt);
    MethodVisitor mv = cw.visitMethod(ACC_PUBLIC, "nameOfEvent", descriptor, null, null);
    mv.visitCode();
    mv.visitVarInsn(ILOAD, 1);
    String[] names = signature.getNames();

    Label[] labels = new Label[names.length];
    for (int i = 0; i < names.length; i++) labels[i] = new Label();
    Label defLabel = new Label();
    mv.visitTableSwitchInsn(1, names.length, defLabel, labels);

    for (int i = 0; i < names.length; i++) {
      mv.visitLabel(labels[i]);
      mv.visitLdcInsn(names[i]);
      mv.visitInsn(ARETURN);
    }

    mv.visitLabel(defLabel);
    mv.visitLdcInsn("[event not found]");
    mv.visitInsn(ARETURN);
    mv.visitMaxs(1, 1);
    mv.visitEnd();
  }

  private void generateReaderMethod(
      MethodVisitor mv, String name, String descriptor, Label label, Label defaultLabel) {
    mv.visitLabel(label);
    mv.visitVarInsn(ALOAD, 0); // this pointer

    String processorDescr = "L" + interfaceClassName + ";";
    mv.visitFieldInsn(GETFIELD, baseClassName + "_GEN", "processor", processorDescr);

    Type[] parameters = Type.getArgumentTypes(descriptor);
    for (Type ptype : parameters) {
      String pdescr = ptype.getDescriptor();
      int lastSlash = pdescr.lastIndexOf('/'); // see note [2]
      String methodSuffix = lastSlash > 0 ? ptype.getInternalName().substring(lastSlash) : pdescr;
      methodSuffix = methodSuffix.replace("[", "Array"); // [SEE_GENERATED_READ]
      mv.visitVarInsn(ALOAD, 0); // this pointer
      mv.visitMethodInsn(INVOKESPECIAL, baseClassName, "read" + methodSuffix, "()" + pdescr, false);
    }
    mv.visitMethodInsn(INVOKEVIRTUAL, interfaceClassName, name, descriptor, false);
    mv.visitJumpInsn(GOTO, defaultLabel); // break (AFTER all methods)
  }

  public static String generateKey(String[] nameStr) {
    return String.join("", nameStr);
  }

  private byte[] createWriter() {
    ArrayList<String> names = new ArrayList<>();
    ArrayList<String> descriptors = new ArrayList<>();
    ClassWriter cw = createClassWriter();

    var declaredMethods = interfaceClass.getDeclaredMethods();
    var comparator = Comparator.comparing(Method::getName).thenComparing(Method::getParameterCount);
    Arrays.sort(declaredMethods, comparator);
    for (Method method : declaredMethods) { // see note [3]
      if (!Modifier.isFinal(method.getModifiers()) && method.getReturnType() == void.class) {
        String descriptor = Type.getMethodDescriptor(method);
        names.add(method.getName());
        descriptors.add(descriptor);
        int eventID = names.size();
        generateWriterMethod(cw, method, eventID, descriptor);
      }
    }
    this.signature.setNames(names.toArray(new String[0]));
    this.signature.setDescriptors(descriptors.toArray(new String[0]));
    cw.visitEnd();
    return cw.toByteArray();
  }

  private void generateWriterMethod(ClassWriter cw, Method method, int eventID, String descriptor) {
    Parameter[] parameters = method.getParameters();
    int[] slots = new int[parameters.length];
    Type[] types = new Type[parameters.length];
    MethodVisitor mv = cw.visitMethod(ACC_PUBLIC, method.getName(), descriptor, null, null);
    mv.visitCode();
    mv.visitVarInsn(ALOAD, 0); // this pointer for the 'prefix' call!

    Type tpeString = Type.getType(String.class);
    Type tpeByteArray = Type.getType(byte[].class);

    int slot = 1;
    int localsStartAt = Type.getArgumentsAndReturnSizes(descriptor);
    int extraSlot = 0;
    int size = 0;
    for (int i = 0; i < parameters.length; i++) {
      Parameter parameter = parameters[i];
      Type ptype = Type.getType(parameter.getType());
      types[i] = ptype;
      slots[i] = slot;
      switch (ptype.getSort()) {
        case Type.INT:
          size += 4;
          break;
        case Type.LONG:
          size += 8;
          break;
        case Type.BOOLEAN:
          size += 4;
          break;
        case Type.ARRAY:
          if (ptype.getElementType().getSort() != Type.BYTE)
            throw new IllegalArgumentException("Only supported for byte arrays");
          // no break, fall through to string handling
        case Type.OBJECT:
          size += 4; // for the size and will add the actual byte size later
          mv.visitVarInsn(ALOAD, 0); // this pointer
          mv.visitVarInsn(ALOAD, slot);
          var getBytesDescriptor = Type.getMethodDescriptor(tpeByteArray, ptype);
          mv.visitMethodInsn(INVOKESPECIAL, baseClassName, "getBytes", getBytesDescriptor, false);
          mv.visitInsn(DUP);

          boolean addToCounter = false;
          if (extraSlot != 0) addToCounter = true;
          else extraSlot = localsStartAt;

          mv.visitVarInsn(ASTORE, extraSlot);
          mv.visitInsn(ARRAYLENGTH); // length
          if (addToCounter) mv.visitInsn(IADD); // Add to the accumulator
          slots[i] = extraSlot;
          types[i] = tpeByteArray;
          extraSlot++;
          break;
        default:
          throw new Error();
      }
      slot +=
          ptype.getSize(); // 2 slots for double/long, 1 for everything else, already accounted for
      // prefix
    }

    mv.visitIntInsn(SIPUSH, size);
    if (extraSlot != 0) mv.visitInsn(IADD); // Add to the accumulator

    mv.visitIntInsn(SIPUSH, eventID);
    mv.visitMethodInsn(INVOKESPECIAL, baseClassName, "prefix", "(II)V", false);

    for (int i = 0; i < types.length; i++) {
      Type ptype = types[i];
      mv.visitVarInsn(ALOAD, 0); // this pointer
      mv.visitVarInsn(ptype.getOpcode(ILOAD), slots[i]);
      String writeDescriptor = "(" + ptype.getDescriptor() + ")V";
      mv.visitMethodInsn(INVOKESPECIAL, baseClassName, "write", writeDescriptor, false);
    }
    mv.visitVarInsn(ALOAD, 0); // this pointer
    mv.visitMethodInsn(INVOKESPECIAL, baseClassName, "suffix", "()V", false);

    mv.visitInsn(RETURN);
    mv.visitMaxs(1, 1); // see note [4]
    mv.visitEnd();
  }
}
