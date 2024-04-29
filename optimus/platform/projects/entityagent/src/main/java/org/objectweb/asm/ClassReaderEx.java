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
package org.objectweb.asm;

public class ClassReaderEx extends ClassReader {

  public ClassReaderEx(byte[] classFile) {
    super(classFile);
  }

  public boolean hasAnnotation(String annotation) {
    char[] charBuffer = new char[getMaxStringLength()];
    int currentAttributeOffset = getFirstAttributeOffset();
    for (int i = readUnsignedShort(currentAttributeOffset - 2); i > 0; --i) {
      // Read the attribute_info's attribute_name and attribute_length fields.
      String attributeName = readUTF8(currentAttributeOffset, charBuffer);
      int attributeLength = readInt(currentAttributeOffset + 2);
      currentAttributeOffset += 6;
      if (Constants.RUNTIME_INVISIBLE_ANNOTATIONS.equals(attributeName)) {
        return searchForAnnotation(annotation, currentAttributeOffset, charBuffer);
      }
      currentAttributeOffset += attributeLength;
    }
    return false;
  }

  boolean searchForAnnotation(String annotation, int annotationsOffset, char[] charBuffer) {
    int numAnnotations = readUnsignedShort(annotationsOffset);
    int currentOffset = annotationsOffset + 2;

    while (numAnnotations-- > 0) {
      // Parse the type_index field.
      String cAnnotation = readUTF8(currentOffset, charBuffer);
      currentOffset += 2;
      if (annotation.equals(cAnnotation)) {
        return true;
      }

      currentOffset = skipElementValues(currentOffset);
    }
    return false;
  }

  private int skipElementValues(int currentOffset) {
    int numElementValuePairs = readUnsignedShort(currentOffset);
    currentOffset += 2;
    while (numElementValuePairs-- > 0) {
      currentOffset = skipElementValue(currentOffset + 2);
    }
    return currentOffset;
  }

  private int skipElementValue(int currentOffset) {
    int type = classFileBuffer[currentOffset] & 0xFF;
    switch (type) {
      case 'e': // enum_const_value
        return currentOffset + 5;
      case '@': // annotation_value
        return skipElementValues(currentOffset + 3);
      case '[': // array_value
        return skipElementValues(currentOffset + 1);
      default:
        return currentOffset + 3;
    }
  }
}
