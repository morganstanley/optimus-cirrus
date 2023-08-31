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
package com.ms.silverking.os.linux.proc;

import java.io.File;
import java.io.IOException;
import java.lang.reflect.Field;

import com.ms.silverking.io.StreamParser;

/**
 * Reads a single line file from /proc (e.g. /proc/<pid>/stat) into a corresponding Java class that
 * is effectively a C-style struct.
 */
public class StructReader<T> {
  private final Class<T> _class;

  public StructReader(Class<T> _class) {
    this._class = _class;
  }

  public T read(File file) throws IOException {
    return read(StreamParser.parseLine(file));
  }

  public T read(String def) {
    return read(def.split("\\s+"));
  }

  public T read(String[] tokens) {
    Field[] fields;
    int i;
    T newObject;

    fields = _class.getFields();
    i = 0;
    try {
      newObject = (T) _class.newInstance();
      while (i < tokens.length && i < fields.length) {
        String fieldType;

        fieldType = fields[i].getType().getName();
        try {
          if (fieldType.equals("char")) {
            fields[i].setChar(newObject, tokens[i].charAt(0));
          } else if (fieldType.equals("long")) {
            fields[i].setLong(newObject, Long.parseLong(tokens[i]));
          } else if (fieldType.equals("int")) {
            fields[i].setInt(newObject, Integer.parseInt(tokens[i]));
          } else if (fieldType.equals("java.lang.String")) {
            fields[i].set(newObject, tokens[i]);
          } else {
            throw new RuntimeException("Unsupported field type: " + fieldType);
          }
        } catch (NumberFormatException nfe) {
        }
        i++;
      }
      return newObject;
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  // unit testing
  public static void main(String[] args) {
    try {
      StructReader<ProcessStat> reader;

      reader = new StructReader<ProcessStat>(ProcessStat.class);
      reader.read(new File("/proc/1/stat"));
    } catch (Exception e) {
      e.printStackTrace();
    }
  }
}
