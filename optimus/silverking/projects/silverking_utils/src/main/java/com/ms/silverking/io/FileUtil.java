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
package com.ms.silverking.io;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.channels.FileChannel.MapMode;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;

import com.google.common.collect.ImmutableList;
import com.google.common.io.Files;

public class FileUtil {
  private static final String defaultTempFileSuffix = ".tmp";

  public static void writeToFileViaTemp(File file, byte[] value) throws IOException {
    File tempFile;

    tempFile = new File(file.getPath() + defaultTempFileSuffix);
    writeToFileViaTemp(file, tempFile, value);
  }

  public static void writeToFileViaTemp(File file, File tempFile, byte[] value) throws IOException {
    FileOutputStream out;

    out = new FileOutputStream(tempFile);
    try {
      out.write(value);
    } finally {
      out.close();
    }
    tempFile.renameTo(file);
  }

  public static void writeToFile(File file, byte[] value) throws IOException {
    FileOutputStream out;

    out = new FileOutputStream(file);
    try {
      out.write(value);
    } finally {
      out.close();
    }
  }

  public static void writeToFile(File file, byte[]... values) throws IOException {
    FileOutputStream out;

    out = new FileOutputStream(file);
    try {
      for (byte[] value : values) {
        out.write(value);
      }
    } finally {
      out.close();
    }
  }

  public static void writeToFile(File file, ByteBuffer value) throws IOException {
    FileOutputStream out;

    out = new FileOutputStream(file);
    try {
      out.getChannel().write(value);
    } finally {
      out.close();
    }
  }

  public static void writeToFile(File file, ByteBuffer... values) throws IOException {
    FileOutputStream out;

    out = new FileOutputStream(file);
    try {
      for (ByteBuffer value : values) {
        out.getChannel().write(value);
      }
    } finally {
      out.close();
    }
  }

  public static void writeToFile(String fileName, String text) throws IOException {
    writeToFile(new File(fileName), text);
  }

  public static void writeToFile(File file, String text) throws IOException {
    CoreFileUtil.writeToFile(file, text);
  }

  public static void writeToFile(String fileName, Collection lines) throws IOException {
    writeToFile(new File(fileName), lines);
  }

  public static void writeToFile(File file, Collection lines) throws IOException {
    try (BufferedWriter out = new BufferedWriter(new OutputStreamWriter(new FileOutputStream(file)))) {
      for (Object line : lines) {
        out.write(line.toString() + "\n");
      }
    }
  }

  public static File[] namesToFiles(String[] names) {
    File[] files;

    files = new File[names.length];
    for (int i = 0; i < files.length; i++) {
      files[i] = new File(names[i]);
    }
    return files;
  }

  public static byte[] readFileAsBytes(File file) throws IOException {
    return CoreFileUtil.readFileAsBytes(file);
  }

  public static int readFileAsInt(String file) throws IOException {
    return readFileAsInt(new File(file));
  }

  public static int readFileAsInt(File file) throws IOException {
    return Integer.parseInt(readFileAsString(file).trim());
  }

  public static String readFileAsString(String file) throws IOException {
    return readFileAsString(new File(file));
  }

  public static String readFileAsString(File file) throws IOException {
    return new String(readFileAsBytes(file));
  }

  public static List<String> readFileAsLineList(File file) throws IOException {
    String s;
    String[] lines;

    s = readFileAsString(file);
    lines = s.split("\\r?\\n");
    return ImmutableList.copyOf(lines);
  }

  public static void cleanDirectory(File dir) {
    for (File file : dir.listFiles()) {
      file.delete();
    }
  }

  public static List<File> listFilesRecursively(File path) {
    List<File> list;

    list = new ArrayList<>();
    listFilesRecursively(path, list);
    return list;
  }

  private static void listFilesRecursively(File path, List<File> list) {
    File[] files;

    files = path.listFiles();
    if (files != null) {
      for (File file : files) {
        list.add(file);
        if (file.isDirectory()) {
          listFilesRecursively(file, list);
        }
      }
    }
  }

  public static void copyDirectories(File src, File dest) {
    if (src.isFile()) {
      throw new RuntimeException("src needs to a be a directory");
    }

    for (File srcFile : src.listFiles()) {
      File destFile = new File(dest, srcFile.getName());
      if (srcFile.isDirectory()) {
        destFile.mkdir();
        copyDirectories(srcFile, destFile);
      } else {
        try {
          Files.copy(srcFile, destFile);
        } catch (IOException e) {
          throw new RuntimeException("couldn't copy " + srcFile + " to " + destFile, e);
        }
      }
    }
  }

  public static List<Long> numericFilesInDirAsSortedLongList(File dir) {
    List<Long> fileNumbers;
    String[] files;

    fileNumbers = new ArrayList<>();
    files = dir.list();
    if (files != null) {
      for (String file : files) {
        try {
          fileNumbers.add(Long.parseLong(file));
        } catch (NumberFormatException nfe) {
          // non-numeric filename -- skip it.
        }
      }
      Collections.sort(fileNumbers);
    }
    return fileNumbers;
  }

  public static List<Integer> numericFilesInDirAsSortedIntegerList(File dir) {
    List<Integer> fileNumbers;
    String[] files;

    fileNumbers = new ArrayList<>();
    files = dir.list();
    if (files != null) {
      for (String file : files) {
        try {
          fileNumbers.add(Integer.parseInt(file));
        } catch (NumberFormatException nfe) {
          // non-numeric filename -- skip it.
        }
      }
      Collections.sort(fileNumbers);
    }
    return fileNumbers;
  }

  public enum FileMapMode {PrivateMap, FileBackedMap_Writable, FileBackedMap_ReadOnly}

  public static MappedByteBuffer mapFile(File file, FileMapMode fileMapMode) throws IOException {
    return mapFile(new RandomAccessFile(file, fileMapMode == FileMapMode.FileBackedMap_Writable ? "rw" : "r"),
        fileMapMode, 0, file.length());
  }

  public static MappedByteBuffer mapFile(RandomAccessFile rf, FileMapMode fileMapMode, long position, long size)
      throws IOException {
    try {
      MappedByteBuffer mbb;
      FileChannel fc;

      fc = rf.getChannel();
      switch (fileMapMode) {
      case PrivateMap:
        mbb = fc.map(MapMode.PRIVATE, position, size);
        break;
      case FileBackedMap_Writable: // fall through
      case FileBackedMap_ReadOnly:
        mbb = fc.map(fileMapMode == FileMapMode.FileBackedMap_Writable ? MapMode.READ_WRITE : MapMode.READ_ONLY,
            position, size);
        break;
      default:
        throw new RuntimeException("panic");
      }
      return mbb;
    } finally {
      rf.close();
    }
  }
}
