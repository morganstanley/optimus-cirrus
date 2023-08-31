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
package com.ms.silverking.fs.test;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.List;
import org.slf4j.LoggerFactory;
import org.slf4j.Logger;

public class FileWriter {

  private final File file;
  private final long size;
  private final int bufferSize;
  private final byte[] buffer;

  private static Logger log = LoggerFactory.getLogger(FileWriter.class);

  public FileWriter(File file, long size) {
    this.file = file;
    this.size = size;

    bufferSize = 64 * 1024;
    buffer = new byte[bufferSize];
    fillBuffer(buffer);
  }

  public void write() throws IOException {
    OutputStream out = new FileOutputStream(file);
    long totalBytesWritten = 0;

    do {
      int bytesToWrite = (int) Math.min(size - totalBytesWritten, bufferSize);
      out.write(buffer, 0, bytesToWrite);
      totalBytesWritten += bytesToWrite;
    } while (totalBytesWritten < size);
    out.close();
  }

  public List<byte[]> read() throws IOException {
    InputStream out = new FileInputStream(file);
    long totalBytesRead = 0;

    List<byte[]> bytesRead = new ArrayList<>();

    do {
      int bytesToRead = (int) Math.min(size - totalBytesRead, bufferSize);
      byte[] readBuffer = new byte[bytesToRead];
      out.read(readBuffer, 0, bytesToRead);
      bytesRead.add(readBuffer);
      totalBytesRead += bytesToRead;
    } while (totalBytesRead < size);
    out.close();

    return bytesRead;
  }

  //    public StringBuffer read2() throws IOException {
  //        InputStream out = new FileInputStream(file);
  //        long totalBytesRead = 0;
  //
  //        StringBuffer bytesRead = new StringBuffer();
  //
  //        do {
  //            int    bytesToRead = (int)Math.min(size - totalBytesRead, bufferSize);
  //            byte[] readBuffer = new byte[bytesToRead];
  //            out.read(readBuffer, 0, bytesToRead);
  //            bytesRead.append( createToString(readBuffer) );
  //            totalBytesRead += bytesToRead;
  //        } while (totalBytesRead < size);
  //        out.close();
  //
  //        return bytesRead;
  //     }
  //
  //    public <T> String createToString(T... elements) {
  //        return Arrays.deepToString(elements);
  //    }

  public int getBufferSize() {
    return bufferSize;
  }

  public static void fillBuffer(byte[] buffer) {
    for (int i = 0; i < buffer.length; i++) {
      buffer[i] = (byte) i;
    }
  }

  public static void main(String[] args) {
    try {
      if (args.length != 1 && args.length != 2) {
        log.info("args: <file> <size>");
      } else {
        File file = new File(args[0]);
        long size = Long.parseLong(args[1]);

        FileWriter fw = new FileWriter(file, size);
        fw.write();
        fw.read();
      }
    } catch (Exception e) {
      e.printStackTrace();
    }
  }
}
