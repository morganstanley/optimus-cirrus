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
import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.Random;
import org.slf4j.LoggerFactory;
import org.slf4j.Logger;

public class SparseFileCreator {
  private static final int bufLength = 1024 * 1024;
  private static final byte[] buf = new byte[bufLength];

  private static Logger log = LoggerFactory.getLogger(SparseFileCreator.class);

  static {
    new Random(0).nextBytes(buf);
  }

  private void write(RandomAccessFile raf, int length) throws IOException {
    int bytesToWrite;
    int totalWritten;

    totalWritten = 0;
    while (totalWritten < length) {
      bytesToWrite = Math.min(buf.length, length - totalWritten);
      raf.write(buf, 0, bytesToWrite);
      totalWritten += bytesToWrite;
    }
  }

  public void createSparseFile(
      File f, int headerLength, int skipLength, int tailLength, int finalLength)
      throws IOException {
    RandomAccessFile raf;

    raf = new RandomAccessFile(f, "rw");
    write(raf, headerLength);
    raf.seek(raf.getFilePointer() + skipLength);
    write(raf, tailLength);
    if (finalLength > 0) {
      raf.setLength(finalLength);
    }
  }

  public static void main(String[] args) {
    if (args.length != 4 && args.length != 5) {
      log.error("args: <file> <headerLength> <skipLength> <tailLength> [finalLength]");
    } else {
      try {
        SparseFileCreator sfc;
        String file;
        int headerLength;
        int skipLength;
        int tailLength;
        int finalLength;

        file = args[0];
        headerLength = Integer.parseInt(args[1]);
        skipLength = Integer.parseInt(args[2]);
        tailLength = Integer.parseInt(args[3]);
        if (args.length == 5) {
          finalLength = Integer.parseInt(args[4]);
        } else {
          finalLength = 0;
        }
        sfc = new SparseFileCreator();
        sfc.createSparseFile(new File(file), headerLength, skipLength, tailLength, finalLength);
      } catch (Exception e) {
        e.printStackTrace();
      }
    }
  }
}
