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
package com.ms.silverking.compression;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;

import com.ms.silverking.text.StringUtil;
import org.apache.commons.compress.compressors.bzip2.BZip2CompressorInputStream;
import org.apache.commons.compress.compressors.bzip2.BZip2CompressorOutputStream;
import org.slf4j.LoggerFactory;
import org.slf4j.Logger;

public class BZip2 implements Compressor, Decompressor {
  private static final int bzip2InitFactor = 10;

  public BZip2() {
  }

  private static Logger log = LoggerFactory.getLogger(BZip2.class);

  public byte[] compress(byte[] rawValue, int offset, int length) throws IOException {
    ByteArrayOutputStream baos;
    BZip2CompressorOutputStream bzip2os;

    byte[] buf;
    int compressedLength;

    //Log.info("rawValue.length ", rawValue.length);
    baos = new ByteArrayOutputStream(rawValue.length / bzip2InitFactor);
    baos.write(0x42);
    baos.write(0x5a);
    bzip2os = new BZip2CompressorOutputStream(baos);
    bzip2os.write(rawValue, offset, length);
    bzip2os.flush();
    bzip2os.close();

    baos.flush();
    baos.close();
    buf = baos.toByteArray();
    //System.out.println(StringUtil.byteArrayToHexString(buf));

    compressedLength = buf.length;
    //Log.info("compressedLength ", compressedLength);

    if (log.isDebugEnabled()) {
      log.debug("rawValue.length: {}", rawValue.length);
      log.debug("buf.length: {}", buf.length);
      log.debug("compressedLength: {}", compressedLength);
    }
    //System.arraycopy(md5, 0, buf, compressedLength, md5.length);

    //Log.info("buf.length ", buf.length);
    //System.out.println("\t"+ StringUtil.byteArrayToHexString(buf));
    return buf;
  }

  public byte[] decompress(byte[] value, int offset, int length, int uncompressedLength) throws IOException {
    BZip2CompressorInputStream bzip2is;
    InputStream inStream;
    byte[] uncompressedValue;

    //System.out.println(value.length +" "+ offset +" "+ length);
    //System.out.println(StringUtil.byteArrayToHexString(value, offset, length));
    uncompressedValue = new byte[uncompressedLength];
    inStream = new ByteArrayInputStream(value, offset, length);
    try {
      int b;

      b = inStream.read();
      if (b != 'B') {
        throw new IOException("Invalid bzip2 value");
      }
      b = inStream.read();
      if (b != 'Z') {
        throw new IOException("Invalid bzip2 value");
      }
      bzip2is = new BZip2CompressorInputStream(inStream);
      try {
        int totalRead;

        totalRead = 0;
        do {
          int numRead;

          numRead = bzip2is.read(uncompressedValue, totalRead, uncompressedLength - totalRead);
          if (numRead < 0) {
            throw new RuntimeException("panic");
          }
          totalRead += numRead;
        } while (totalRead < uncompressedLength);
        return uncompressedValue;
      } finally {
        bzip2is.close();
      }
    } finally {
      inStream.close();
    }
  }

  // for unit testing only
  public static void main(String[] args) {
    try {
      for (String arg : args) {
        byte[] original;
        byte[] compressed;
        byte[] uncompressed;

        original = arg.getBytes();
        compressed = new BZip2().compress(original, 0, 0);
        uncompressed = new BZip2().decompress(compressed, 0, 0, original.length);
        log.info("{}  {}  {}  {}",arg ,original.length ,compressed.length, new String(uncompressed));
        log.info(StringUtil.byteArrayToHexString(original));
        log.info(StringUtil.byteArrayToHexString(uncompressed));
      }
    } catch (Exception e) {
      e.printStackTrace();
    }
  }
}