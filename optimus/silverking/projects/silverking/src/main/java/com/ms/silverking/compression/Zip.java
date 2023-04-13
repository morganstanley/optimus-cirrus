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
import java.util.zip.DeflaterOutputStream;
import java.util.zip.InflaterInputStream;

import com.ms.silverking.text.StringUtil;
import org.slf4j.LoggerFactory;
import org.slf4j.Logger;

public class Zip implements Compressor, Decompressor {
  private static final int zipInitFactor = 10;

  private static Logger log = LoggerFactory.getLogger(Zip.class);

  public byte[] compress(byte[] rawValue, int offset, int length) throws IOException {
    ByteArrayOutputStream baos;
    DeflaterOutputStream zipos;
    byte[] buf;
    int compressedLength;

    //Log.info("rawValue.length ", rawValue.length);
    baos = new ByteArrayOutputStream(rawValue.length / zipInitFactor);
    zipos = new DeflaterOutputStream(baos);
    zipos.write(rawValue, offset, length);
    zipos.flush();
    zipos.close();

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
    //Log.info("buf.length ", buf.length);
    return buf;
  }

  public byte[] decompress(byte[] value, int offset, int length, int uncompressedLength) throws IOException {
    InflaterInputStream zipis;
    InputStream inStream;
    byte[] uncompressedValue;

    uncompressedValue = new byte[uncompressedLength];
    inStream = new ByteArrayInputStream(value, offset, length);
    try {
      zipis = new InflaterInputStream(inStream);
      try {
        int totalRead;

        totalRead = 0;
        do {
          int numRead;

          numRead = zipis.read(uncompressedValue, totalRead, uncompressedLength - totalRead);
          if (numRead < 0) {
            throw new RuntimeException("panic");
          }
          totalRead += numRead;
        } while (totalRead < uncompressedLength);
        return uncompressedValue;
      } finally {
        zipis.close();
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
        compressed = new Zip().compress(original, 0, 0);
        uncompressed = new Zip().decompress(compressed, 0, 0, original.length);
        log.info("{} {} {} {}", arg , original.length ,compressed.length , new String(uncompressed));
        log.info(StringUtil.byteArrayToHexString(original));
        log.info(StringUtil.byteArrayToHexString(uncompressed));
      }
    } catch (Exception e) {
      e.printStackTrace();
    }
  }
}
