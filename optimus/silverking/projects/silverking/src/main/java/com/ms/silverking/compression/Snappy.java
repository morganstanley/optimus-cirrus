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

import java.io.IOException;

import com.ms.silverking.text.StringUtil;
import org.slf4j.LoggerFactory;
import org.slf4j.Logger;

public class Snappy implements Compressor, Decompressor {
  private static final int snappyInitFactor = 10;

  private static Logger log = LoggerFactory.getLogger(Snappy.class);

  public Snappy() {
  }

  public byte[] compress(byte[] rawValue, int offset, int length) throws IOException {
        /*
        SnappyCompressor    sc;
        byte[]                output;
        
        sc = new SnappyCompressor();
        sc.setInput(rawValue, offset, length);
        output = new byte[length];
        sc.compress(output, 0, length);
        return output;
        */
    return rawValue;
  }

  public byte[] decompress(byte[] value, int offset, int length, int uncompressedLength) throws IOException {
        /*
        SnappyDecompressor    sd;
        byte[]    output;
        
        sd = new SnappyDecompressor();
        sd.setInput(value, offset, length);
        output = new byte[length];
        sd.decompress(output, 0, length);
        return output;
        */
    return value;
  }

  /**
   * @param args
   */
  public static void main(String[] args) {
    try {
      for (String arg : args) {
        byte[] original;
        byte[] compressed;
        byte[] uncompressed;
        original = arg.getBytes();
        compressed = new Snappy().compress(original, 0, original.length);
        uncompressed = new Snappy().decompress(compressed, 0, compressed.length, original.length);
        //print of uncompressed may be 'corrupted' by non-printable chars from MD5
        log.info("{} {} {} {}",arg , original.length , compressed.length , new String(uncompressed));
        log.info(StringUtil.byteArrayToHexString(original));
        log.info(StringUtil.byteArrayToHexString(uncompressed));
        //int len = uncompressed.length - MD5Hash.MD5_BYTES;
        //byte[] noMd5 = new byte[len];
        //System.arraycopy(uncompressed, 0, noMd5, 0, len);
        //System.out.println(StringUtil.byteArrayToHexString(noMd5));
      }
    } catch (Exception e) {
      e.printStackTrace();
    }
  }

}
