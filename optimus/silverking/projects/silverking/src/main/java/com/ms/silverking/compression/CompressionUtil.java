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

import com.ms.silverking.cloud.dht.client.Compression;

public class CompressionUtil {
  public static byte[] compress(Compression compression, byte[] rawValue, int offset, int length)
      throws IOException {
    Compressor compressor;

    compressor = CodecProvider.getCompressor(compression);
    if (compressor == null) {
      compressor = CodecProvider.nullCodec;
    }
    return compressor.compress(rawValue, offset, length);
  }

  public static byte[] decompress(
      Compression compression, byte[] value, int offset, int length, int uncompressedLength)
      throws IOException {
    Decompressor decompressor;

    decompressor = CodecProvider.getDecompressor(compression);
    if (decompressor == null) {
      decompressor = CodecProvider.nullCodec;
    }
    return decompressor.decompress(value, offset, length, uncompressedLength);
  }
}
