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
package com.ms.silverking.cloud.dht.net;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import com.ms.silverking.numeric.NumConversion;
import optimus.breadcrumbs.ChainedID;

public class ChainedIdSerialization {
  public static ByteBuffer serializeChainId(ChainedID chainedID) {

    byte[] reprBytes = chainedID.repr().getBytes(StandardCharsets.UTF_8);
    byte[] vertexIdBytes = chainedID.vertexId().getBytes(StandardCharsets.UTF_8);

    // Claculating the total length of elements to be sent into the byteBuffer
    int totalEncodeLength =
        reprBytes.length + vertexIdBytes.length + NumConversion.BYTES_PER_INT * 5;
    // Allocating the size of the buffer
    ByteBuffer chainedBuffer = ByteBuffer.allocate(totalEncodeLength);
    // Putting in the elements into the byteBuffer
    chainedBuffer.putInt(
        chainedID.ArrayRepVersion()); // putting in the ArrayRepVersion which is an intiger so has 4
    // bytes
    chainedBuffer.putInt(reprBytes.length); // putting in the size of the reprByte
    chainedBuffer.put(
        chainedID.repr().getBytes(StandardCharsets.UTF_8)); // putting in the reprBytes
    chainedBuffer.putInt(
        chainedID.depth()); // putting in the depth which is an intiger so has 4 bytes
    chainedBuffer.putInt(
        chainedID.crumbLevel()); // putting in the level which is an intiger so has 4 bytes
    chainedBuffer.putInt(vertexIdBytes.length); // putting in the size of the vertexId
    chainedBuffer.put(
        chainedID.vertexId().getBytes(StandardCharsets.UTF_8)); // putting in the vertexId

    return chainedBuffer;
  }

  public static ChainedID deserializeChainId(ByteBuffer byteBuffer) {

    int arrayRepVersion, reprLength, depth, level, vertexIdLength, offset = 0;
    byte[] reprBytes, vertexIdBytes;
    byteBuffer.position(
        0); // Seting default position to start reading elements from. If not set, elements might be
    // read from other positions

    arrayRepVersion = byteBuffer.getInt(offset);
    offset += NumConversion.BYTES_PER_INT;
    byteBuffer.position(offset); // set postion for next element to be read from

    reprLength = byteBuffer.getInt(offset); // getting next element in byteBuffer
    reprBytes = new byte[reprLength]; // Creating byte variable to save element in
    offset += NumConversion.BYTES_PER_INT;
    byteBuffer.position(offset); // set postion for next element to be read from
    byteBuffer.get(reprBytes, 0, reprLength); // saving deserialied bytes into byte array
    String repr = new String(reprBytes); // Creating byte variable to save element in
    offset += reprLength;
    byteBuffer.position(offset); // set postion for next element to be read from

    depth = byteBuffer.getInt(offset);
    offset += NumConversion.BYTES_PER_INT;
    byteBuffer.position(offset);

    level = byteBuffer.getInt(offset);
    offset += NumConversion.BYTES_PER_INT;
    byteBuffer.position(offset);

    vertexIdLength = byteBuffer.getInt(offset);
    vertexIdBytes = new byte[vertexIdLength];
    offset += NumConversion.BYTES_PER_INT;
    byteBuffer.position(offset);
    byteBuffer.get(vertexIdBytes, 0, vertexIdLength);
    String vertexId = new String(vertexIdBytes);

    return new ChainedID(repr, depth, level, vertexId);
  }
}
