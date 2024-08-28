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
package optimus.dist.gsfclient;

import java.time.Duration;

// meta-information about the job generated and consumed in GridDistributionClient
public class JobClientMetrics {

  private long requestBytes;
  private long uncompressedRequestBytes;
  private Duration requestCompressionTime;

  private long responseBytes;
  private long uncompressedResponseBytes;
  private Duration responseDecompressionTime;

  public JobClientMetrics(long requestBytes, long responseBytes) {
    this.requestBytes = requestBytes;
    this.responseBytes = responseBytes;
  }

  public static JobClientMetrics create() {
    return new JobClientMetrics(0, 0);
  }

  public long getRequestBytes() {
    return requestBytes;
  }

  public void setRequestBytes(long requestBytes) {
    this.requestBytes = requestBytes;
  }

  public long getResponseBytes() {
    return responseBytes;
  }

  public void setResponseBytes(long responseBytes) {
    this.responseBytes = responseBytes;
  }

  public long getUncompressedRequestBytes() {
    return uncompressedRequestBytes;
  }

  public void setUncompressedRequestBytes(long uncompressedRequestBytes) {
    this.uncompressedRequestBytes = uncompressedRequestBytes;
  }

  public Duration getRequestCompressionTime() {
    return requestCompressionTime;
  }

  public void setRequestCompressionTime(Duration requestCompressionTime) {
    this.requestCompressionTime = requestCompressionTime;
  }

  public long getUncompressedResponseBytes() {
    return uncompressedResponseBytes;
  }

  public void setUncompressedResponseBytes(long uncompressedResponseBytes) {
    this.uncompressedResponseBytes = uncompressedResponseBytes;
  }

  public Duration getResponseDecompressionTime() {
    return responseDecompressionTime;
  }

  public void setResponseDecompressionTime(Duration responseDecompressionTime) {
    this.responseDecompressionTime = responseDecompressionTime;
  }
}
