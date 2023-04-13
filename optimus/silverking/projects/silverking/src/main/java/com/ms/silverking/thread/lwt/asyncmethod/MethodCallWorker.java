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
package com.ms.silverking.thread.lwt.asyncmethod;

import com.ms.silverking.thread.lwt.BaseWorker;
import com.ms.silverking.thread.lwt.LWTPool;
import com.ms.silverking.thread.lwt.LWTPoolParameters;
import com.ms.silverking.thread.lwt.LWTPoolProvider;

public class MethodCallWorker extends BaseWorker<MethodCallWork> {
  public MethodCallWorker(LWTPool methodCallPool) {
    super(methodCallPool, true, 0);
  }

  public MethodCallWorker(LWTPoolParameters poolParams) {
    this(LWTPoolProvider.createPool(poolParams));
  }

  public void asyncInvocation(MethodCallWork work) {
    addWork(work);
  }

  @Override
  public void doWork(MethodCallWork mcw) {
    try {
      mcw.apply();
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }
}
