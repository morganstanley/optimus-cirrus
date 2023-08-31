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
package com.ms.silverking.thread.lwt;

import java.util.Collection;

/**
 * Collection of workers that accepts group work operations such as broadcasting a work item to all
 * members of the group, scattering a collection of work items among the group, etc.
 *
 * @param <I>
 */
public interface WorkerGroup<I> {
  public String getName();

  public void addWorker(BaseWorker<I> worker);

  public void broadcastWork(I item);

  public void scatterWork(I[] items);

  public void scatterWork(Collection<I> items);

  public void addWork(I item);
}
