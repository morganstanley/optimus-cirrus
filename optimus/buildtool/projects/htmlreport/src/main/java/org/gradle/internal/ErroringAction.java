/*
 * Copyright 2012 the original author or authors. (see https://github.com/gradle/gradle which also uses Apache 2.0)
 *
 * Modifications were made to that code for compatibility with Optimus Build Tool and its report file layout.
 * For those changes only, where additions and modifications are indicated with 'ms' in comments:
 *
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

package org.gradle.internal;

import org.gradle.api.Action;

/**
 * Action adapter/implementation for action code that may throw exceptions.
 *
 * <p>Implementations implement doExecute() (instead of execute()) which is allowed to throw checked
 * exceptions. Any checked exceptions thrown will be wrapped as unchecked exceptions and re-thrown.
 *
 * <p>How the exception is wrapped is subject to {@link
 * UncheckedException#throwAsUncheckedException(Throwable)}.
 *
 * @param <T> The type of object which this action accepts.
 */
public abstract class ErroringAction<T> implements Action<T> {

  public void execute(T thing) {
    try {
      doExecute(thing);
    } catch (Exception e) {
      throw UncheckedException.throwAsUncheckedException(e);
    }
  }

  protected abstract void doExecute(T thing) throws Exception;
}
