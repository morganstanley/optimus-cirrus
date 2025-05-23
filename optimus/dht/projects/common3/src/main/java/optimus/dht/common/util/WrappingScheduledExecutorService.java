package optimus.dht.common.util;

/*
 * Copyright (C) 2013 The Guava Authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
/* (regex-ignore-line - persistent exception for URL)
 * This file is based on https://github.com/google/guava/blob/master/guava/src/com/google/common/util/concurrent/WrappingScheduledExecutorService.java
 * Minor changes were made to some of the parameter validation. For those changes only:
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

import java.util.concurrent.Callable;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

/**
 * An abstract {@code ScheduledExecutorService} that allows subclasses to {@linkplain
 * #wrapTask(Callable) wrap} tasks before they are submitted to the underlying executor.
 *
 * <p>Note that task wrapping may occur even if the task is never executed.
 *
 * @author Luke Sandberg // regex-ignore-line allowed by license citation convention
 */
abstract class WrappingScheduledExecutorService extends WrappingExecutorService
    implements ScheduledExecutorService {
  final ScheduledExecutorService delegate;

  protected WrappingScheduledExecutorService(ScheduledExecutorService delegate) {
    super(delegate);
    this.delegate = delegate;
  }

  @Override
  public final ScheduledFuture<?> schedule(Runnable command, long delay, TimeUnit unit) {
    return delegate.schedule(wrapTask(command), delay, unit);
  }

  @Override
  public final <V> ScheduledFuture<V> schedule(Callable<V> task, long delay, TimeUnit unit) {
    return delegate.schedule(wrapTask(task), delay, unit);
  }

  @Override
  public final ScheduledFuture<?> scheduleAtFixedRate(
      Runnable command, long initialDelay, long period, TimeUnit unit) {
    return delegate.scheduleAtFixedRate(wrapTask(command), initialDelay, period, unit);
  }

  @Override
  public final ScheduledFuture<?> scheduleWithFixedDelay(
      Runnable command, long initialDelay, long delay, TimeUnit unit) {
    return delegate.scheduleWithFixedDelay(wrapTask(command), initialDelay, delay, unit);
  }
}
