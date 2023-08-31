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
package com.ms.silverking.collection;

import java.security.InvalidParameterException;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.ListIterator;

import com.ms.silverking.util.Arrays;

/** An ImmutableList that allows nulls (unlike Guava) */
public class SKImmutableList<T> implements List<T> {
  private final T[] elements;

  private SKImmutableList(T[] elements) {
    this.elements = elements;
  }

  private SKImmutableList() {
    this.elements = (T[]) new Object[0];
  }

  public static <T> SKImmutableList of() {
    return new SKImmutableList<>();
  }

  public static <T> SKImmutableList copyOf(T[] a) {
    return copyOf(a, 0, a.length);
  }

  public static <T> SKImmutableList copyOf(T[] a, int fromIndex, int toIndex) {
    T[] elements;
    int length;

    length = toIndex - fromIndex;
    if (length == 0) {
      return of();
    } else if (length > 0) {
      // elements = (T[])Array.newInstance((Class<T>)a[0].getClass(), length);
      elements = (T[]) new Object[length];
      System.arraycopy(a, fromIndex, elements, 0, length);
      return new SKImmutableList<>(elements);
    } else {
      throw new InvalidParameterException();
    }
  }

  @Override
  public int size() {
    return elements.length;
  }

  @Override
  public boolean isEmpty() {
    return elements.length == 0;
  }

  @Override
  public boolean contains(Object o) {
    return Arrays.contains(elements, o);
  }

  @Override
  public Iterator<T> iterator() {
    return Arrays.iterator(elements);
  }

  @Override
  public Object[] toArray() {
    Object[] a;

    a = new Object[elements.length];
    System.arraycopy(elements, 0, a, 0, elements.length);
    return a;
  }

  @Override
  public <T> T[] toArray(T[] a) {
    System.arraycopy(elements, 0, a, 0, elements.length);
    return a;
  }

  @Override
  public boolean add(T e) {
    throw new UnsupportedOperationException();
  }

  @Override
  public boolean remove(Object o) {
    throw new UnsupportedOperationException();
  }

  @Override
  public boolean containsAll(Collection<?> c) {
    throw new RuntimeException("Not yet implemented");
  }

  @Override
  public boolean addAll(Collection<? extends T> c) {
    throw new UnsupportedOperationException();
  }

  @Override
  public boolean addAll(int index, Collection<? extends T> c) {
    throw new UnsupportedOperationException();
  }

  @Override
  public boolean removeAll(Collection<?> c) {
    throw new UnsupportedOperationException();
  }

  @Override
  public boolean retainAll(Collection<?> c) {
    throw new UnsupportedOperationException();
  }

  @Override
  public void clear() {
    throw new UnsupportedOperationException();
  }

  @Override
  public T get(int index) {
    return elements[index];
  }

  @Override
  public T set(int index, T element) {
    throw new UnsupportedOperationException();
  }

  @Override
  public void add(int index, T element) {
    throw new UnsupportedOperationException();
  }

  @Override
  public T remove(int index) {
    throw new UnsupportedOperationException();
  }

  @Override
  public int indexOf(Object o) {
    return Arrays.indexOf(elements, (T) o);
  }

  @Override
  public int lastIndexOf(Object o) {
    return Arrays.lastIndexOf(elements, (T) o);
  }

  @Override
  public ListIterator<T> listIterator() {
    throw new UnsupportedOperationException();
  }

  @Override
  public ListIterator<T> listIterator(int index) {
    throw new UnsupportedOperationException();
  }

  @Override
  public List<T> subList(int fromIndex, int toIndex) {
    return copyOf(elements, fromIndex, toIndex);
  }
}
