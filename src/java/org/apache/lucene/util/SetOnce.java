package org.apache.lucene.util;

import java.util.concurrent.atomic.AtomicBoolean;

/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

/**
 * A convenient class which offers a semi-immutable object wrapper
 * implementation which allows one to set the value of an object exactly once,
 * and retrieve it many times. If {@link #set(Object)} is called more than once,
 * {@link AlreadySetException} is thrown and the operation
 * will fail.
 * 该对象里面的obj对象只能被实例化一次
 *
 * @lucene.experimental
 */
public final class SetOnce<T> implements Cloneable {

  /** Thrown when {@link SetOnce#set(Object)} is called more than once. */
  public static final class AlreadySetException extends IllegalStateException {
    public AlreadySetException() {
      super("The object cannot be set twice!");
    }
  }
  
  private volatile T obj = null;//实例化对象
  private final AtomicBoolean set;//true表示已经obj被实例化了
  
  /**
   * A default constructor which does not set the internal object, and allows
   * setting it by calling {@link #set(Object)}.
   */
  public SetOnce() {
    set = new AtomicBoolean(false);
  }

  /**
   * Creates a new instance with the internal object set to the given object.
   * Note that any calls to {@link #set(Object)} afterwards will result in
   * {@link AlreadySetException}
   *
   * @throws AlreadySetException if called more than once
   * @see #set(Object)
   */
  public SetOnce(T obj) {
    this.obj = obj;
    set = new AtomicBoolean(true);//说明实例化了,因此要设置为true
  }
  
  /** Sets the given object. If the object has already been set, an exception is thrown. */
  public final void set(T obj) {
    if (set.compareAndSet(false, true)) {
      this.obj = obj;
    } else {
      throw new AlreadySetException();//说明已经被实例化了,因此抛异常
    }
  }
  
  /** Returns the object set by {@link #set(Object)}. 
   * 获取实例化对象 
   **/
  public final T get() {
    return obj;
  }
  
  @Override
  public SetOnce<T> clone() {
    return obj == null ? new SetOnce<T>() : new SetOnce<T>(obj);
  }
  
}
