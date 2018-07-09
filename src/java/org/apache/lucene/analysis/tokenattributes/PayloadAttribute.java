package org.apache.lucene.analysis.tokenattributes;

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

import org.apache.lucene.index.DocsAndPositionsEnum; // javadocs
import org.apache.lucene.util.Attribute;
import org.apache.lucene.util.BytesRef;

/**
 * The payload of a Token.
 * <p>
 * The payload is stored in the index at each position, and can
 * be used to influence scoring when using Payload-based queries 
 * in the {@link org.apache.lucene.search.payloads} and
 * {@link org.apache.lucene.search.spans} packages.
 * <p>
 * NOTE: because the payload will be stored at each position, its usually
 * best to use the minimum number of bytes necessary. Some codec implementations
 * may optimize payload storage when all payloads have the same length.
 * 
 * @see DocsAndPositionsEnum
 * 为一个token提供额外的信息--该额外信息是一个字节数组组成的,因此理论上写入什么都可以,包括备注属性，而且什么格式都可以,比如json
 */
public interface PayloadAttribute extends Attribute {
  /**
   * Returns this Token's payload.
   * @see #setPayload(BytesRef)
   */ 
  public BytesRef getPayload();

  /** 
   * Sets this Token's payload.
   * @see #getPayload()
   */
  public void setPayload(BytesRef payload);
}
