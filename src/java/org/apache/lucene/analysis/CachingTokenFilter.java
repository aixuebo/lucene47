package org.apache.lucene.analysis;

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

import java.io.IOException;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;

import org.apache.lucene.util.AttributeSource;

/**
 * This class can be used if the token attributes of a TokenStream
 * are intended to be consumed more than once. It caches
 * all token attribute states locally in a List.
 * 该类会将所有的token持有的属性缓存起来,用于需要被消费多次的场景
 * 
 * 注意:我觉得该类是非常占用内存的，尤其当文本非常大的时候,他需要将所有的分词放在缓存里面,不过考虑到每一个document也不会太大，所以也可以接受
 * 
 * <P>CachingTokenFilter implements the optional method
 * {@link TokenStream#reset()}, which repositions the
 * stream to the first Token. 
 */
public final class CachingTokenFilter extends TokenFilter {
  private List<AttributeSource.State> cache = null;//缓存的token集合
  private Iterator<AttributeSource.State> iterator = null; //token的内存迭代器
  private AttributeSource.State finalState;//最后一个token状态
  
  /**
   * Create a new CachingTokenFilter around <code>input</code>,
   * caching its token attributes, which can be replayed again
   * after a call to {@link #reset()}.
   */
  public CachingTokenFilter(TokenStream input) {
    super(input);
  }
  
  //重新复写该方法,一次性读取所有token进入缓存
  @Override
  public final boolean incrementToken() throws IOException {
    if (cache == null) {
      // fill cache lazily
      cache = new LinkedList<AttributeSource.State>();
      fillCache();//填充所有token进入缓存
      iterator = cache.iterator();//内存中重新进行迭代
    }
    
    //不断的迭代token
    if (!iterator.hasNext()) {
      // the cache is exhausted, return false
      return false;
    }
    // Since the TokenFilter can be reset, the tokens need to be preserved as immutable.
    restoreState(iterator.next());//还原所有的属性集合
    return true;
  }
  
  @Override
  public final void end() {
    if (finalState != null) {
      restoreState(finalState);
    }
  }

  /**
   * Rewinds the iterator to the beginning of the cached list.
   * <p>
   * Note that this does not call reset() on the wrapped tokenstream ever, even
   * the first time. You should reset() the inner tokenstream before wrapping
   * it with CachingTokenFilter.
   */
  @Override
  public void reset() {
    if(cache != null) {
      iterator = cache.iterator();//重新进行token计算,从缓存中重新迭代即可
    }
  }
  
  private void fillCache() throws IOException {
    while(input.incrementToken()) {//不断获取每一个token
      cache.add(captureState());//加入缓存
    }
    // capture final state
    input.end();
    finalState = captureState();//加入最后的一个token状态
  }

}
