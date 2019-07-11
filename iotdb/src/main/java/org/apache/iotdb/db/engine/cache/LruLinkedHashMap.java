/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.iotdb.db.engine.cache;

import java.util.LinkedHashMap;
import java.util.Map;
/**
 *  This class is a LRU cache. <b>Note: It's not thread safe.</b>
 */
public class LruLinkedHashMap<K, V> extends LinkedHashMap<K, V> {

  private static final long serialVersionUID = 1290160928914532649L;
  private static final float LOAD_FACTOR_MAP = 0.75f;
  private int maxCapacity;

  public LruLinkedHashMap(int maxCapacity, boolean isLru) {
    super(maxCapacity, LOAD_FACTOR_MAP, isLru);
    this.maxCapacity = maxCapacity;
  }

  @Override
  protected boolean removeEldestEntry(
      Map.Entry<K, V> eldest) {
    return size() > maxCapacity;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    return super.equals(o);
  }

  @Override
  public int hashCode() {
    return super.hashCode();
  }
}
