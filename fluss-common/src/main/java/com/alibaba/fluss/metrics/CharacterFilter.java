/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.alibaba.fluss.metrics;

import com.alibaba.fluss.annotation.PublicEvolving;

/**
 * Interface for a character filter function. The filter function is given a string which the filter
 * can transform. The returned string is the transformation result.
 *
 * @since 0.2
 */
@PublicEvolving
public interface CharacterFilter {

    CharacterFilter NO_OP_FILTER = input -> input;

    /**
     * Filter the given string and generate a resulting string from it.
     *
     * <p>For example, one implementation could filter out invalid characters from the input string.
     *
     * @param input Input string
     * @return Filtered result string
     */
    String filterCharacters(String input);
}
