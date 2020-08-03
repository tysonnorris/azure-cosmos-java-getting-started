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

package com.azure.cosmos.sample.async;

import io.netty.util.ResourceLeakDetector;

import java.util.concurrent.atomic.AtomicLong;

public class RecordingLeakDetector<T> extends ResourceLeakDetector<T> {
    private final AtomicLong counter;
    public RecordingLeakDetector(AtomicLong counter, Class<?> resourceType, int samplingInterval) {
        super(resourceType, samplingInterval);
        this.counter = counter;
    }

    @Override
    protected void reportTracedLeak(String resourceType, String records) {
        super.reportTracedLeak(resourceType, records);
        counter.incrementAndGet();
    }

    @Override
    protected void reportUntracedLeak(String resourceType) {
        super.reportUntracedLeak(resourceType);
        counter.incrementAndGet();
    }
}
