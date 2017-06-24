/*
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
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.fs.azure.integration;

/**
 * Sizes of data.
 * Checkstyle doesn't like the naming scheme or the fact its an interface.
 */
public interface Sizes {

  int _1KB = 1024;
  int _4KB = 4 * _1KB;
  int _8KB = 8 * _1KB;
  int _16KB = 16 * _1KB;
  int _32KB = 32 * _1KB;
  int _64KB = 64 * _1KB;
  int _128KB = 128 * _1KB;
  int _256KB = 256 * _1KB;
  int _1MB = _1KB * _1KB;
  int _2MB = 2 * _1MB;
  int _5MB = 5 * _1MB;
  int _10MB = 10* _1MB;
}
