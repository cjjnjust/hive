/**
 *  Licensed to the Apache Software Foundation (ASF) under one
 *  or more contributor license agreements.  See the NOTICE file
 *  distributed with this work for additional information
 *  regarding copyright ownership.  The ASF licenses this file
 *  to you under the Apache License, Version 2.0 (the
 *  "License"); you may not use this file except in compliance
 *  with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */
package org.apache.hadoop.hive.ql.exec.spark;

import org.apache.hadoop.hive.ql.io.HiveKey;
import org.apache.hadoop.io.BytesWritable;
import org.apache.spark.HashPartitioner;
import org.apache.spark.Partitioner;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.JavaSparkContext$;
import org.apache.tools.ant.taskdefs.Java;
import scala.Tuple2;

/**
 * Implement Order By Limit in SortByLimitShuffler
 */
public class SortByLimitShuffler implements SparkShuffler<BytesWritable> {

  private final int limit;
  /**
   * @param limit The limit of result.
   */
  public SortByLimitShuffler(int limit) {
    this.limit = limit;
  }

  @Override
  public JavaPairRDD<HiveKey, BytesWritable> shuffle(
      JavaPairRDD<HiveKey, BytesWritable> input, int numPartitions) {
    JavaPairRDD<HiveKey, BytesWritable> rdd = input.sortByKey();
    
    rdd.zipWithIndex().filter((Tuple2<Tuple2<HiveKey, BytesWritable>, Long> v) -> v._2 < limit);
    rdd.keys();

    return rdd;
  }

  @Override
  public String getName() {
    return "SortByLimit";
  }
}
