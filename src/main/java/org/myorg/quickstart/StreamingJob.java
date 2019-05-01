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

package org.myorg.quickstart;

import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.util.Collector;
import java.util.List;
import java.io.FileOutputStream;

/**
 * Skeleton for a Flink Streaming Job.
 *
 * <p>For a tutorial how to write a Flink streaming application, check the
 * tutorials and examples on the <a href="http://flink.apache.org/docs/stable/">Flink Website</a>.
 *
 * <p>To package your application into a JAR file for execution, run
 * 'mvn clean package' on the command line.
 *
 * <p>If you change the name of the main class (with the public static void main(String[] args))
 * method, change the respective entry in the POM.xml file (simply search for 'mainClass').
 */
public class StreamingJob {

	public static void main(String[] args) throws Exception {
		// set up the streaming execution environment
		final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

		DataSet<String> text = env.readTextFile("/home/user/Desktop/words.txt");
		DataSet<Tuple2<String, Integer>> wordCounts =text.flatMap(new Tokenizer()).groupBy(0).sum(1);
		FileOutputStream out=new FileOutputStream("/home/user/Desktop/wordscountresult.txt");
		List<Tuple2<String, Integer>>  wordList = wordCounts.collect();

		for(Tuple2<String, Integer> i : wordList)
		{
			out.write(i.toString().getBytes());
			out.write('\n');
		}
	}

		public static class Tokenizer implements FlatMapFunction<String, Tuple2<String, Integer>> {

			@Override
			public void flatMap(String value, Collector<Tuple2<String, Integer>> out) {
				// normalize and split the line
				String[] tokens = value.toLowerCase().split("\\W+");

				// emit the pairs
				for (String token : tokens) {
					if (token.length() > 0) {
						out.collect(new Tuple2<String, Integer>(token, 1));
					}
				}
			}
		}


}
