package com.iwinner.tech.hadoop.udf;

import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDF;
import org.apache.hadoop.hive.ql.udf.UDFType;
import org.apache.hadoop.io.LongWritable;

@Description(name = "row_sequence", value = "_FUNC_() - Returns a generated row sequence number starting from 1")
@UDFType(deterministic = false, stateful = true)
public class UDFRowSequence extends UDF {
	private LongWritable result = new LongWritable();

	public UDFRowSequence() {
		this.result.set(0L);
	}

	public LongWritable evaluate() {
		this.result.set(this.result.get() + 1L);
		return this.result;
	}
}