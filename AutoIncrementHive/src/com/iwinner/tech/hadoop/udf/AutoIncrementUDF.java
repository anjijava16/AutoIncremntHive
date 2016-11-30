package com.iwinner.tech.hadoop.udf;

import org.apache.hadoop.hive.ql.exec.UDF;
import org.apache.hadoop.hive.ql.udf.UDFType;

@UDFType(stateful = true)
public class AutoIncrementUDF extends UDF {

	int count;

	public int evaluate() {

		count++;

		return count;
	}
	
	public static void main(String[] args) {
		AutoIncrementUDF us=new AutoIncrementUDF();
		
		for(int i=0;i<10;i++){
		System.out.println(us.evaluate());
		}
	
	}
}

