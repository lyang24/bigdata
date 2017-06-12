package com.example.pig;

import java.io.IOException;

import org.apache.commons.math3.distribution.PoissonDistribution;
import org.apache.pig.builtin.PigStorage;
import org.apache.pig.data.Tuple;

public class Bootstrap extends PigStorage
{
    Tuple record;
    int replication = 0;
    PoissonDistribution pd = new PoissonDistribution(5.0, 1.0);

    public Tuple getNext() throws IOException {
    	if (replication == 0){
        	replication = pd.sample();
        	record = super.getNext();
            //record != null 防止读到最后一行的情况下，replication =0的时候还继续读，浪费时间
            //getNext()读取中间的空行，返回的是空的tuple，并非null
            //用pig script来filter空行
        	while (record != null && replication == 0){
        		replication = pd.sample();
        		record = super.getNext();
        	}
    	}
    	replication -= 1;
    	return record;
    }

}
