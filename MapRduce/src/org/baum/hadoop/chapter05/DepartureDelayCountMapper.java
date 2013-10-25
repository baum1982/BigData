package org.baum.hadoop.chapter05;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;



/**
 * 출발이 지연된 데이터만 걸러서 월별로 매핑
 * @author baum
 *
 */
public class DepartureDelayCountMapper extends
		Mapper<LongWritable, Text, Text, IntWritable> {
		
	// map 출력값
	private final static IntWritable one = new IntWritable(1);
	
	// map 출력기
	private Text outputKey = new Text();
	
	
	@Override
	protected void map(LongWritable key, Text value, Context context)
			throws IOException, InterruptedException {
		
		if(key.get() > 0){
			// 콤마 구분자 분리
			String[] columns = value.toString().split(",");
			
			if(columns != null && columns.length > 0){
				
				// 헤더는 무시
				if("Year".equals(columns[0]))
					return;
				
				
				// 출력키 설정 (년,월)
				outputKey.set(columns[0] + "," + columns[1]);
				
				// [15]: DepDelay, 출발지연
				if(!"NA".equals(columns[15])){
					int depDelayTime = 	Integer.parseInt(columns[15]);
					if(depDelayTime > 0){
						// 출력 데이터 생성
						context.write(outputKey, one);
					}
				}
			}
		}
	}
}
