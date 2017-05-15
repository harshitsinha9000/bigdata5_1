import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

public class Partitioner52 extends Partitioner<Text,IntWritable> {



	@Override
	public int getPartition(Text key, IntWritable value, int numreducetasks) {
		// TODO Auto-generated method stub
		if(numreducetasks==0)
			return 0;
		
		Pattern p1 = Pattern.compile("$[A-F|A-F]");
		Pattern p2 = Pattern.compile("$[G-L|g-l]");
		Pattern p3 = Pattern.compile("$[M-R|m-r]");
		Pattern p4 = Pattern.compile("$[S-Z|s-z]");
		
		Matcher m1 =p1.matcher(key.toString());
		Matcher m2 =p2.matcher(key.toString());
		Matcher m3 =p3.matcher(key.toString());
		Matcher m4 =p4.matcher(key.toString());
		
		if(m1.matches())
			return (0 % numreducetasks);
		else if(m2.matches())
			return (1 % numreducetasks);
		else if(m3.matches())
			return (2 % numreducetasks);
		else
			return (3 % numreducetasks);
		
	}

	
}
