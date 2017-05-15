import java.io.IOException;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.*; 

public class Mapper421 extends Mapper<LongWritable, Text,Text,IntWritable> {
	public void map(LongWritable key, Text value,Context context) 
			throws IOException, InterruptedException {
		String[] lineArray = value.toString().split("|");
		String NA = new String("NA");
		String Onida = new String("Onida");
		Text txt;
		
		
		if(!((lineArray[0].equalsIgnoreCase(NA))||(lineArray[1].equalsIgnoreCase(NA))))
		{
			if(lineArray[0].equalsIgnoreCase(Onida))
			{
			context.write(new Text(lineArray[3]),new IntWritable(1));
			}
			
		}
		
	}
}
