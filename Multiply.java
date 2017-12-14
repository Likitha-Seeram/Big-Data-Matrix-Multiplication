import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.*;
import java.util.Scanner;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.*;
import org.apache.hadoop.mapreduce.lib.output.*;

class Element implements Writable {
	short tag;
	int index;
	double value;
	
	Element() {}
	
	Element(short t, int i, double v) {
		tag = t;
		index = i;
		value = v;
	}
	
	public void write ( DataOutput out ) throws IOException {
        out.writeShort(tag);
        out.writeInt(index);
        out.writeDouble(value);
    }

    public void readFields ( DataInput in ) throws IOException {
        tag = in.readShort();
        index = in.readInt();
        value = in.readDouble();
    }
}

class Pair implements WritableComparable<Pair> {
	int i;
	int j;
	
	Pair() {}
	
	Pair(int y, int z) {
		i = y;
		j = z;
	}
	
	@Override
	public void readFields(DataInput in) throws IOException {
		i = in.readInt();
		j = in.readInt();
	}
	@Override
	public void write(DataOutput out) throws IOException {
		out.writeInt(i);
		out.writeInt(j);
	}
	@Override
	public int compareTo(Pair o) {
		int n = this.i - o.i;
		if(n == 0) return this.j - o.j;
		return n;
	}
	@Override
	public String toString() {
		return i+","+j+",";
		}
}

public class Multiply {
	
	public static class MmatrixMapper extends Mapper<Object,Text,IntWritable,Element > {
        @Override
        public void map ( Object key, Text value, Context context )
                        throws IOException, InterruptedException {
        	Scanner s = new Scanner(value.toString()).useDelimiter(",");
        	int i = s.nextInt();
        	int j = s.nextInt();
        	double v = s.nextDouble();
        	context.write(new IntWritable(j), new Element((short) 0,i,v));
        	s.close();
        }
    }

    public static class NmatrixMapper extends Mapper<Object,Text,IntWritable,Element > {
        @Override
        public void map ( Object key, Text value, Context context )
                        throws IOException, InterruptedException {
        	Scanner s = new Scanner(value.toString()).useDelimiter(",");
        	int i = s.nextInt();
        	int j = s.nextInt();
        	double v = s.nextDouble();
        	context.write(new IntWritable(i), new Element((short) 1,j,v));
        	s.close();
        }
    }

    public static class FirstReducer extends Reducer<IntWritable,Element,Pair,DoubleWritable> {
        ArrayList<Element> A = new ArrayList<Element>();
        ArrayList<Element> B = new ArrayList<Element>();
        @Override
        public void reduce ( IntWritable key, Iterable<Element> values, Context context )
                           throws IOException, InterruptedException {
        	A.clear();
        	B.clear();
            for(Element x : values) {
            	if(x.tag == 0) {
            		Element temp = new Element();
            		temp.tag = 0;
            		temp.index = x.index;
            		temp.value = x.value;
            		A.add(temp);		
            	}
            	else  {
            		Element temp = new Element();
            		temp.tag = 1;
            		temp.index = x.index;
            		temp.value = x.value;
            		B.add(temp);
            	}
            }
            for(Element a : A) {
            	for(Element b : B) {
            		context.write(new Pair(a.index,b.index), new DoubleWritable(a.value*b.value));
            	}
            }
        }
    }
    
    public static class ResultMapper extends Mapper<Pair,DoubleWritable,Pair,DoubleWritable > {
        @Override
        public void map ( Pair key, DoubleWritable value, Context context )
                        throws IOException, InterruptedException {
        	context.write(key, value);
        }
    }

    public static class SecondReducer extends Reducer<Pair,DoubleWritable,Pair,DoubleWritable> {
        @Override
        public void reduce ( Pair key, Iterable<DoubleWritable> values, Context context )
                           throws IOException, InterruptedException {
        	double sum = 0.0;
        	
        	for(DoubleWritable d : values) {
        		sum = sum + d.get();
        	}
        	context.write(key, new DoubleWritable(sum));
        }
    }
    
	public static void main ( String[] args ) throws Exception {
        Job job1 = Job.getInstance();
        job1.setJobName("Matrix-Multiplication-Stage1");
        job1.setJarByClass(Multiply.class);
        
        job1.setOutputKeyClass(Pair.class);
        job1.setOutputValueClass(DoubleWritable.class);
        
        job1.setMapOutputKeyClass(IntWritable.class);
        job1.setMapOutputValueClass(Element.class);
        job1.setReducerClass(FirstReducer.class);
        
        job1.setOutputFormatClass(SequenceFileOutputFormat.class);
        MultipleInputs.addInputPath(job1,new Path(args[0]),TextInputFormat.class,MmatrixMapper.class);
        MultipleInputs.addInputPath(job1,new Path(args[1]),TextInputFormat.class,NmatrixMapper.class);
        FileOutputFormat.setOutputPath(job1,new Path(args[2]));
        job1.waitForCompletion(true);
        
        
        Job job2 = Job.getInstance();
        job2.setJobName("Matrix-Multiplication-Stage2");
        job2.setJarByClass(Multiply.class);
        
        job2.setOutputKeyClass(Pair.class);
        job2.setOutputValueClass(DoubleWritable.class);
        
        job2.setMapOutputKeyClass(Pair.class);
        job2.setMapOutputValueClass(DoubleWritable.class);
        job2.setReducerClass(SecondReducer.class);
        
        job2.setOutputFormatClass(TextOutputFormat.class);
        MultipleInputs.addInputPath(job2,new Path(args[2]),SequenceFileInputFormat.class,ResultMapper.class);
        FileOutputFormat.setOutputPath(job2,new Path(args[3]));
        job2.waitForCompletion(true);
    }
}
