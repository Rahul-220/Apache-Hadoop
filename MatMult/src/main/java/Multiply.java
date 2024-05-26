import java.io.*;
import java.util.*;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.util.*;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.*;
import org.apache.hadoop.mapreduce.lib.output.*;


class Elem implements Writable{
    int tag; // 0 for M, 1 for N
    int index; // one of the indexes (other one is a key)
    double value;

    Elem() {}

    // Constructor
    Elem(int tag, int index, double value) {
        this.tag = tag;
        this.index = index;
        this.value = value;
    }

    public void write(DataOutput output) throws IOException {
        output.writeInt(tag);
        output.writeInt(index);
        output.writeDouble(value);
    }

    public void readFields(DataInput input) throws IOException {
        tag = input.readInt();
        index = input.readInt();
        value = input.readDouble();
    }
}


class Pair implements WritableComparable<Pair> {
    public int i;
    public int j;
	
    Pair () {}

    // Constructor
    Pair ( int i, int j ) { 
        this.i = i; 
        this.j = j; 
    }

    public void write(DataOutput output) throws IOException {
        output.writeInt(i);
        output.writeInt(j);
    }

    public void readFields(DataInput input) throws IOException {
        i = input.readInt();
        j = input.readInt();
    }
 
    public int compareTo(Pair comp) {
        if(i > comp.i){
            return 1;
        }
        else if(i < comp.i){
            return -1;
        }
        else {
            if(j > comp.j){
                return 1;
            }
            else if(j < comp.j) {
                return -1;
            }
        }
        return 0;
    }

    public String toString() {
        return i + "," + j + ",";
    }

    /*...*/
}

public class Multiply extends Configured implements Tool {
     
    // First Map-Reduce job
    public static class MapperM extends Mapper<Object,Text,IntWritable,Elem> {
        @Override
        // Mapper for matrix M
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String read = value.toString();
            String strArray[] = read.split(","); // Split line into 3 values: i, j, and v

            int i = Integer.parseInt(strArray[0]);
            double v = Double.parseDouble(strArray[2]);

            IntWritable j = new IntWritable(Integer.parseInt(strArray[1]));
             // Emit key-value pair
            context.write(j, new Elem(0, i, v));

        }
    }

    public static class MapperN extends Mapper<Object,Text,IntWritable,Elem> {
        @Override
        // Mapper for matrix N
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String read = value.toString();
            String strArray[] = read.split(","); // Split line into 3 values: i, j, and v

            int j = Integer.parseInt(strArray[1]);
            double v = Double.parseDouble(strArray[2]);

            IntWritable i = new IntWritable(Integer.parseInt(strArray[0]));
            // Emit key-value pair
            context.write(i, new Elem(1, j, v));
        }
    }

    public static class Reducer1 extends Reducer<IntWritable,Elem, Pair, DoubleWritable> {

        static Vector<Elem> A = new Vector<Elem>();
		static Vector<Elem> B = new Vector<Elem>();
        
        @Override
        // First Reducer
        public void reduce(IntWritable key, Iterable<Elem> values, Context context) throws IOException, InterruptedException{

            A.clear();
            B.clear();

             // Separate values based on tag
            for(Elem elem : values){
                if(elem.tag == 0){
                    A.add(new Elem(elem.tag,elem.index,elem.value));
                }else if(elem.tag == 1){
                    B.add(new Elem(elem.tag,elem.index,elem.value));
                }
            }

            // Perform matrix multiplication
            for(Elem a : A){
                for(Elem b : B){
                    context.write(new Pair(a.index,b.index), new DoubleWritable(a.value * b.value));
                }
            }
        }
    }

    public static class Mapper2 extends Mapper<Object, Text, Pair, DoubleWritable> {
         // Second Map-Reduce job
        @Override
        // Do nothing
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException{

            String read = value.toString();
            String pairVal[] = read.split(",");

            Pair p = new Pair(Integer.parseInt(pairVal[0]),Integer.parseInt(pairVal[1]));
            // Emit key-value pair
            context.write(p, new DoubleWritable(Double.parseDouble(pairVal[2])));
        }
    }

    public static class Reducer2 extends Reducer<Pair, DoubleWritable, Pair, DoubleWritable> {

        @Override
        //Second Reducer
        public void reduce(Pair key, Iterable<DoubleWritable> values, Context context) throws IOException, InterruptedException{

            double m = 0.0;
            // Perform summation
            for(DoubleWritable v : values){
                m = m + v.get();
            }
            // Emit final result
            context.write(key, new DoubleWritable(m));
        }
    }
        
    /* ... */

    @Override
    public int run ( String[] args ) throws Exception {
        /* ... */
        return 0;
    }

    public static void main ( String[] args ) throws Exception {

        // First Map-Reduce job to multiply each values of corresponding rows and columns
        Job job1 = Job.getInstance();
        job1.setJobName("MultiplyJob1");
        job1.setJarByClass(Multiply.class);

        // First Reducer ouptput format
        job1.setOutputKeyClass(Pair.class);
        job1.setOutputValueClass(DoubleWritable.class);

        // First job Mapper output format
        job1.setMapOutputKeyClass(IntWritable.class);
        job1.setMapOutputValueClass(Elem.class);

        job1.setReducerClass(Reducer1.class);

        job1.setOutputFormatClass(TextOutputFormat.class);
        MultipleInputs.addInputPath(job1,new Path(args[0]),TextInputFormat.class,MapperM.class); // input matrix M
        MultipleInputs.addInputPath(job1,new Path(args[1]),TextInputFormat.class,MapperN.class); // input matrix N
        FileOutputFormat.setOutputPath(job1,new Path(args[2])); // intermediate results

        job1.waitForCompletion(true);

        // Second Map-Reduce job to sum all the corresponding values for each cell
        Job job2 = Job.getInstance();
        job2.setJobName("MultiplyJob2");
        job2.setJarByClass(Multiply.class);

        // Second Reducer ouptput format
        job2.setOutputKeyClass(Pair.class);
        job2.setOutputValueClass(DoubleWritable.class);

        // Second job Mapper output format
        job2.setMapOutputKeyClass(Pair.class);
        job2.setMapOutputValueClass(DoubleWritable.class);

        job2.setMapperClass(Mapper2.class);
        job2.setReducerClass(Reducer2.class);

        job2.setInputFormatClass(TextInputFormat.class);
        job2.setOutputFormatClass(TextOutputFormat.class);
        FileInputFormat.setInputPaths(job2, new Path(args[2])); // intermediate results
        FileOutputFormat.setOutputPath(job2, new Path(args[3])); // output directory

        job2.waitForCompletion(true);

    }
}

