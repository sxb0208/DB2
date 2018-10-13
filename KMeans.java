import java.io.*;
import java.net.URI;
import java.util.Hashtable;
import java.util.Enumeration;
import java.util.Vector;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.*;
import org.apache.hadoop.mapreduce.lib.output.*;

class Point implements WritableComparable<Point> {
    public double x;
    public double y;
    Point(){x=0; y=0;}
    Point ( double x, double y ) {
        this.x = x; this.y = y;
    }
    @Override
    public void readFields(DataInput in) throws IOException {
        x=in.readDouble();
        y=in.readDouble();
    }
    @Override
    public void write(DataOutput o) throws IOException {
        o.writeDouble(x);
        o.writeDouble(y);
    }
    @Override
    public int compareTo(Point o) {
        int c = Double.compare(this.x, o.x);
        if (c != 0) return c;
        else return Double.compare(this.y, o.y);
    }
    @Override
    public String toString() {
    return x +","+y;
    }
    }

class Avg implements Writable {
	public double sumX;
	public double sumY;
	public long count;
	Avg () { sumX=0; sumY=0;count=0;}
	Avg(double x,double y, long c)
	{
		this.sumX=x; this.sumY=y; this.count=c;
	}
	@Override
	public String toString()
	{
		return sumX+","+sumY+","+count;
	}
	@Override
	public void write(DataOutput o) throws IOException
	{
		o.writeDouble(sumX);
        o.writeDouble(sumY);
        o.writeLong(count);	
	}
	@Override
	public void readFields(DataInput i) throws IOException {
		sumX=i.readDouble();
        sumY=i.readDouble();
        count=i.readLong();
	}
}

public class KMeans {
    static Vector<Point> centroids = new Vector<Point>(100);
    static Hashtable<Point,Avg> table = new Hashtable<Point,Avg>();
    
    public static class AvgMapper extends Mapper<Object,Text,Point,Avg> {
        @Override
        public void setup(Context context) throws IOException {
            try{
        	String line;
            
            URI[] paths = context.getCacheFiles();
            Configuration conf = context.getConfiguration();
            FileSystem fs = FileSystem.get(conf);
            BufferedReader br = new BufferedReader(new InputStreamReader(fs.open(new Path(paths[0]))));
            while ((line = br.readLine()) != null) {
            	 Point p = new Point();
            	Avg a= new Avg(0,0,-1);
            	 String[] parts = line.split(",");
               p.x= Double.parseDouble(parts[0]);
                p.y = Double.parseDouble(parts[1]);
                table.put(p,a);
                centroids.addElement(p);
              
               }
          System.out.println(table);
            }
            catch(Exception exc){
            	System.out.println("Error!In Setup function");
            }
        }    
         @Override   
         protected void cleanup(Context context) throws IOException, InterruptedException, NullPointerException{
        	try{
        		Enumeration<Point> names;
        		Point key = new Point();
        		names = table.keys();
        		 while(names.hasMoreElements()) {
           	      key = (Point)names.nextElement();
           	   context.write(key,table.get(key));
        	}
         }
       catch(Exception exc)
        	{
    	   System.out.println("Error! In cleanup function");
        	}
         }
        @Override
        public void map ( Object key, Text value, Context context )
                        throws IOException, InterruptedException {
            try{
        	Point CentroidPoint=new Point();
            double nearest=Double.MAX_VALUE;
            double distance=0.00;
            Avg a=new Avg();
            String[] line = value.toString().split(",");
            Point  targetPoint= new Point(Double.parseDouble(line[0]),Double.parseDouble(line[1]));
            for (Point p: centroids) {
                    distance=Math.sqrt(Math.pow(Math.abs (targetPoint.x - p.x),2)+Math.pow(Math.abs (targetPoint.y - p.y),2));
                    if(distance<nearest) {
                        nearest=distance;
                        CentroidPoint=p;
                    }
            }
            if(table.get(CentroidPoint).count==-1)
            {
            	a=new Avg(targetPoint.x,targetPoint.y,1);
            	table.replace(CentroidPoint,a);
            }
            else
            {
            	a=new Avg(table.get(CentroidPoint).sumX+targetPoint.x,table.get(CentroidPoint).sumY+targetPoint.y,table.get(CentroidPoint).count+1);
            	table.replace(CentroidPoint,a);
            } }
            catch(Exception exc)
            {
            	System.out.println("Error!In Map function");
            }
        }
    }


    
    
    public static class AvgReducer extends Reducer<Point,Avg,Point,Object> {
        @Override
        public void reduce ( Point cent,Iterable<Avg> p, Context context )
                           throws IOException, InterruptedException,NullPointerException {
           try
           {
        	long num = 0;
            double centerx=0.0;
            double centery=0.0;
            for (Avg cluster_p : p) {
               centerx += cluster_p.sumX;
                centery += cluster_p.sumY;
                num+=cluster_p.count;
             }
            cent.x = centerx/num;
            cent.y = centery/num;
          
            context.write(cent,NullWritable.get());
        }
           catch(Exception exc)
           {
        	   System.out.println("Error! In Reduce Function");
           }
    }
    }
    
    public static void main ( String[] args ) throws Exception {

        Job job=Job.getInstance();
        job.addCacheFile(new URI(args[1]));
        job.setJobName("KMEANS CLUSTERING");
        job.setJarByClass(KMeans.class);
        job.setOutputKeyClass(Point.class);
        job.setOutputValueClass(NullWritable.class);
        job.setMapOutputKeyClass(Point.class);
        job.setMapOutputValueClass(Avg.class);
        job.setReducerClass(AvgReducer.class);
        job.setMapperClass(AvgMapper.class);
        job.setOutputFormatClass(TextOutputFormat.class);
        FileInputFormat.setInputPaths(job,new Path(args[0]));
        FileOutputFormat.setOutputPath(job,new Path(args[2]));
        job.waitForCompletion(true);
    }
}