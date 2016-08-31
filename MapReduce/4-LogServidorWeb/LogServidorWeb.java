/**
 *	Asignatura:  DGPSI (Master Ingenieria Informatica)
 *	Practica:    1
 *	Autores:     Jorge Casas Hernan y Mariano Hernandez Garcia
 *	Declaracion: Declaramos que hemos realizado este documento nosotros
 *	             mismos de manera exclusiva y sin compartir nada con
 *	             otros grupos. 
 */

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.StringTokenizer;
import java.util.Map;
import java.util.Set;
import java.util.HashMap;
import java.util.TreeMap;
import java.util.HashMap;
import java.util.Comparator;
import java.util.Enumeration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

class TriIntWritable implements Writable {
  public int data1;
  public int data2;
  public int data3;
  
  public void write(DataOutput out) throws IOException {
    out.writeInt(data1);
    out.writeInt(data2);
    out.writeInt(data3);
  }
  
  public void readFields(DataInput in) throws IOException {
    data1 = in.readInt();
    data2 = in.readInt();
    data3 = in.readInt();
  }
  
  public static TriIntWritable read(DataInput in) throws IOException {
    TriIntWritable w = new TriIntWritable();
    w.readFields(in);
    return w;
  }
}


public class LogServidorWeb {

  private final static String input_file = "weblog.txt";
  private final static String output_file = "salida_logWeb";
  
  public static class TokenizerMapper extends Mapper<Object, Text, Text, TriIntWritable>{
    
    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {

	String [] values = value.toString().split(" ");
	TriIntWritable w = new TriIntWritable();
	if ( values[values.length-1].equals("-") )  
		w.data1 = 0;
	else
		w.data1 = Integer.parseInt(values[values.length-1]);

	w.data2 = Integer.parseInt(values[values.length-2]);

        context.write(new Text(values[0]), w);

    }
  }


  public static class LogWebCombiner extends Reducer<Text, TriIntWritable, Text, TriIntWritable> {

    public void reduce(Text key, Iterable<TriIntWritable> values, Context context) throws IOException, InterruptedException {
	
	TriIntWritable tupla = new TriIntWritable();
	tupla.data1 = 0; // Contador de peticiones.
	tupla.data2 = 0; // Contador de bytes.
	tupla.data3 = 0; // Contador de errores.
	
	for ( TriIntWritable w : values ) {
		++tupla.data1;
		tupla.data2 += w.data1;
		if ( w.data2 >= 400 && w.data2 < 600 )
			++tupla.data3;
		
	}

	context.write(key, tupla);
    }

  }
  
  
  public static class LogWebReducer extends Reducer<Text,TriIntWritable,Text,Text> {

    public void reduce(Text key, Iterable<TriIntWritable> values, Context context) throws IOException, InterruptedException {

	int req_count   = 0;
	int bytes_count = 0;
	int num_errors  = 0;
	
	for ( TriIntWritable w : values ) {
		req_count += w.data1;
		bytes_count += w.data2;
		num_errors += w.data3;
	}

	context.write(key, new Text( "(" + req_count + ", " + bytes_count + ", " + num_errors + ")" ) ); 
    }
  }

  public static void main(String[] args) throws Exception {
    JobConf conf = new JobConf();
    Job job = Job.getInstance(conf);
    job.setJarByClass(LogServidorWeb.class);
    
    job.setMapperClass(TokenizerMapper.class);
    job.setCombinerClass(LogWebCombiner.class);
    job.setReducerClass(LogWebReducer.class);

    // Declaración de tipos de salida para el mapper
    job.setMapOutputKeyClass(Text.class);
    job.setMapOutputValueClass(TriIntWritable.class);

    // Declaración de tipos de salida para el reducer
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(TriIntWritable.class);

    // Archivos de entrada y directorio de salida
    FileInputFormat.addInputPath(job, new Path( input_file ));
    FileOutputFormat.setOutputPath(job, new Path( output_file ));
    
    // Aquí podemos elegir el numero de nodos Reduce
    // Dejamos 1 para que toda la salida se guarde en el mismo fichero 'part-r-00000'
    job.setNumReduceTasks(1);

    // Ejecuta la tarea y espera a que termine. El argumento boolean es para 
    // indicar si se quiere información sobre de progreso (verbosity)
    System.exit(job.waitForCompletion(true) ? 0 : 1);
  }
}

