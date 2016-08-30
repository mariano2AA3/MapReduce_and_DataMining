/**
 *	Asignatura:  DGPSI (Master Ingenieria Informatica)
 *	Practica:    1
 *	Autores:     Jorge Casas Hernan y Mariano Hernandez Garcia
 *	Declaracion: Declaramos que hemos realizado este documento nosotros
 *		     mismos de manera exclusiva y sin compartir nada con
 *	             otros grupos. 
 */

import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.fs.Path;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;


public class Felicidad {

  private final static String input_file = "happiness.txt";
  private final static String output_file = "salida_happy";

  
  public static class TokenizerMapper extends Mapper<Object, Text, IntWritable, Text>{
    
    private Text word = new Text();
      
    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {

      String[] values = value.toString().split("\t");

      if ( Float.parseFloat(values[2]) < 2.0f && !values[5].equals("--") ) 
        context.write(new IntWritable(1), new Text( values[0] ) );

    }
  }
  
  
  public static class HappinessReducer extends Reducer<IntWritable,Text,IntWritable,Text> {

    public void reduce(IntWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException {

      String sol = "";
      int counter = 0;

      for (Text val : values) {
        sol += val.toString() + ", ";
        ++counter;

      }

      sol.substring(0, sol.length() - 2);
      context.write(new IntWritable( counter ), new Text(sol) );  
      
    }
  }

  public static void main(String[] args) throws Exception {
    JobConf conf = new JobConf();
    Job job = Job.getInstance(conf);
    job.setJarByClass(Felicidad.class);
    job.setMapperClass(TokenizerMapper.class);
    
    
    job.setReducerClass(HappinessReducer.class);

    // Declaración de tipos de salida para el mapper
    job.setMapOutputKeyClass(IntWritable.class);
    job.setMapOutputValueClass(Text.class);

    // Declaración de tipos de salida para el reducer
    job.setOutputKeyClass(IntWritable.class);
    job.setOutputValueClass(Text.class);

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

