/**
 *	Asignatura:	DGPSI (Master Ingenieria Informatica)
 *	Practica:	1
 *	Autores:	Jorge Casas Hernan y Mariano Hernandez Garcia
 *	Declaracion:    Declaramos que hemos realizado este documento nosotros
 *	                mismos de manera exclusiva y sin compartir nada con
 *			otros grupos. 
 */

import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class Temperatura {

  private final static String input_file = "JCMB_last31days.csv";
  private final static String output_file = "salida";

  
  public static class TokenizerMapper extends Mapper<Object, Text, Text, FloatWritable>{
    
    private Text word = new Text();
      
    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {

      String[] values = value.toString().split(",");
      context.write(new Text(values[2]), new FloatWritable(Float.parseFloat(values[8])));

    }
  }
  
  
  public static class FloatTempReducer extends Reducer<Text,FloatWritable,Text,Text> {

    public void reduce(Text key, Iterable<FloatWritable> values, Context context) throws IOException, InterruptedException {

      float max_float = Float.MIN_VALUE;
      float min_float = Float.MAX_VALUE;

      for (FloatWritable val : values) {
        float temp = val.get();

        if ( temp > max_float ) max_float = temp;
        else if ( temp < min_float ) min_float = temp;

      }

      context.write(key, new Text("(" + min_float + ", " + max_float + ")"));  
      
    }
  }

  public static void main(String[] args) throws Exception {
    JobConf conf = new JobConf();
    Job job = Job.getInstance(conf);
    job.setJarByClass(Temperatura.class);
    job.setMapperClass(TokenizerMapper.class);
    //Si existe combinador
    //job.setCombinerClass(Clase_del_combinador.class);
    job.setReducerClass(FloatTempReducer.class);

    // Declaración de tipos de salida para el mapper
    job.setMapOutputKeyClass(Text.class);
    job.setMapOutputValueClass(FloatWritable.class);
    // Declaración de tipos de salida para el reducer
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(FloatWritable.class);

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

