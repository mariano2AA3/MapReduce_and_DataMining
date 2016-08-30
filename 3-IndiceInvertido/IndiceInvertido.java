/**
 *	Asignatura:  DGPSI (Master Ingenieria Informatica)
 *	Practica:    1
 *	Autores:     Jorge Casas Hernan y Mariano Hernandez Garcia
 *	Declaracion: Declaramos que hemos realizado este documento nosotros
 *		     mismos de manera exclusiva y sin compartir nada con
 *		     otros grupos. 
 */

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

class ValueComparator implements Comparator<String> {
    Map<String, Integer> base;

    public ValueComparator(Map base) {
        this.base = base;
    }

    @Override
    public int compare(String a, String b) {
        if (base.get(a) >= base.get(b)) {
            return -1;
        } else {
            return 1;
        }
    }
}

public class IndiceInvertido {

  private final static String input_file1 = "Adventures_of_Huckleberry_Finn.txt";
  private final static String input_file2 = "Hamlet.txt";
  private final static String input_file3 = "Moby_Dick.txt";
  private final static String output_file = "salida_indiceInvertido";

  
  public static class TokenizerMapper extends Mapper<Object, Text, Text, Text>{
    
    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {

      StringTokenizer itr = new StringTokenizer(value.toString());
      String palabra = "";
      while(itr.hasMoreTokens()) {
        palabra = itr.nextToken().toLowerCase();

        // Limpiamos la palabra
        palabra = palabra.replace(",", "").replace("'", "").replace("\"", "").replace(".", "").replace(":", "").replace(";", "").replace("!", "").replace("?", "").replace("_", "").replace("~", "");

        FileSplit fs = (FileSplit)context.getInputSplit();
        context.write(new Text(palabra), new Text( fs.getPath().getName() ) );
      }

    }
  }
  
  
  public static class IndiceReducer extends Reducer<Text,Text,Text,Text> {

    public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {

      HashMap<String,Integer> map = new HashMap<String,Integer>();
      ValueComparator bvc = new ValueComparator(map);
      TreeMap sorted_map = new TreeMap(bvc);
      String results = "";
      boolean moreThan20 = false;

      for ( Text book_name : values ) {
        Object num = map.get(book_name.toString());
        if ( num != null )
          map.put(book_name.toString(), (int)num + 1);
        else
          map.put(book_name.toString(), 1);
      }

      sorted_map.putAll(map);
      Set<String> setKey = sorted_map.keySet();
      for(String book_name_map : setKey){
        int value = map.get(book_name_map);
        if ( map.get(book_name_map) >= 20 )
          moreThan20 = true;

        results += "(" + book_name_map + ", " + value + "), ";

      }

      if ( moreThan20 )
          context.write(key, new Text(results.substring(0, results.length() - 2)) ); 

    
      
    }
  }

  public static void main(String[] args) throws Exception {
    JobConf conf = new JobConf();
    Job job = Job.getInstance(conf);
    job.setJarByClass(IndiceInvertido.class);
    job.setMapperClass(TokenizerMapper.class);
    
    
    job.setReducerClass(IndiceReducer.class);

    // Declaración de tipos de salida para el mapper
    job.setMapOutputKeyClass(Text.class);
    job.setMapOutputValueClass(Text.class);

    // Declaración de tipos de salida para el reducer
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(Text.class);

    // Archivos de entrada y directorio de salida
    FileInputFormat.addInputPath(job, new Path( input_file1 ));
    FileInputFormat.addInputPath(job, new Path( input_file2 ));
    FileInputFormat.addInputPath(job, new Path( input_file3 ));
    FileOutputFormat.setOutputPath(job, new Path( output_file ));
    
    // Aquí podemos elegir el numero de nodos Reduce
    // Dejamos 1 para que toda la salida se guarde en el mismo fichero 'part-r-00000'
    job.setNumReduceTasks(1);

    // Ejecuta la tarea y espera a que termine. El argumento boolean es para 
    // indicar si se quiere información sobre de progreso (verbosity)
    System.exit(job.waitForCompletion(true) ? 0 : 1);
  }
}

