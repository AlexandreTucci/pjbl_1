import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class Main {

    private static final int QUESTAO = 8;

    private static final String INPUT_PATH  = "data/operacoes_comerciais_inteira.csv";

    private static final String OUTPUT_PATH = "output/questao" + QUESTAO;

    public static void main(String[] args) throws Exception {

        String tmpDir = System.getProperty("user.dir") + "/tmp";
        new java.io.File(tmpDir).mkdirs();

        System.setProperty("hadoop.tmp.dir", tmpDir);
        System.setProperty("hadoop.home.dir", "C:/hadoop-3.2.1");

        Configuration conf = new Configuration();
        conf.set("fs.defaultFS", "file:///");
        conf.set("mapreduce.framework.name", "local");
        conf.set("mapreduce.app-submission.cross-platform", "true");
        conf.setBoolean("dfs.permissions.enabled", false);
        conf.set("hadoop.tmp.dir", tmpDir);
        conf.set("mapred.local.dir", tmpDir + "/mapred/local");
        conf.set("mapreduce.cluster.local.dir", tmpDir + "/mapred/local");
        conf.set("mapreduce.jobtracker.staging.root.dir", tmpDir + "/staging");
        conf.set("mapreduce.jobtracker.system.dir", tmpDir + "/mapred/system");

        new java.io.File(tmpDir + "/mapred/local").mkdirs();
        new java.io.File(tmpDir + "/mapred/system").mkdirs();
        new java.io.File(tmpDir + "/staging").mkdirs();

        Job job = criarJob(conf);

        if (job == null) {
            System.err.println("Questão inválida: " + QUESTAO);
            System.exit(1);
        }

        FileInputFormat.addInputPath(job, new Path(INPUT_PATH));
        FileOutputFormat.setOutputPath(job, new Path(OUTPUT_PATH));

        System.out.println("  Executando Questão " + QUESTAO);
        System.out.println("  Input:  " + INPUT_PATH);
        System.out.println("  Output: " + OUTPUT_PATH);

        // Executa o job e aguarda conclusão
        boolean sucesso = job.waitForCompletion(true);
        System.exit(sucesso ? 0 : 1);
    }

    // Retorna o job correspondente à questão selecionada.
    private static Job criarJob(Configuration conf) throws Exception {
        switch (QUESTAO) {
            
            // Q1 - Quantidade de transações envolvendo o Brasil
            case 1:
                // TODO: descomente quando implementar Q1
                 Job job1 = Job.getInstance(conf, "Q1 - Transações Brasil");
                 job1.setJarByClass(Main.class);
                 job1.setMapperClass(q1_brasil.BrasilMapper.class);
                 job1.setReducerClass(q1_brasil.BrasilReducer.class);
                 job1.setOutputKeyClass(Text.class);
                 job1.setOutputValueClass(LongWritable.class);
                 return job1;
            
            // Q2 - Quantidade de transações por ano
            case 2:
                Job job2 = Job.getInstance(conf, "Q2 - Transacoes por Ano");
                job2.setJarByClass(Main.class);
                job2.setMapperClass(q2_ano.AnoMapper.class);
                job2.setReducerClass(q2_ano.AnoReducer.class);
                job2.setOutputKeyClass(Text.class);
                job2.setOutputValueClass(LongWritable.class);
                return job2;
            
            // Q3 - Quantidade de transações por categoria
            case 3:
                // TODO: descomente quando implementar Q3
                 Job job3 = Job.getInstance(conf, "Q3 - Transações por Categoria");
                 job3.setJarByClass(Main.class);
                 job3.setMapperClass(q3_categoria.CategoriaMapper.class);
                 job3.setReducerClass(q3_categoria.CategoriaReducer.class);
                 job3.setOutputKeyClass(Text.class);
                 job3.setOutputValueClass(LongWritable.class);
                 return job3;

            
            // Q4 - Número de transações por tipo de fluxo
            case 4:
                // TODO: descomente quando implementar Q4
                 Job job4 = Job.getInstance(conf, "Q4 - Transações por Flow");
                 job4.setJarByClass(Main.class);
                 job4.setMapperClass(q4_fluxo.FluxoMapper.class);
                 job4.setReducerClass(q4_fluxo.FluxoReducer.class);
                 job4.setOutputKeyClass(Text.class);
                 job4.setOutputValueClass(LongWritable.class);
                 return job4;

            
            // Q5 - Valor médio das transações por ano no Brasil
            //      (requer Writable customizado)
            case 5:
                Job job5 = Job.getInstance(conf, "Q5 - Media por Ano no Brasil");
                job5.setJarByClass(Main.class);
                job5.setMapperClass(q5_media_brasil.MediaBrasilMapper.class);
                job5.setReducerClass(q5_media_brasil.MediaBrasilReducer.class);
                job5.setOutputKeyClass(Text.class);
                job5.setOutputValueClass(q5_media_brasil.MediaWritable.class);
                return job5;

            
            // Q6 - Transação mais cara e mais barata no Brasil em 2016
            //      (Combiner obrigatório + Writable customizado)

            case 6:
                Job job6 = Job.getInstance(conf, "Q6 - MinMax Brasil 2016");
                job6.setJarByClass(Main.class);
                job6.setMapperClass(q6_minmax_brasil_2016.MinMaxMapper.class);
                job6.setCombinerClass(q6_minmax_brasil_2016.MinMaxCombiner.class);
                job6.setReducerClass(q6_minmax_brasil_2016.MinMaxReducer.class);
                job6.setOutputKeyClass(Text.class);
                job6.setOutputValueClass(q6_minmax_brasil_2016.MinMaxWritable.class);
                return job6;

            
            // Q7 - Valor médio por ano, somente Export no Brasil
            //      (Combiner obrigatório)

            case 7:
                Job job7 = Job.getInstance(conf, "Q7 - Media Export Brasil por Ano");
                job7.setJarByClass(Main.class);
                job7.setMapperClass(q7_media_export_brasil.MediaExportMapper.class);
                job7.setCombinerClass(q7_media_export_brasil.MediaExportCombiner.class);
                job7.setReducerClass(q7_media_export_brasil.MediaExportReducer.class);
                job7.setOutputKeyClass(Text.class);
                job7.setOutputValueClass(q7_media_export_brasil.MediaWritable.class);
                return job7;

            
            // Q8 - Maior e menor preço por ano e país
            //      (Comparable Writable + Combiner obrigatórios)

            case 8:
                Job job8 = Job.getInstance(conf, "Q8 - MinMax por Ano e Pais");
                job8.setJarByClass(Main.class);
                job8.setMapperClass(q8_minmax_ano_pais.MinMaxPaisMapper.class);
                job8.setCombinerClass(q8_minmax_ano_pais.MinMaxPaisCombiner.class);
                job8.setReducerClass(q8_minmax_ano_pais.MinMaxPaisReducer.class);
                job8.setOutputKeyClass(q8_minmax_ano_pais.AnoPaisWritable.class);
                job8.setOutputValueClass(q8_minmax_ano_pais.MinMaxPaisWritable.class);
                return job8;

            default:
                return null;
        }
    }
}