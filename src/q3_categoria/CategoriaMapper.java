package q3_categoria;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

// Quantidade de transações por Categoria

public class CategoriaMapper extends Mapper<LongWritable, Text, Text, LongWritable> {

    private static final LongWritable UM = new LongWritable(1);
    private final Text categoria = new Text();

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String linha = value.toString();

        // 1. Remove o cabeçalho
        if (linha.startsWith("Country")) return;

        // 2. Divide pelo separador ";"
        String[] campos = linha.split(";");

        // 3. Trata dados faltantes
        if (campos.length < 10) return;

        String cat = campos[9].trim();

        // 4. Trata categoria vazia
        if (cat.isEmpty()) return;

        categoria.set(cat);
        context.write(categoria, UM);
    }
}
