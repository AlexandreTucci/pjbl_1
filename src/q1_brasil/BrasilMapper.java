package q1_brasil;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * Q1 - Número de transações envolvendo o Brasil
 *
 * Mapper: para cada linha do CSV, verifica se o país (coluna 0) é "Brazil".
 * Se sim, emite (chave="Brazil", valor=1).
 *
 * Entrada:  cada linha do CSV
 * Saída:    ("Brazil", 1)
 */
public class BrasilMapper extends Mapper<LongWritable, Text, Text, LongWritable> {

    private static final LongWritable UM = new LongWritable(1);
    private static final Text BRASIL = new Text("Brazil");

    @Override
    protected void map(LongWritable key, Text value, Context context)
            throws IOException, InterruptedException {

        String linha = value.toString();

        // 1. Remove o cabeçalho (primeira linha do CSV)
        if (linha.startsWith("Country")) return;

        // 2. Divide a linha pelo separador ";"
        String[] campos = linha.split(";");

        // 3. Trata dados faltantes (linha com menos de 10 colunas)
        if (campos.length < 10) return;

        String pais = campos[0].trim();

        // 4. Filtra somente transações do Brasil
        if (pais.equalsIgnoreCase("Brazil")) {
            context.write(BRASIL, UM);
        }
    }
}