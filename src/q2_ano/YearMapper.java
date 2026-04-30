package q2_ano;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * Q2 - Número de transações por ano
 *
 * Mapper: para cada linha do CSV, emite (ano, 1).
 *
 * Entrada:  cada linha do CSV
 * Saída:    ("2016", 1), ("2015", 1), ...
 */
public class YearMapper extends Mapper<LongWritable, Text, Text, LongWritable> {

    private static final LongWritable UM = new LongWritable(1);
    private final Text ano = new Text();

    @Override
    protected void map(LongWritable key, Text value, Context context)
            throws IOException, InterruptedException {

        String linha = value.toString();

        // 1. Remove o cabeçalho
        if (linha.startsWith("Country")) return;

        // 2. Divide pelo separador ";"
        String[] campos = linha.split(";");

        // 3. Trata dados faltantes
        if (campos.length < 10) return;

        String year = campos[1].trim();

        // 4. Trata ano vazio ou inválido
        if (year.isEmpty()) return;

        ano.set(year);
        context.write(ano, UM);
    }
}