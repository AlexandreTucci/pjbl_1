package q4_fluxo;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

// Número de transações por tipo de fluxo (Export, Import, etc.)

public class FluxoMapper extends Mapper<LongWritable, Text, Text, LongWritable> {

    private static final LongWritable UM = new LongWritable(1);
    private final Text fluxo = new Text();

    @Override
    protected void map(LongWritable key, Text value, Context context)
            throws IOException, InterruptedException {

        String linha = value.toString();

        // 1. Remove o cabeçalho
        if (linha.startsWith("country")) return;

        // 2. Divide pelo separador ";"
        String[] campos = linha.split(";");

        // 3. Trata dados faltantes (precisa ter pelo menos 5 colunas para acessar flow)
        if (campos.length < 5) return;

        String flow = campos[4].trim();

        // 4. Trata fluxo vazio ou inválido
        if (flow.isEmpty()) return;

        fluxo.set(flow);
        context.write(fluxo, UM);
    }
}

