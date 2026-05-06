package q5_media_brasil;

import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * Q5 - Writable customizado para calcular média
 *
 * Armazena:
 * - soma: soma total dos preços
 * - contagem: número de transações
 *
 * A média é calculada no Reducer: soma / contagem
 */
public class MediaWritable implements Writable {

    private double soma;
    private long contagem;

    // Construtor padrão obrigatório para deserialização
    public MediaWritable() {
        this.soma = 0.0;
        this.contagem = 0;
    }

    public MediaWritable(double soma, long contagem) {
        this.soma = soma;
        this.contagem = contagem;
    }

    // Serialização — escrita no disco/rede
    @Override
    public void write(DataOutput out) throws IOException {
        out.writeDouble(soma);
        out.writeLong(contagem);
    }

    // Desserialização — leitura do disco/rede
    @Override
    public void readFields(DataInput in) throws IOException {
        this.soma = in.readDouble();
        this.contagem = in.readLong();
    }

    public double getSoma() { return soma; }
    public long getContagem() { return contagem; }

    public void setSoma(double soma) { this.soma = soma; }
    public void setContagem(long contagem) { this.contagem = contagem; }

    @Override
    public String toString() {
        if (contagem == 0) return "0.00";
        return String.format("%.2f", soma / contagem);
    }
}