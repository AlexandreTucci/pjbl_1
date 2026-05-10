package q5_media_brasil;

import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class MediaWritable implements Writable {

    private double soma;
    private long contagem;

    public MediaWritable() {
        this.soma = 0.0;
        this.contagem = 0;
    }

    public MediaWritable(double soma, long contagem) {
        this.soma = soma;
        this.contagem = contagem;
    }

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeDouble(soma);
        out.writeLong(contagem);
    }

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