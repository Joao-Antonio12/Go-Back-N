/*
João Antonio Siqueira Pascuini 2024.1.08.028

Classe de um datagrama trocado de emissor e receptor:

1. Cria o pacote a partir de dados (tipo, sequencia, payload)
2. Transforma o pacote em bytes para serem enviados pela rede
3. Renconstrói o pacote a partir de bytes recebidos da rede -> 11 bytes de cabeçalho

*/
import java.nio.ByteBuffer; //Facilita montar e desmontar os bytes
import java.nio.charset.StandardCharsets; //Converter texto em bytes

public class Datagrama {

    //Tipos possiveis de pacote
    public static final byte TIPO_DATA = 0;
    public static final byte TIPO_ACK = 1;
    public static final byte TIPO_HANDSHAKE = 2;
    public static final byte TIPO_FIN = 3;

    //TAM max de dados que um pacote pode carregar
    public static final int TAMANHO_MAX_PAYLOAD = 1024;

    //TAM cabecalho: 1 + 4 + 4 + 2 = 11B
    public static final int TAMANHO_CABECALHO = 11;

    //Campos do pacote
    private byte tipo;
    private int numSeq; //id de ordem
    private int numAck; //so usado em pcts de confirmacao
    private short tamanhoDados;
    private byte[] dados;

    //Construtor, cria um novo pacote com os campos informados

    public Datagrama(byte tipo, int numSeq, int numAck, byte[] dados){
        this.tipo = tipo;
        this.numSeq = numSeq;
        this.numAck = numAck;

        //Se nao tiver dados, usa array vazio, evita erros
        if (dados == null){
            this.dados = new byte[0];
        } else{
            this.dados = dados;
        }

        //Calcula o tamanho automaticamente a partir do array dados
        this.tamanhoDados = (short) this.dados.length;

    }

    public byte[] serializar(){ //tranforma o objeto em uma sequencia de bytes
        //buffer do TAM exato
        ByteBuffer buffer = ByteBuffer.allocate(TAMANHO_CABECALHO + dados.length);

        //Escreve cada campo na ordem correta (1, 4, 4, 2, variavel)
        buffer.put(tipo);
        buffer.putInt(numSeq);
        buffer.putInt(numAck);
        buffer.putShort(tamanhoDados);
        buffer.put(dados);

        //Retorna o array de bytes para ser enviado
        return buffer.array();
    }

    public static Datagrama deserializar (byte[] bytesRecebidos, int tamanhoRecebido){ //estatico pq ainda n existe o objeto montado
        //Le campo por campo enfiando os bytes em um buffer
        ByteBuffer buffer = ByteBuffer.wrap(bytesRecebidos, 0, tamanhoRecebido);

        //Le na mesma ordem que foi escrito
        byte tipo = buffer.get();
        int numSeq = buffer.getInt();
        int numAck = buffer.getInt();
        short tamanhoDados = buffer.getShort();

        //Le dados restantes
        byte[] dados = new byte[tamanhoDados];
        buffer.get(dados, 0, tamanhoDados);

        //Cria e retorna o objeto montado
        return new Datagrama(tipo, numSeq, numAck, dados);
    }

    /*
    Criacao do pacote especial de handshake, é o 1º pacote enviado ao receptor
    Carrega a prob. de perda e o caminho onde salvar o arq
    Formato do payload -> "0.1|/tmp/arquivo.jpg"
    */
    public static Datagrama criarHandshake(double probPerda, String pathDestino){
        //JUnta os dois textos separados por |
        String payloadTexto = probPerda + "|" + pathDestino;

        //Converte texto pra byte
        byte[] payloadBytes = payloadTexto.getBytes(StandardCharsets.UTF_8);

        //retorno do datagrama tipo handshake
        return new Datagrama(TIPO_HANDSHAKE, 0, 0, payloadBytes);

    }
    //Getters
    public byte getTipo() { return tipo; }
    public int getNumSeq() { return numSeq; }
    public int getNumAck() { return numAck; }
    public short getTamanhoDados() { return tamanhoDados; }
    public byte[] getDados() { return dados; }
}
