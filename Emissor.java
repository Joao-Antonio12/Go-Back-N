/*
João Antonio Siqueira Pascuini 2024.1.08.028

Emissor do protocolo Go-Back-N:
1. Le o arquivo e quebra em pedaços de 1024 bytes
2. Manda um handshake pro receptor (parâmetros combinados)
3. Envia os pacotes usando janela deslizante (FSM ou GBN)
4. Manda FIN quando acabar

Uso: java Emissor <arquivo_origem> <IP_destino>:<path_destino> <tamanho_janela> <prob_perda>

*/

import java.io.*;
import java.net.*;
import java.nio.ByteBuffer;
import java.util.*;
import java.util.concurrent.*;


public class Emissor {
    //VAriáveis compartilhadas entre as threads
    static int base = 0;
    static int nextSeqNum = 0;
    static int tamanhoJanela;

    //Lock é a trava que garante que só uma thread por vez mexa em nextSeqNum
    static final Object lock = new Object();

    //Controle do timer
    static ScheduledExecutorService timerService = Executors.newSingleThreadScheduledExecutor();
    static ScheduledFuture<?> timerAtual = null;
    static final long TIMEOUT_MS = 500; // 1/2 s de timeout

    //Armazena pacotes em um ArrayList pra não precisar reler pra retransmitir
    static List<Datagrama> pacotesEnviados = new ArrayList<>();

    //Socket e endereço do receptor
    static DatagramSocket socket;
    static InetAddress enderecoDestino;
    static int portaDestino = 5000;

    //Flag de confirmacao
    static boolean tudoConfirmado = false;

    //Contadores pra estatística
    static int totalRetransmissoes = 0;
    static int totalAcksRecebeidos =0;


    public static void main(String[] args) throws Exception{
        //Passo 1: Ler os argumentos da linha de comando

        //Verifica se o user passou todos os 4 argumentos
        if (args.length != 4){
            System.out.println("Uso: java Emissor <arquivo> <IP:path_destino> <janela> <prob_perda>");
            System.out.println("Ex:  java Emissor foto.jpg 127.0.0.1:/tmp/foto.jpg 4 0.10");

            return; //encerra o programa
        }

        String arquivoOrigem = args[0]; //Caminho do arquivo a ser enviado
        String destinoCompleto = args[1]; //"IP:path" juntos

        //Separador do IP do path
        String[] partes = destinoCompleto.split(":", 2);
        String ipDestino = partes[0];
        String pathDestino = partes[1];

        tamanhoJanela = Integer.parseInt(args[2]); //Tamanho da janela N
        double probPerda = Double.parseDouble(args[3]); //Prob de perda

        System.out.println("Arquivo: " + arquivoOrigem);
        System.out.println("Destino: " + ipDestino + "-->" + pathDestino);
        System.out.println("Janela: " + tamanhoJanela);
        System.out.println("Prob. de perda: " + probPerda + "%");

        //Passo 2: Ler o arquivo e quebrar em pacotes
        //Le todos os bytes de uma vez
        File arquivo = new File(arquivoOrigem);
        byte[] bytesArquivo = new byte[(int) arquivo.length()];
        FileInputStream fis = new FileInputStream(arquivo);
        fis.read(bytesArquivo);
        fis.close();

        System.out.println("Tamanho do arquivo: " + bytesArquivo.length + " bytes");

        //Quebra em pedaços de 1024 bytes, cada um vira um datagrama
        int offset = 0; //Posicao atual no arquivo original
        int seqNum = 0; //Num de seq do pacote

        while (offset < bytesArquivo.length){
            //Calculca quanto bytes pegar (1024 ou o que sobrar no final)
            int tamanho = Math.min(Datagrama.TAMANHO_MAX_PAYLOAD, bytesArquivo.length - offset);

            //Copia o pedaço do arquivo
            byte[] pedaco = new byte[tamanho];
            System.arraycopy(bytesArquivo, offset, pedaco, 0, tamanho);

            //Cria o pacote de dados
            Datagrama pkt = new Datagrama(Datagrama.TIPO_DATA, seqNum, 0, pedaco);
            pacotesEnviados.add(pkt);

            offset += tamanho;
            seqNum++;
        }

        int totalPacotes = pacotesEnviados.size();
        System.out.println("Total de pacotes: " + totalPacotes);

        //Passo 3: Conectar e mandar handshake
        enderecoDestino = InetAddress.getByName(ipDestino);
        socket = new DatagramSocket(); //Porta aleatoria pro emissor

        //Cria e envia o handshake
        Datagrama handshake = Datagrama.criarHandshake(probPerda, pathDestino);
        byte[] bytesHs = handshake.serializar();
        socket.send(new DatagramPacket(bytesHs, bytesHs.length, enderecoDestino, portaDestino));

        //Espera o ACK do handshake
        byte[] bufferReceber = new byte[Datagrama.TAMANHO_CABECALHO + Datagrama.TAMANHO_MAX_PAYLOAD];
        DatagramPacket resposta = new DatagramPacket(bufferReceber, bufferReceber.length);
        socket.receive(resposta);
        System.out.println("Handshake confirmado, iniciando transferência");

        //MArca o tempo de inicio pra calular o troughput
        long tempoInicio = System.currentTimeMillis();

        //Passo 4: Thread que fica ouvindo ACK's
        //Emissor precisa de uma thread para envio de pacotes novos e outra para receber os ACKs

        Thread threadAck = new Thread(() ->{
            try{
                byte[] bufAck = new byte[Datagrama.TAMANHO_CABECALHO];

                while (!tudoConfirmado){
                    //Fica travada aqui até chegar um ACK
                    DatagramPacket pktAck = new DatagramPacket(bufAck, bufAck.length);
                    socket.receive(pktAck);

                    Datagrama ack = Datagrama.deserializar(pktAck.getData(), pktAck.getLength());

                    //O numAck do ACK acumulartivo indica que ele recebeu tudo até esse numero
                    int numAckRecebido = ack.getNumAck();

                    synchronized (lock){
                        //Só avança se o ACK é mais novo que a base atual
                        if(numAckRecebido >= base){
                            base = numAckRecebido + 1; //Avança janela
                            totalAcksRecebeidos++;

                            System.out.println(" ACK " + numAckRecebido + "--> base avança para " + base);

                            //Cancela o timer antigo
                            cancelarTimer();

                            //Se ainda tem pacote não confirmados, reinicia o timer
                            if (base < nextSeqNum){
                                iniciarTimer();
                            }

                            //Se a base chegou no total tudo esta confirmado
                            if (base >= totalPacotes){
                                tudoConfirmado = true;
                            }

                            //Acorda a thread principal
                            lock.notifyAll();
                        }
                    }

                }
            } catch (Exception e){
                //socket fechado = normal, acontece quando termina
                if(!tudoConfirmado){
                    e.printStackTrace();
                }
            }
        });

        //Configura como deamon se a main acabar, essa thread morre junto
        threadAck.setDaemon(true);
        threadAck.start();

        //Passo 5: Loop principal e envio de pacotes com janelas deslizantes [base .. nextSeqNum .. base+N-1]

        synchronized (lock){
            while(!tudoConfirmado){

                //Convia todos os pacotes que cabem na janela
                while (nextSeqNum < totalPacotes && nextSeqNum < base + tamanhoJanela){
                    enviarPacote(pacotesEnviados.get(nextSeqNum));
                    System.out.println("Enviado pacote " + nextSeqNum);

                    //Se for o primeiro pacote da janela, inicia o timer
                    if (nextSeqNum == base){
                        iniciarTimer();
                    }
                    nextSeqNum++;

                }

                //Espera: ou chega um ACK que leva pro notifyall, ou dá timeout
                //Essa linha congela a thread principal ate o lock.notifyAll ser chamado por alguém
                if(!tudoConfirmado){
                    lock.wait();
                }
            }
        }

        //Passo 6: Envia FIN e mostra estatística

        Datagrama fin = new Datagrama(Datagrama.TIPO_FIN, totalPacotes, 0, null);
        byte[] bytesFin = fin.serializar();
        socket.send(new DatagramPacket(bytesFin, bytesFin.length, enderecoDestino, portaDestino));
        System.out.println("\nFIN enviado.");

        long tempoFim = System.currentTimeMillis();
        double duracaoSeg = (tempoFim - tempoInicio) / 1000.0;
        double throughput = (bytesArquivo.length / 1024.0) / duracaoSeg; // KB/s

        cancelarTimer();
        timerService.shutdown();
        socket.close();

        System.out.println("Total de pacotes enviados: " + totalPacotes);
        System.out.println("ACKs recebidos: " + totalAcksRecebeidos);
        System.out.println("Retransmissoes: " + totalRetransmissoes);
        System.out.println("Tempo total: " + String.format("%.2f", duracaoSeg) + " s");
        System.out.println("Throughput: " + String.format("%.2f", throughput) + " KB/s");
    }

    //Envia um pacote pelo socket UDP

    static void enviarPacote(Datagrama pkt){
        try{
            byte[]  bytes = pkt.serializar();
            socket.send(new DatagramPacket(bytes, bytes.length, enderecoDestino, portaDestino));
        } catch (IOException e){
            e.printStackTrace();
        }
    }

    //Inicia o temporizador, se expirar (TIMEOUT_MS), retransmite todos os pacotes de base até nextSeqNum-1 -> Go-Back-N, ou seja, volta pra base e retransmite tudo
    static void iniciarTimer(){
        timerAtual = timerService.schedule(() -> {
            synchronized (lock){
                System.out.println("\n TIMEOUT! Retransmitindo de " + base + " até " + (nextSeqNum - 1));

                //Reenvia TODOS os pacotes da janela (base até nexSeqNum-1)
                for (int i = base; i < nextSeqNum; i++) {
                    enviarPacote(pacotesEnviados.get(i));
                    totalRetransmissoes++;
                    System.out.println("Retransmitindo pacote " + i);
                }
                //Reinicia o timer
                iniciarTimer();

                //Acorda a tread principal
                lock.notifyAll();

            }
        }, TIMEOUT_MS, TimeUnit.MILLISECONDS);
    }

    //Cancela o temporizador atual (se existir)
    static void cancelarTimer(){
        if (timerAtual != null){
            timerAtual.cancel(false);
            timerAtual = null;
        }
    }
}

