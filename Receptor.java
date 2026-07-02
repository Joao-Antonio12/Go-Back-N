/*
João Antonio Siqueira Pascuini 2024.1.08.028

Receptor do protocolo Go-Back-N:

1. Espera um handshake do Emissor para saber os parametros
2. Recebe pacotes, aceita somente os que chegam em ordem (FSM ou GBN)
3. Simula a perda de pacotes e mostra as métricas

Roda antes do emissor, fica esperando parado
Uso: java Receptor
*/

import java.io.*; //FileOutputStream p/ salvar o arquivo
import java.net.*; //DatagramSocket e DatagramPocket p/ comunicacao UDP
import java.util.Random; //Sorteio de perda simulada


public class Receptor {

    public static void main(String[] args) throws Exception{
        //Porta que o receptor fica ouvindo -> 5000
        int porta = 5000;

        //Cria o socket UDP nessa porta
        DatagramSocket socket = new DatagramSocket(porta);
        System.out.println("Receptor aguardando na porta " + porta + "...");

        //Buffer de recebimento de pacotes (TAM = cabeçalho + payload max)
        byte[] bufferReceber = new byte[Datagrama.TAMANHO_CABECALHO + Datagrama.TAMANHO_MAX_PAYLOAD];

        //Passo 1 -> Esperar e processar o handshake

        //Fica travado até chegar o primeiro pacote
        DatagramPacket pacoteRecebido = new DatagramPacket(bufferReceber, bufferReceber.length);
        socket.receive(pacoteRecebido);

        //TRanforma os bytes recebidos num Datagrama
        Datagrama handshake = Datagrama.deserializar(pacoteRecebido.getData(), pacoteRecebido.getLength());

        //Guarda o endereço do emissor para envio do ACK de volta
        InetAddress enderecoEmissor = pacoteRecebido.getAddress();
        int portaEmissor = pacoteRecebido.getPort();

        //payload do handshake é um texto
        String payloadTexto = new String(handshake.getDados(), "UTF-8");
        String[] partes = payloadTexto.split("\\|");
        double probPerda = Double.parseDouble(partes[0]);
        String pathDestino = partes[1];

        System.out.println("Handshake recebido!");
        System.out.println("Probabilidade de perda: " + (probPerda * 100) + "%");
        System.out.println("Salvar em: " + pathDestino);

        //Manda um ACK de volta pro emissor saber que pode começar
        Datagrama ackHandshake = new Datagrama(Datagrama.TIPO_ACK, 0, 0, null);
        byte[] bytesAck = ackHandshake.serializar();
        socket.send(new DatagramPacket(bytesAck, bytesAck.length, enderecoEmissor, portaEmissor));

        //Passo 2: Recebimento de dados usando a FSM do receptor GBN

        //Abre o arq onde serao salvos os dados recebidos
        FileOutputStream arquivoSaida = new FileOutputStream(pathDestino);

        //Geracao aleatoria pra simular perda
        Random random = new Random();

        //VAriaveis
        int expectedSeqNum = 0; //N de seq que estamos esperando, aceita só esse

        //COntadores de estatísticas
        int totalRecebidos = 0;
        int totalDescartados = 0; //"perdidos"
        int totalForadeOrdem = 0;

        boolean transferindo = true;

        while (transferindo){
            //Espera o prox pacote chegar
            pacoteRecebido = new DatagramPacket(bufferReceber, bufferReceber.length);
            socket.receive(pacoteRecebido);

            //Converte bytes em datagramas
            Datagrama pkt = Datagrama.deserializar(pacoteRecebido.getData(), pacoteRecebido.getLength());

            //Se for FIN, acabou a transferência
            if (pkt.getTipo() == Datagrama.TIPO_FIN) {
                System.out.println("FIN recebido. Tranferência concluída!");

                //Confirma o FIN pro emissor
                Datagrama ackFin = new Datagrama(Datagrama.TIPO_ACK, 0, pkt.getNumSeq(), null);
                byte[] bytesFin = ackFin.serializar();
                socket.send(new DatagramPacket(bytesFin, bytesFin.length, enderecoEmissor, portaEmissor));

                transferindo = false;
                continue; //sai do loop
            }
            //Se nao é DATA, ignora
            if (pkt.getTipo() != Datagrama.TIPO_DATA){
                continue;
            }

            if (pkt.getNumSeq() == expectedSeqNum){
                //Caso 1: Pacote chega na ordem certa, antes de aceitar simula perda (Random [0.0, 1.0]
                double sorteio = random.nextDouble();

                if (sorteio < probPerda){ //Finge que perdeu o pct, nao envia nada
                    totalDescartados++;
                    System.out.println("(Perda Simulada) Pacote: " + pkt.getNumSeq());
                }else{
                    arquivoSaida.write(pkt.getDados(), 0, pkt.getTamanhoDados()); //Salva os byes num arquivo
                    totalRecebidos++;

                    //Envio do ACK confirmando esse numero de sequencia
                    Datagrama ack = new Datagrama(Datagrama.TIPO_ACK, 0, expectedSeqNum, null);
                    byte[] bytesAckData = ack.serializar();
                    socket.send(new DatagramPacket(
                            bytesAckData, bytesAckData.length,
                            enderecoEmissor, portaEmissor
                    ));

                    System.out.println("Pacote" +expectedSeqNum + "OK -> ACK" + expectedSeqNum);
                    //Avança, esperando o proximo
                    expectedSeqNum++;
                }
            }else{
                //Caso 2: Pacote fora de ordem, o GBN descarta e reenvia o ACK do ultimo pacote aceito
                totalForadeOrdem++;
                System.out.println("(Fora de Ordem) Esperava " + expectedSeqNum + "Chegou " + pkt.getNumSeq());

                //Só reenvia ACK se foi aceito pelo menos 1 pacote
                if (expectedSeqNum > 0){
                    Datagrama ackRepetido = new Datagrama(
                            Datagrama.TIPO_ACK, 0, expectedSeqNum - 1, null
                    );
                    byte[] bytesRep = ackRepetido.serializar();
                    socket.send(new DatagramPacket(
                            bytesRep, bytesRep.length,
                            enderecoEmissor, portaEmissor
                    ));
                }
            }
        }
    //Passo 3: Fecha tudo e mostra estatísticas
        arquivoSaida.close();
        socket.close();

        System.out.println("\nESTATISTICAS:");
        System.out.println("Pacotes recebidos com sucesso: " + totalRecebidos);
        System.out.println("Pacotes descartados (perda simulada): " + totalDescartados);
        System.out.println("Pacotes fora de ordem: " + totalForadeOrdem);

        //Calcula a taxa de perda efetiva
        double taxaPerda = 0;
        if (totalRecebidos + totalDescartados > 0){
            taxaPerda = (double) totalDescartados / (totalRecebidos + totalDescartados) * 100;
            System.out.println("Taxa de perda efetiva: " + String.format("%.2f", taxaPerda) + "%");
        }
    }
}