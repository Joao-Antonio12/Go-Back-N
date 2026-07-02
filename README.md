# Go-Back-N em Java via UDP

Trabalho final da disciplina de Redes de Computadores (UNIFAL-MG).

Implementação do protocolo de transferência confiável **Go-Back-N (GBN)** sobre sockets UDP, em Java puro (sem dependências externas além do JDK). Dois módulos independentes, Emissor e Receptor, transferem um arquivo binário arbitrário entre dois hosts, com simulação de perda de pacotes no lado receptor.

## Estrutura

```
src/
├── Datagrama.java   # Formato do pacote: serialização e deserialização do cabeçalho
├── Receptor.java    # FSM do receptor GBN + simulação de perda + estatísticas
└── Emissor.java     # FSM do emissor GBN: janela deslizante, timer e retransmissão
```

Formato do datagrama (11 bytes de cabeçalho + até 1024 bytes de payload):

| Campo | Tamanho | Descrição |
|---|---|---|
| tipo | 1 byte | 0=DATA, 1=ACK, 2=HANDSHAKE, 3=FIN |
| num_seq | 4 bytes | Número de sequência |
| num_ack | 4 bytes | Número de confirmação (ACKs) |
| tamanho_dados | 2 bytes | Bytes válidos no payload |
| dados | até 1024 bytes | Payload |

## Requisitos

- JDK 17 ou superior (testado com JDK 26)

## Compilação

```bash
cd src
javac Datagrama.java Receptor.java Emissor.java
```

## Execução

**1. Inicie o Receptor primeiro** (fica aguardando na porta 5000):

```bash
java Receptor
```

**2. Em outro terminal, inicie o Emissor:**

```bash
java Emissor <arquivo_origem> <IP_destino>:<path_destino> <tamanho_janela> <prob_perda>
```

Exemplo (transferência local com janela 4 e 10% de perda simulada). Os caminhos abaixo são só ilustrativos — funciona com qualquer pasta existente no sistema, Windows ou Linux:

```bash
java Emissor C:\tmp\arquivo.pdf 127.0.0.1:C:\tmp\arquivo_recebido.pdf 4 0.10
```

Parâmetros:
- `arquivo_origem`: caminho do arquivo a enviar (evite espaços no nome)
- `IP_destino:path_destino`: IP do receptor e caminho absoluto onde salvar
- `tamanho_janela`: tamanho N da janela deslizante (ex.: 1, 4, 8, 16)
- `prob_perda`: probabilidade de perda simulada, entre 0.0 e 1.0

## Verificação de integridade

Após a transferência, compare os arquivos byte a byte:

```bash
# Windows
fc /b C:\tmp\arquivo.pdf C:\tmp\arquivo_recebido.pdf

# Linux/Mac
cmp arquivo.pdf arquivo_recebido.pdf
```

## Saída

O Emissor exibe em tempo real os pacotes enviados, ACKs recebidos e retransmissões por timeout; ao final, mostra tempo total e throughput. O Receptor exibe os pacotes aceitos, perdas simuladas e pacotes fora de ordem descartados; ao final, mostra a taxa de perda efetiva (que tende à probabilidade configurada conforme o número de pacotes cresce).

## Resultados (arquivo de 1,66 MB, perda de 10%)

| Janela (N) | Tempo (s) | Retransmissões | Fora de ordem | Perda efetiva |
|---|---|---|---|---|
| 1 | 81,51 | 161 | 0 | 9,03% |
| 4 | 90,40 | 711 | 532 | 9,94% |
| 8 | 82,80 | 1305 | 1141 | 9,18% |
| 16 | 83,89 | 2636 | 2470 | 9,28% |

Análise completa no relatório técnico (PDF no repositório).

## Referências

- KUROSE, J.; ROSS, K. *Redes de Computadores: Uma Abordagem Top-Down*. 8. ed. Capítulo 3, Seções 3.4 e 3.4.3.
