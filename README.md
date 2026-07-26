# Simulação do Problema Produtor-Consumidor e Controle de Starvation em Sistemas Operacionais

Este projeto foi desenvolvido como uma atividade prática da disciplina de **Sistemas Operacionais**. Ele demonstra a resolução do problema clássico de sincronização **Produtor-Consumidor (Producer-Consumer)** utilizando primitiva de concorrencia nativa do Python, além de implementar estratégias para mitigação e monitoramento de **Starvation**.

---

## 📌 Objetivos

1. **Sincronização de Threads:** Demonstrar a coordenação entre múltiplas threads produtoras e consumidoras compartilhando uma região de memória limitada (buffer circular).
2. **Exclusão Mútua:** Garantir que o acesso ao buffer compartilhado ocorra sem *race conditions* (condições de corrida).
3. **Prevenção de Starvation:** Utilizar estruturas de filas de espera (`queue.Queue`) e sincronização baseada em eventos (`threading.Event`) para ordenar as solicitações de consumo e minimizar a inanição de processos.

---

## 🛠️ Conceitos de Sistemas Operacionais Aplicados

- **Semáforos (`threading.Semaphore`):**
  - `buffer_vazio`: Controla a quantidade de posições livres disponíveis no buffer.
  - `dados_disponivel`: Sinaliza a presença de itens prontos para serem consumidos.
- **Mutex / Locks (`threading.Lock`):**
  - `controle_acesso_buffer`: Garante exclusão mútua durante a inserção e remoção de elementos do buffer.
  - `stats_lock`: Garante concorrência segura na escrita de métricas e estatísticas do sistema.
- **Starvation & Fairness:**
  - Implementação de uma fila FIFO de eventos para organizar o atendimento das threads consumidoras, prevenindo que uma mesma thread monopolize os recursos enquanto outras aguardam indefinidamente.
- **Timeouts:**
  - Limite temporal de espera para evitar deadlock e detectar possíveis cenários de inanição.

