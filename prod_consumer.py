import threading
import time
import queue
import random


# implementação de fila para controlar starvation

class buffer_memoria: 
    def __init__(self, tamanho_maximo = 3):
        self.buffer = []
        self.tamanho_maximo = tamanho_maximo

        self.fila = queue.Queue()
        ##espaços disponíveis na memória
        self.buffer_vazio = threading.Semaphore(tamanho_maximo)
        self.dados_disponivel = threading.Semaphore(0) # dado disponivel
        #mutex - controlar acesso
        self.controle_acesso_buffer = threading.Lock() 

        self.stats = {}
        self.stats_lock = threading.Lock()
        self.continuar = True

    def registrar_consumer(self, nome): 
        #adquire lock e libera ao final
         with self.stats_lock: 
            self.stats[nome] = {
                'tentativas' : 0, 
                'sucesso' : 0, 
                'tempo_espera' : 0, 
                'ultimo_acesso': time.time()
            }

    def produzir_dado(self, nome_producer, dado):

        
        #aguarda a disponibilidade de recurso
        self.buffer_vazio.acquire()
        
        with self.controle_acesso_buffer:
            
            if self.continuar: 
                
                #adiciona dado no buffer
                self.buffer.append(dado)
                print(f" Producer '{nome_producer}': Produziu '{dado}' | Cesta: {len(self.buffer)}/{self.tamanho_maximo}")
               
                #se a fila estiver cheia chama o proximo da fila
                if not self.fila.empty():
                    proximo_evento = self.fila.get()
                    proximo_evento.set()
                else:
                    self.dados_disponivel.release()
                    return True
                
        return False
    
    def consumir_dado(self, nome_consumer): 
        inicio_espera = time.time()

        with self.stats_lock: 

            #incrementa o número de tentativas de acesso
            self.stats[nome_consumer]['tentativas'] +=1

            evento = threading.Event()

            #adiciona o eventeo a fila de espera
            self.fila.put(evento)
            print(f"{nome_consumer}:Entrou na fila de espera (posição {self.fila.qsize()})")

            #tenta acessar um dado disponivel ou esperar até liberar
            pegou_dado = False

            if self.dados_disponivel.acquire(blocking=False):
                pegou_dado = True

                # Remove da fila já que conseguiu diretamente
                try:
                    self.fila_espera.get_nowait()
                except queue.Empty:
                    pass
            else: 
                #aguarda na fila por 5 segundos
                print(f"{nome_consumer}: aguardando turno...")
                pegou_dado = evento.wait(timeout=5)

            if pegou_dado: 
                with self.controle_acesso_buffer: 
                    if len(self.buffer) > 0 and self.continuar: 
                        dado = self.buffer.pop(0)
                        tempo_espera = time.time() - inicio_espera

                        # Atualiza estatísticas
                        with self.stats_lock:
                            self.stats[nome_consumer]['sucessos'] += 1
                            self.stats[nome_consumer]['tempo_espera'] += tempo_espera
                            self.stats[nome_consumer]['ultimo_acesso'] = time.time()

                        print(f"{nome_consumer}: Consegui '{dado}'! (esperei {tempo_espera:.1f}s)")
                        print(f"Cesta: {len(self.buffer)}/{self.tamanho_maximo}")

                        #libera espaço 
                        self.buffer_vazio.release()
                        return dado
                    
                    else: 
                        self.dados_disponivel.release()
            else: 
                print(f"😋 {nome_consumer}: ⚠️  TIMEOUT! Possivel starvation!")

            return None

    #exibir estatisticas de acesso    
    def exibir_estatisticas(self):
        print("\n📊 ESTATÍSTICAS:")
        print("-" * 50)


# aplicando sistema de turnos 

class sis_turnos:
    def __init__(self, consumers):
        self.consumers = consumers
        self.turno_atual = 0
        self.turno_lock =  threading.Lock()
        self.eventos = {nome: threading.Event() for nome in consumers}
       
        # Começa com o primeiro consumido
        self.eventos[consumers[0]].set()

    def aguarda_turno(self, nome):
        return self.eventos[nome].wait(timeout=3)
    
    def passar_turno(self, turno_atual): 
        with self.turno_lock:
            #limpa o evento atual
            self.eventos[turno_atual].clear()

            #Passa para o proximo
            self.turno_atual = (self.turno_atual + 1) % len(self.consumers)
            prox = self.consumers[self.turno_atual]
            self.eventos[prox].set()

            print(f"Troca de turno! Turno passou de {turno_atual} para {prox}")


# função dos producers

def producer(buffer_memoria, nome = "producer 1"):
    dados_gerados = 0

    while buffer_memoria.continuar and dados_gerados < 8:
        # gerando dado
        print(f"{nome}: Gerando dado...")
        time.sleep(random.uniform(1,2))
        dado = f"Dado-{nome}-{dados_gerados+ 1}"

        if buffer_memoria.produzir_dado(nome, dado):
            dados_gerados +=1
            print(f"{nome}: dado gerado\n")

def consumer(buffer_memoria, nome): 
    dados_consumidos = 0

    while buffer_memoria.continuar and dados_consumidos < 4: 
        dado =  buffer_memoria.consumir_dado(nome)

        if dado: 
            tempo_de_consumo = random.uniform(0.5 , 1.5)
            print(f"{nome}: consumindo dado {dado} ({tempo_de_consumo:.1f}s)")
            dados_consumidos += 1

        else: 
            print(f"{nome}: Dado não consumido! Tentando novamente!\n")
            time.sleep(0.5)

def main(): 

    input("Pressione ENTER para começar...")
    print("=" * 50)

    buffer = buffer_memoria(tamanho_maximo=2)

    consumers = ["cons 1", "cons 2", "cons 3"]

    for nome in consumers: 
        buffer.registrar_consumer(nome)

    threads = []

    t_producer =  threading.Thread(target=producer, args=(buffer, "cons 1"))
    threads.append(t_producer)

    for nome in consumers: 
        t = threading.Thread(target=producer, args=(buffer, nome))
        threads.append(t)
   
    # Monitora por 12 segundos
    time.sleep(12)
    
    # Para execução
    buffer.continuar = False
    
    # Espera threads terminarem
    for t in threads:
        t.join(timeout=2)
    
if __name__ == "__main__":
    main()





      









        



