# Trabalho Prático — Simulação Black Friday (Dashboard de Vendas)

## Contexto
Este trabalho tem como objetivo construir uma **simulação de Black Friday** com dados fluindo de ponta a ponta, desde geração/ingestão até visualização em dashboard.

## Desafio
Construir uma solução com:
- Dataset inicial e/ou gerador contínuo de dados.
- Dados de **vendas**, **KPIs da empresa** e **controladoria**.
- Dashboard único com atualização em tempo real ou quase real.

## Dashboard (obrigatório)
O dashboard deve conter as seguintes seções:

### 1) C-Level
- GMV
- Lucro Líquido
- Ticket Médio

### 2) Vendas
- GMV por vendedor
- GMV por loja
- Número de pedidos
- Ticket médio
- Desconto médio
- Top performers
- Bottom performers
- Meta vs realizado

### 3) Controladoria
- Margem bruta
- Custo
- Impacto de descontos
- Cancelamentos e devoluções
- Lucro por categoria
- Lucro por canal

## Regras
- Pode usar toda e qualquer tecnologia.
- O mais importante é ter o dado **fluindo de ponta a ponta**.

## Entregáveis esperados
- Pipeline de processamento.
- Dashboard final com as 3 seções.
- Breve explicação da arquitetura escolhida.

## Critério principal de avaliação
- Clareza do fluxo fim a fim e consistência dos indicadores apresentados.

## Arquitetura implementada

Fluxo fim a fim:

1. `generate_seed_dataset.py` gera a base inicial de pedidos e metas.
2. `bootstrap_seed.py` publica a seed no Kafka e envia os CSVs para o MinIO.
3. `stream_generator.py` continua produzindo eventos em tempo real no tópico `blackfriday_orders`.
4. `stream_processor_spark.py` consome o Kafka com Spark Structured Streaming, grava camadas Bronze e Silver em parquet e publica um snapshot Gold para o dashboard.
5. `dashboard.py` lê o snapshot consolidado e exibe as seções de C-Level, Vendas e Controladoria.

### Camadas de dados

- Bronze: parquet com os eventos brutos recebidos do Kafka.
- Silver: parquet com o schema tratado e métricas derivadas por pedido.
- Gold: snapshot JSON com agregações prontas para o dashboard.

### Engine de streaming

- O processamento principal usa Spark Structured Streaming em modo local dentro do container `bf-processor-spark`.
- A leitura é feita diretamente do Kafka no tópico `blackfriday_orders`.
- O dashboard continua consumindo o mesmo contrato de snapshot JSON, entao a troca de engine nao muda a camada de visualizacao
- O pipeline esta funcional, mas ainda fica atras do trigger de 3 segundos.
- No estado atual, os micro-batches costumam levar cerca de 8 a 10 segundos.

### Melhorias possiveis

- Aumentar o trigger do Spark para refletir melhor o tempo real de processamento.
- Reduzir a quantidade de agregacoes executadas a cada micro-batch.
- Compactar ou desacoplar parte das escritas de Bronze e Silver.

### Indicadores calculados

- `recognized_revenue`: considera pedido pago como receita positiva, devolução como impacto negativo e cancelamento como zero.
- `recognized_cogs`: segue a mesma regra para custo.
- `gross_profit`: diferença entre receita reconhecida e custo reconhecido.
- `avg_ticket`: GMV dividido por pedidos pagos.
- `target_gmv_pct`: percentual de atingimento da meta por vendedor.

## Como executar

Subir o ambiente:

```bash
make up
```

Alvos principais:

- `make network`: cria a rede externa `kafka_net` se ela ainda nao existir.
- `make build`: reconstrói as imagens da aplicacao.
- `make infra`: sobe Zookeeper, Kafka e MinIO.
- `make seed`: prepara o topico e publica a seed inicial, com rebuild das imagens bootstrap.
- `make app`: sobe generator, Spark processor e dashboard, com rebuild das imagens de runtime.
- `make up`: executa o fluxo completo com rebuild.
- `make ps`: mostra o estado dos containers.
- `make logs`: acompanha logs do bootstrap e dos servicos principais.
- `make logs-bootstrap`: mostra apenas logs de `bf-topic-init` e `bf-seed-bootstrap`.
- `make down`: derruba o ambiente.
- `make clean`: derruba o ambiente e remove artefatos locais em `trabalho/data/out`, mesmo se eles tiverem sido criados por containers.
- `make reset`: derruba o ambiente, remove volumes Docker e limpa `trabalho/data/out`.

Servers hospedados em:

- Streamlit: `http://localhost:8505`
- MinIO Console: `http://localhost:9001`
- Kafka UI: `http://localhost:8080`
