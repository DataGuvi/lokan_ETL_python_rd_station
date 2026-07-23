# lokan_ETL_python

Processo de ETL para integração de dados do **RD Station CRM** com um banco **PostgreSQL**. O projeto contempla carga completa (*full load*) e carga incremental, extraindo os dados pela API V2, tratando-os e gravando na camada de staging do data warehouse.

Além da carga, o pipeline incremental detecta **negociações excluídas na origem** (soft delete) e registra cada execução em uma tabela de logs, com alerta por e-mail em caso de falha.

---

## Sumário

- [Arquitetura](#arquitetura)
- [OAuth 2.0](#oauth-20)
- [Estrutura do projeto](#estrutura-do-projeto)
- [Tabelas no PostgreSQL](#tabelas-no-postgresql)
- [Fluxos de execução](#fluxos-de-execução)
- [Detecção de registros excluídos](#detecção-de-registros-excluídos)
- [Particularidades da API do RD Station](#particularidades-da-api-do-rd-station)
- [Instalação](#instalação)
- [Configuração](#configuração)
- [Como executar](#como-executar)
- [Monitoramento](#monitoramento)

---

## Arquitetura

O projeto segue a separação clássica **E-T-L**, com os acessos externos isolados em `drivers/`:

```
RD Station CRM API V2
        │
        ▼
   drivers/api_client.py   ← autenticação OAuth2, throttle, paginação
        │
        ▼
   src/extract.py          ← extração de deals e lookups
        │
        ▼
   src/transform.py        ← achatamento, tradução de colunas, limpeza
        │
        ▼
   src/load.py             ← carga, upsert via stage, soft delete
        │
        ▼
PostgreSQL (schema dw_rdstation)
```
## OAuth 2.0

```
A autenticação da API da Lokan do RD Station V2 depende de um access_code, que é gerado automaticamente pela pipeline durante a execução e salvo no .env do servidor.

Como a pipeline roda no servidor 1, para executar o projeto localmente na IDE é necessário:

Acessar o .env do servidor 1;
Copiar o access_code gerado por lá;
Copiar também o refresh_token correspondente;
Colar ambos os valores no .env local do seu ambiente de desenvolvimento.

Sem esses dois valores atualizados, a autenticação falha ao rodar localmente.
```

## Estrutura do projeto

```
├── main.py                     # Pipeline de carga completa (full load)
├── main_incremental.py         # Pipeline de carga incremental + detecção de exclusões
├── config/
│   └── settings.py             # Variáveis de ambiente e DATABASE_URL
├── drivers/
│   ├── api_client.py           # Cliente da API do RD Station (OAuth2, throttle, paginação)
│   └── database.py             # Engine SQLAlchemy (singleton, pool_pre_ping)
├── src/
│   ├── extract.py              # Extração de deals e montagem dos lookups
│   ├── transform.py            # Transformação para o modelo do DW
│   ├── load.py                 # Carga, upsert e marcação de deletados
│   └── watermark.py            # Cálculo do cutoff da carga incremental
└── utils/
    ├── date_utils.py           # Fusos, formatação e filtros RDQL
    ├── db_logger.py            # Registro de execuções na etl_execution_logs
    ├── log_mail_message.py     # Envio de e-mail de alerta (SMTP)
    └── logger.py               # Configuração do logging para stdout
```

## Tabelas no PostgreSQL

Schema padrão: `dw_rdstation` (configurável via `POSTGRES_SCHEMA`).

### `fato_rd_station_negociacoes`

Tabela fato com uma linha por negociação. PK em `id_negociacao`.

| Coluna | Tipo | Descrição |
|---|---|---|
| `id_negociacao` | varchar | ID da negociação no RD Station (PK) |
| `nome_negociacao` | text | Nome da negociação |
| `data_criacao` | timestamp | `created_at` |
| `data_atualizacao` | timestamp | `updated_at` — usada como watermark do incremental |
| `data_fechamento` | timestamp | `closed_at` |
| `id_cliente` / `descricao_cliente` | varchar / text | Organização; se ausente, usa o primeiro contato |
| `id_origem` / `descricao_origem` | varchar / text | Origem (*source*) |
| `id_vendedor` / `descricao_vendedor` | varchar / text | Responsável (*user*) |
| `status_negociacao` | varchar | `ongoing`, `won`, `lost` |
| `id_motivo_perda` / `descricao_motivo_perda` | varchar / text | Motivo de perda |
| `descricao_municipio` | text | Campo customizado `cidade-teste` |
| `descricao_estado` | varchar | Campo customizado `estado` |
| `descricao_filial` | text | Campo customizado `filial` |
| `descricao_unidade_negocio` | text | Campo customizado `setor` |
| `id_produto` / `descricao_produto` | text | Produtos vinculados, concatenados por vírgula |
| `valor_total` | numeric | `total_price` |
| `descricao_prazo` | text | Campo customizado `prazo-de-locacao` |
| `time_import` | timestamp | Momento da carga (NOT NULL) |
| `registro_deletado` | boolean | `true` quando a negociação foi excluída no RD Station |

### `stg_rd_station_negociacoes`

Tabela de trabalho, com as mesmas colunas da fato (exceto `registro_deletado`). É **truncada no início de cada uso** e serve a dois propósitos: receber o lote do upsert incremental e receber a lista de ids ativos na comparação de exclusões.

### `etl_execution_logs`

Log de execuções, compartilhado entre projetos de ETL.

| Coluna | Descrição |
|---|---|
| `id` | serial |
| `project_name` | identificador do projeto (`PROJECT_NAME`) |
| `operation` | operação executada (ex.: `etl_incremental_negociacoes`) |
| `status` | `SUCCESS` ou `FAILED` |
| `error_reason` | mensagem do erro, quando houver |
| `start_time` / `end_time` | início e fim da execução |

## Fluxos de execução

### Full load — `main.py`

Recarrega a base inteira. **Faz `TRUNCATE` na fato antes de inserir.**

1. Extrai todos os deals (varredura convergente, veja abaixo)
2. Monta os lookups: vendedores, origens, motivos de perda, organizações, contatos e produtos
3. Transforma para o modelo do DW
4. `TRUNCATE` + `INSERT` em `fato_rd_station_negociacoes`

> ⚠️ O `TRUNCATE` apaga também a marcação de `registro_deletado` e qualquer linha inserida manualmente.

### Incremental — `main_incremental.py`

Processa apenas o que mudou desde a última carga.

1. **Cutoff**: lê `MAX(data_atualizacao)` da fato e subtrai 2h de margem (`src/watermark.py`). Se a tabela estiver vazia, aborta pedindo o full load
2. **Extração**: busca os deals com `updated_at >= cutoff`
3. **Exclusões**: detecta e marca negociações removidas na origem (veja a seção seguinte) — roda **sempre**, mesmo quando nada foi atualizado, porque exclusão não gera evento de atualização
4. **Lookups**: busca apenas os relacionamentos dos deals do lote
5. **Transformação**
6. **Upsert via stage**: carrega o lote na stage, faz `INSERT` dos ids novos e `UPDATE` dos existentes — nunca `TRUNCATE` na fato

A coluna `registro_deletado` está em `COLUNAS_PROTEGIDAS_NO_UPDATE` (`src/load.py`) e nunca é sobrescrita pelo upsert.

## Detecção de registros excluídos

O RD Station não notifica exclusões: a negociação simplesmente some da listagem. Sem tratamento, a fato acumularia para sempre registros que não existem mais, inflando contagens e valores.

O processo é um **soft delete em três etapas**, e nenhuma linha é apagada fisicamente:

1. **Levantar candidatos** — os ids da listagem completa vão para a stage; todo id que está na fato e não está na stage vira candidato. Na mesma etapa, ids que reapareceram têm `registro_deletado` devolvido para `false`
2. **Confirmar um a um** — cada candidato é consultado com `GET /deals/{id}`. **Só HTTP 404 conta como exclusão**; qualquer outro erro é tratado como "não sei" e o registro não é marcado
3. **Marcar** — `registro_deletado = true` nos confirmados

A etapa 2 é indispensável: a listagem da API não é confiável (veja abaixo). Em um teste real, os 155 candidatos levantados pela comparação eram **todos falsos positivos** — negociações vivas que a paginação havia pulado.

Como rede de segurança, `LIMITE_MARCACAO_DELETADOS` (20%) aborta a operação se uma única execução tentar marcar mais de 20% da fato.

## Particularidades da API do RD Station

Comportamentos verificados empiricamente que explicam decisões do código:

**Paginação não-determinística.** A listagem `GET /deals` reporta sempre o mesmo total, mas cada varredura devolve um conjunto diferente de ids — pula ~5-15% dos registros aleatoriamente e duplica outros entre páginas. `sort` e `order` são ignorados, não há cursor (`links.next` é apenas `page+1`) e `page[size]` acima de 200 devolve HTTP 400.

Como o pulo é **aleatório**, varrer repetidamente e unir os resultados converge para o conjunto completo. É o que faz `get_all_deals_convergente()`: repete a varredura até duas passadas seguidas não trazerem nada novo (na prática, 3-4 varreduras). Use sempre essa função quando faltar registro for um problema — `get_all_deals()` sozinha perde dados.

**Filtro RDQL exige ISO 8601 com `T`.** `updated_at:>=2026-07-22 13:11:07` devolve **HTTP 500** em qualquer codificação de URL; `updated_at:>=2026-07-22T13:11:07Z` funciona. Datetime sem fuso é interpretado como UTC.

**Rate limit de 120 requisições/minuto.** O cliente aplica throttle de 0,55s entre requisições e respeita o `Retry-After` em respostas 429.

**Campos vazios chegam como `""`.** O transform normaliza string vazia (e só-espaços) para `None`, para o banco receber `NULL` em vez de string vazia.

**Endpoints sem listagem paginada.** Organizações, contatos, produtos e motivos de perda são buscados um a um pelo ID, o que domina o tempo de execução das cargas grandes.

## Instalação

Requer **Python 3.10+** (o ambiente atual roda 3.14) e acesso ao PostgreSQL de destino.

```bash
python -m venv .venv
.venv\Scripts\activate        # Windows
source .venv/bin/activate     # Linux/macOS

pip install -r requirements.txt
```

## Configuração

Copie `.env.example` para `.env` e preencha:

```bash
cp .env.example .env
```

| Variável | Descrição |
|---|---|
| `RD_CLIENT_ID`, `RD_CLIENT_SECRET`, `RD_REFRESH_TOKEN` | Credenciais OAuth2 do RD Station |
| `RD_API_BASE_URL`, `RD_OAUTH_URL` | Endpoints da API (já preenchidos com o padrão) |
| `POSTGRES_HOST`, `POSTGRES_PORT`, `POSTGRES_DB` | Conexão com o banco |
| `POSTGRES_USER`, `POSTGRES_PASSWORD` | Credenciais do banco |
| `POSTGRES_SCHEMA` | Schema de destino (padrão `dw_rdstation`) |
| `PROJECT_NAME` | Nome do projeto na `etl_execution_logs` |
| `EMAIL_SENDER`, `SENDER_PASSWORD`, `EMAIL_RECEIVER` | SMTP para o alerta de falha |

O `access_token` e o `refresh_token` são renovados automaticamente e **reescritos no `.env`** a cada autenticação. A conexão com o PostgreSQL usa `sslmode=require` (exigido pelo Azure).

## Como executar

```bash
# Carga completa — recria a fato do zero
python main.py

# Carga incremental — atualiza o que mudou e marca as exclusões
python main_incremental.py
```

O incremental depende de a fato já ter dados: sem `MAX(data_atualizacao)` não há watermark, e a execução falha pedindo o full load.

Em produção, o esperado é rodar o **full load uma vez** e agendar o **incremental** na frequência desejada.

## Monitoramento

Cada execução do incremental grava uma linha em `etl_execution_logs`, com `SUCCESS` ou `FAILED` e o motivo do erro. Em caso de falha, um e-mail de alerta é enviado com a operação, o horário de início e o traceback completo.

Falhas ao gravar o log **não** interrompem o pipeline nem impedem o alerta: se o banco estiver indisponível, o `DBLogger` registra o problema no log da aplicação e segue.

```sql
-- Últimas execuções
SELECT project_name, operation, status, error_reason, start_time, end_time
FROM dw_rdstation.etl_execution_logs
ORDER BY id DESC
LIMIT 10;

-- Panorama da fato
SELECT registro_deletado, COUNT(*)
FROM dw_rdstation.fato_rd_station_negociacoes
GROUP BY registro_deletado;
```
