# NYC Yellow Taxi — Pipeline no Databricks com Pyspark

Pipeline incremental com arquitetura medalhão sobre os dados públicos de corridas do Yellow Taxi de Nova York (TLC), construída no Databricks Free Edition.

## Arquitetura

```
Volume raw (parquet)  →  Bronze  →  Silver  →  Gold  →  Dashboard
   landing/yellow/      Auto Loader  streaming    batch
```

| Camada | Tabela | Papel |
|---|---|---|
| Raw | `/Volumes/taxi_nyc/raw/landing/yellow/` | Arquivos mensais como publicados pela TLC |
| Bronze | `taxi_nyc.bronze.yellow_trips` | Cópia fiel da origem, com linhagem |
| Silver | `taxi_nyc.silver.yellow_trips` | Dados limpos, renomeados e validados |
| Gold | em construção | Modelo dimensional para o dashboard |

Um arquivo novo no volume dispara o job (file arrival trigger), que processa somente o que ainda não foi carregado.

## Bronze

- **Auto Loader** com `trigger(availableNow=True)`: processa os arquivos novos e encerra, sem compute ligado entre cargas.
- **Idempotência por arquivo:** o checkpoint registra o que já foi processado. Reexecutar sem arquivo novo não altera a tabela.
- **Evolução de schema:** a coluna `cbd_congestion_fee` passou a existir em 2025. O modo `addNewColumns` a incorpora; nas linhas de 2024 ela fica nula, o que significa "não aplicável", não "zero".
- **Linhagem:** cada linha registra o arquivo de origem e o momento da ingestão.
- **Sem transformação de conteúdo:** filtros e correções ficam na silver, para que a bronze sempre responda se o dado chegou assim ou foi alterado depois.

*Por que Auto Loader e não COPY INTO?* Os dois resolveriam o caso, e COPY INTO seria mais simples. Escolhi Auto Loader pela coluna de dados resgatados e pelo controle explícito da evolução de schema.

## Silver

- Leitura incremental da bronze em streaming.
- Nomes de colunas padronizados em snake_case, sem os prefixos do sistema de origem.
- **Validações:**
  - a pipeline **falha** se houver dados resgatados, ou seja, conteúdo fora do schema conhecido;
  - corridas com desembarque anterior ou igual ao embarque são **descartadas**;
  - cada arquivo alimenta apenas o seu mês de competência, e corridas fora dele são **descartadas**.
- Flag `is_flex_fare` para corridas com `payment_type = 0`.

## Descobertas sobre o dado (janeiro de 2024)

- **Nulos agrupados:** ~140 mil corridas têm `passenger_count`, `RatecodeID` e outros campos nulos ao mesmo tempo. Todas são do tipo de pagamento Flex Fare, em que esses campos não se aplicam. As linhas foram mantidas, porque tarifa, distância e zonas são válidas.
- **Valores negativos:** ~35 mil corridas, concentradas em disputa e sem cobrança. São estornos. O tratamento depende da métrica: entram na receita líquida e ficam fora da contagem de corridas e das médias.
- **Datas fora do período:** o arquivo de janeiro contém corridas de 2002 e de fevereiro.
- **Sem identificador de corrida:** a origem não tem chave única. A deduplicação acontece por arquivo, não por linha.


## Fonte

[NYC TLC Trip Record Data](https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page)