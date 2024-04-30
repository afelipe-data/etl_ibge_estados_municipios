# etl_ibge_estados_municipios
Projeto básico de agendamento de ETL usando Rocketry e Pandas para extrair dados da API do IBGE

Trabalhamos com  a extração de dados de API de localidades, forncecida pelo IBGE.

![Desafio Imagem](./challenge.png)

Documentação Oficial da API:

https://servicodados.ibge.gov.br/api/docs/localidades

Nosso objetivo final era extrair dados de três API's

1. API de Estados
    - Objetivo: criarmos uma lista de estados do Brasil
    - Endpoint: `http://servicodados.ibge.gov.br/api/v1/localidades/estados`

2. API com metadado dos estados
    - Objetivo: para cada estado na lista anterior, extraimos a area do estado.
    - Criamos um dicionário com a sigla do estado e a área do estado.
    - Endpoint: `http://servicodados.ibge.gov.br/api/v3/malhas/estados/{UF}/metadados`

3. API com lista de municipios por estado
    - Objetivo: para cada estado na lista, extraimos a lista de municipios.
    - Criamos uma lista contendo o nome dos municipios.
    - Endpoint: `http://servicodados.ibge.gov.br/api/v1/localidades/estados/{UF}/municipios`


Geramos dois datasets:


| Dataset | Conteúdo | Formato de Arquivo |
| ------- | -------- | ------------------ |
| DATASET 1 | Todos os estados do Brasil, e a área de cada estado, ordenados por área. | CSV |
| DATASET 2 | Um dataset com todos os municípios do Brasil, e a UF a qual pertencem. | Parquet particionado por UF|


## Parte 1 - Extração de dados

Extração dos dados das APIs listadas acima.

Saídas:

* Lista com todas as UF's do Brasil
* DATASET 1 - Dicionário com Area em km2 por UF do Brasil
* DATASET 2 - Dicionário com todos os munícipios por UF do Brasil

# Parte 2 - Transformação de dados

Saídas:

* Dataset 1
    * Transformação do dicionário com UF e Area em um DataFrame do Pandas.
    * Converção dos tipos necessários
    * Ordenação do DataFrame pela coluna de Area em ordem decrescente.

* Dataset 2
    * Transformação do dicionário com UF e Municípios em um DataFrame do Pandas.
    

# Parte 3 - Salvando os dados

Salvamos os dados em dois formatos de arquivos diferentes:

1. DATASET 1
    * Exportação dos dados para um arquivo CSV.
    * Separador como `;`
    * Codificação como `utf-8`.

2. DATASET 2
    * Exportação dos dados para um arquivo parquet.
    * Particionamento dos municipios por UF

# Observação:

O ETL desenvolvido aqui está escrito como como no formato Jupyter notebook e Python com agendamento usando Rocketry.