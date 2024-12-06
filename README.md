# ED-DATA

![image](![image](https://github.com/user-attachments/assets/53bdf703-e32f-4242-a772-4ff5c4a3b7cc))
Arquitetura: v1

## Contexto PROJETO

Nesse projeto estou supondo que o cliente esteja enfrentando dificuldades para identificar pontos de perdas e oportunidades de melhoria em seu processo de produção. 
Com base nos dados fornecidos pela equipe de produção, é possívels explorar vários aspectos que afetam diretamente a produtividade e o desempenho do equipamento.

Possíveis causas de perda de produtividade:
- Manutenção não programada: Se o equipamento precisar de manutenção inesperada (como "repair"), isso pode resultar em tempo de inatividade significativo, afetando a produção.
- Desempenho irregular de operação: Variações na velocidade, vibração, temperatura e pressão podem indicar anomalias que reduzem a eficiência.
- Falhas repetidas: Equipamentos que precisam de manutenção frequente podem estar contribuindo para perdas de produtividade.

## Agregação de dados:
Sugestões de dados adicionais:
- Custo de manutenção: Aadicionar um campo que inclua os custos associados à manutenção e reparo, para entender o impacto financeiro das paradas.
- Tempos de parada planejada vs não planejada: Diferenciar paradas planejadas de manutenção preventiva versus falhas inesperadas pode ajudar a otimizar o cronograma de manutenção.
- Metas de produção: Incluir metas de produção esperadas e comparar com a produção real para identificar desvios.

## Dashboard de Solução:
Monitoramento em tempo real de indicadores-chave:

### Produção vs Meta:
- Tempo de inatividade: Gráficos que mostram o tempo de inatividade total (por manutenção planejada ou não) e o impacto na produção.
Monitoramento de variáveis críticas: Exibir temperaturas, pressões e níveis de vibração fora dos limites normais que possam impactar o desempenho.

### Análise de manutenção:
- Horas de manutenção por equipamento: Comparar o tempo de manutenção entre equipamentos para identificar aqueles com mais problemas.
- Tipos de manutenção mais frequentes: Comparar a frequência de manutenções preventivas e corretivas.

### Identificação de anomalias:
Cartas de Controle (LSC x LSI): Configurar alertas para quando variáveis como temperatura, pressão ou vibração saírem de uma faixa aceitável.

Resumindo, é uma abordagem de manter uma arquitetura simples no início, com um foco claro na solução dos problemas do cliente, e manter o alinhamento com as boas práticas de projetos de engenharia de dados.
Isso cria um caminho natural de evolução, sem forçar grandes mudanças ou custos logo de início.

## Índice
```
process-etl/
│
├── .venv/                   # Ambiente virtual do Python
│   ├── etc/
│   ├── include/
│   ├── Lib/
│   ├── Scripts/
│   └── share/
│
├── data/                    # Pasta para armazenar dados processados│          
│
├── etl/                     # Módulo ETL
│   ├── layer_bronze/         # Camada Bronze
│   │   └── load.py
│   ├── layer_gold/
        └── read.py           # Camada Gold
        └── transformation.py
        └── upload.py
│   ├── layer_silver/
        └── read.py           # Camada Silver
        └── transformation.py
        └── upload.py
│   │   ├── __init__.py
│   │   ├── data_manager.py
│   │   └── generate_data.py
│
├── logs/                    # Diretório de logs
│   └── process-etl.log       # Arquivo de log
│
├── .env                     # Arquivo de variáveis de ambiente
├── .gitignore               # Arquivo Git para ignorar arquivos e pastas
├── db_config.py             # Arquivo de configuração de banco de dados
├── log_config.py            # Arquivo de configuração de logging
├── main.py                  # Arquivo principal para execução do projeto
├── README.md                # Documentação do projeto
└── requirements.txt         # Dependências do projeto
```


- [Visão Geral do Projeto](#project-overview): Projeto conta com uma abordagem direta do livro "Fundamentos de Engenharia de dados" e tem como objetivo a construção de um projeto que contempla o ciclo de vida da engenharia de dados.
    Conforme imagem acima, crio "steps" para explicar cada processo ao longo da execução do projeto. Caso queira acompanhar as próxima atividades segue o quadro no trello: [https://trello.com/invite/b/66fa9775f0a5daeab37ceb97/ATTI67ad7fa8d10b196ae269aeb34fcfcd491EFF7D41/engenharia-de-dados](https://trello.com/b/xe0uwXOb)
  
- [Características](#features): Consiste em etapas que convertem dados brutos (CSV com dados fake) em um produto final útil, pronto para ser consumido por análistas, cientistas de dados. No presente projeto pretendo criar um dashboard utilizando o "streamlit". Nesse primeiro momento ainda estou "fugindo" da nuvem por estratégia inicial, pensando em quem ainda está começando a carreira como engenheiro de dados.
  
- [Pré-requisitos](#Prerequisites):
  #### Python 3.x: [Baixar Python](https://www.python.org/downloads/)
  #### PostgreSQL: [Baixar PostgreSQL](https://www.postgresql.org/download/)
  #### pip: Gerenciador de pacotes Python (geralmente já incluído com o Python)

  #### Bibliotecas e pacotes necessários:
  - pandas
  - psycopg2
  - SQLAlchemy
  - Faker
  - csv
  - logging
  
- [Instalação](#installation):  
- [Usage](#usage) - Em construção
- [Development](#development) - Em construção
- [Testing](#testing) - Em construção
- [Deployment](#deployment) - Em construção
- [Contributing](#contributing) - Em construção
- [License](#license) - Em construção
