🛡️ Fraud Shield: Pipeline de Detecção de Fraude de Alta Performance

Este projeto simula um ecossistema completo de engenharia de dados para uma fintech, focado em processamento de grandes volumes, limpeza de dados "sujos" e visualização analítica.

🚀 Arquitetura do Projeto

O projeto foi desenhado para ser totalmente portável e escalável, utilizando Docker para isolar os componentes críticos.

Ingestão: MongoDB (NoSQL) rodando em container para armazenamento de dados brutos (Raw).

Processamento: Apache Spark (PySpark) para transformações pesadas e limpeza de dados.

Armazenamento de Performance: Formato Parquet para otimização de leitura colunar.

Análise & BI: Dashboard interativo em Streamlit e relatórios em Tableau.

Infraestrutura: Docker Compose gerenciando o cluster local.

🧪 O Desafio do Big Data: Pandas vs. SparkUm dos diferenciais deste projeto foi a realização de um Benchmark de Performance para entender o ponto de inflexão entre ferramentas de análise de dados simples e motores de processamento distribuído.

Metodologia do Teste

Volume: 1.000.000 de registros transacionais (~250MB).
Tarefa: Leitura de arquivo, conversão de tipos (limpeza de ruído), filtragem de valores altos e agregação por estado.

📊 Resultados do Benchmark

Ferramenta,Tempo de Execução e Observação Técnica

Pandas: 3.72s - Extremamente rápido para volumes que cabem na RAM, sem overhead de inicialização.

Apache Spark (Docker): 14.36s - Maior tempo devido ao startup da JVM e alocação do cluster.

💡 Conclusão de Engenharia

Embora o Pandas tenha sido mais rápido para 1 milhão de linhas, o Spark foi escolhido como a solução de escalabilidade.

Overhead Fixo: O Spark leva ~10s para iniciar, mas esse tempo permanece quase constante mesmo que o volume suba para 10 ou 50 milhões de linhas.

Gestão de Memória: O Pandas falharia (Out of Memory) em volumes maiores, enquanto o Spark utilizaria processamento distribuído.