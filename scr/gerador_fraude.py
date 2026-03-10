import pandas as pd
import numpy as np
from faker import Faker
import random
from datetime import datetime, timedelta
from pymongo import MongoClient
import os

fake = Faker('pt_BR')

def gerar_massa_critica(n_registros = 3000):
    # Configurando a conexao
    mongo_uri = os.getenv('MONGO_URI', 'mongodb://localhost:27017/')

    print(f"🚀 Iniciando geração de {n_registros} registros...")

    tipos_negocio = ['Restaurante', 'Loja de Roupas', 'Eletrônicos', 'Serviços', 'Mercado','Posto de Combustivel']

    lojas_ids = np.random.randint(100,150, size=n_registros)
    valores = np.round(np.random.uniform(10.0, 800.0, size=n_registros),2)
    scores = np.random.randint(0,30, size=n_registros)

    dados = []

    #Gerando fraudes propositais
    for i in range(n_registros):
        is_fraud = 0
        motivo = "Normal"
        valor = valores[i]
        score = scores[i]

        #logica de fraudes propositais
        if i % 12 == 0:
            is_fraud = 1
            escolha_fraude = random.choice(['valor_alto', 'repeticao', 'distancia'])
            if escolha_fraude == 'valor_alto':
                valor = round(random.uniform(5000.0, 15000.0), 2)
                score = random.randint(70, 100)
                motivo = "Ticket muito acima da média"
            elif escolha_fraude == 'repeticao':
                valor = round(random.uniform(10.0, 100.0), 2)
                score = random.randint(60, 80)
                motivo = "Múltiplas tentativas (Card Testing)"
            elif escolha_fraude == 'distancia':
                score = random.randint(80, 95)
                motivo = "Viagem Impossível"

        dados.append({
            "transacao_id": fake.uuid4(),
            "data_hora": (datetime.now() - timedelta(seconds=i)).strftime("%Y-%m-%d %H:%M:%S"),
            "loja_id": int(lojas_ids[i]),
            "tipo_negocio": random.choice(tipos_negocio),
            "valor": valor,
            "score_risco": int(score),
            "is_fraud": is_fraud,
            "estado_loja": fake.state_abbr(),
            "motivo_fraude": motivo
        })

    df = pd.DataFrame(dados)

    df.to_parquet("dados_gigantes_limpos.parquet", index=False)

    # Inserindo Ruído (5% de valores sujos para o Spark limpar depois)
    df['valor'] = df['valor'].astype(object)
    df.loc[df.sample(frac=0.05).index, 'valor'] = "ERRO_VALOR"

    print(f"📉 Dados gerados: {len(df)} registros.")

    # Salvar no MongoDB

    try:
        client = MongoClient(mongo_uri)
        db = client['sumup_data']
        collection = db['raw_transactions']

        collection.delete_many({}) # Limpa o banco
        collection.insert_many(df.to_dict('records'))
        print(f"🔌 Sucesso! Dados inseridos no MongoDB em: {mongo_uri}")
    except Exception as e:
        print(f"⚠️ Erro no Mongo: {e}. Salvando em arquivos locais.")

    df.to_csv("dados_gigantes_sujos.csv", index=False)

    return df

if __name__ == "__main__":
    # Comece com 5000 para testar o Docker. 
    # Depois mude para 1.000.000 para o Benchmark oficial!
    gerar_massa_critica(1000000)