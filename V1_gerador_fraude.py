import pandas as pd
import numpy as np
from faker import Faker
import random
from datetime import datetime, timedelta
from pymongo import MongoClient

fake = Faker('pt_BR')
Faker.seed(42)

def gerar_dados_sujos(n_registros=2000):
    dados = []
    tipos_negocio = ['Restaurante', 'Loja de Roupas', 'Eletrônicos', 'Serviços', 'Mercado']

    for i in range(n_registros):
        data_base = datetime.now() - timedelta(days=random.randint(0,30))
        loja_id = random.randint(100,150)
        tipo_loja = random.choice(tipos_negocio)

        #padrão normal
        valor = round(random.uniform(10.0, 500.0),2)
        is_fraud = 0
        score = random.randint(0,30)
        motivo = "Normal"

        # Inserindo Anomalias Propositais (Fraudes)

        if i % 12 == 0: # A cada 12 registros, criamos uma suspeita
            is_fraud = 1
            escolha_fraude = random.choice(['valor_alto', 'repeticao', 'distancia'])

            if escolha_fraude == 'valor_alto':
                valor = round(random.uniform(5000.0, 15000.0), 2)
                score = random.randint(70, 100)
                motivo = "Ticket muito acima da média do lojista"
            elif escolha_fraude == 'repeticao':
                valor = round(random.uniform(10.0, 100.0), 2) # Valores baixos para testar cartões
                score = random.randint(60, 80)
                motivo = "Múltiplas tentativas em curto intervalo (Card Testing)"
                # Simula transações idênticas em segundos
            elif escolha_fraude == 'distancia':
                score = random.randint(80, 95)
                motivo = "Transação em estado divergente do cadastro (Viagem Impossível)"

        registro = {
            "transacao_id": fake.uuid4(),
            "data_hora": data_base.strftime("%Y-%m-%d %H:%M:%S"),
            "loja_id": loja_id,
            "tipo_negocio": tipo_loja,
            "valor": valor,
            "metodo_pagamento": random.choice(['Crédito Vista', 'Crédito Parc', 'Débito']),
            "score_risco": score,
            "is_fraud": is_fraud,
            "cidade_loja": fake.city().upper() if random.random() > 0.1 else None, # 10% de Nulos
            "estado_loja": fake.state_abbr(),
            "motivo_fraude": motivo
        }

        # Simular duplicatas propositais (2% dos dados)
        if random.random() < 0.02:
            dados.append(registro)

        dados.append(registro)
    
    df = pd.DataFrame(dados)

    # Inserindo ruído em colunas específicas via Pandas
    # 1. Transformar alguns valores em Strings com erro (Ex: "R$ 150,00" em vez de float)
    indices_sujos = df.sample(frac=0.05).index
    df.loc[indices_sujos, 'valor'] = "ERRO_VALOR"

    # 2. Inserir alguns NaNs na coluna de score
    df.loc[df.sample(frac=0.03).index, 'score_risco'] = np.nan

    print(f"📉 Dados gerados: {len(df)} registros (incluindo sujeira).")

    # --- SALVAR NO MONGODB (Simulando o Banco) ---

    try:
        client = MongoClient('mongodb://localhost:27017/')
        db = client['sumup_data']
        collection = db['raw_transactions']

        # Limpa o banco antes de inserir para não virar bagunça
        collection.delete_many({})

        # Converte DF para dicionário e insere
        collection.insert_many(df.to_dict('records'))
        print("🔌 Dados crus inseridos com sucesso no MongoDB (Docker)!")
    except Exception as e:
        print(f"⚠️ Erro ao conectar no Mongo: {e}. Salvando apenas CSV.")

    df.to_csv("dados_fraude_sujos.csv", index=False)
    return df

if __name__ == "__main__":
    gerar_dados_sujos(2000)

