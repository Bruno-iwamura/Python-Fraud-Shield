import pandas as pd
from pymongo import MongoClient
from pandasql import sqldf


def executar_etl():
    # 1. EXTRAÇÃO (Extract)
    print("Conectando ao MongoDB...")
    client = MongoClient('mongodb://localhost:27017/')
    db = client['sumup_data']
    collection = db['raw_transactions']

    df_raw = pd.DataFrame(list(collection.find()))
    if "_id" in df_raw.columns:
        df_raw = df_raw.drop(columns=["_id"])
    
    print(f"Dados brutos extraídos: {len(df_raw)} linhas.")

    # 2. LIMPEZA INICIAL (Tratamento de tipos)
    # Convertendo 'ERRO_VALOR' para NaN e depois para 0 ou média
    df_raw['valor'] = pd.to_numeric(df_raw['valor'], errors='coerce').fillna(0)

    # 3. TRANSFORMAÇÃO VIA SQL
    print("Executando limpeza via SQL...")

    query = """
    SELECT 
        DISTINCT transacao_id,
        data_hora,
        loja_id,
        tipo_negocio,
        valor,
        metodo_pagamento,
        COALESCE(score_risco, 0) as score_risco, -- Trata Nulos no Score
        is_fraud,
        UPPER(COALESCE(cidade_loja, 'NAO IDENTIFICADO')) as cidade_loja, -- Trata Nulos na Cidade
        estado_loja,
        motivo_fraude
    FROM df_raw
    WHERE valor > 0 -- Remove registros que eram 'ERRO_VALOR' ou zero
    """
    df_cleaned = sqldf(query, locals())

    # 4. CARGA (Load)
    print(f"Dados limpos: {len(df_cleaned)} linhas.")
    df_cleaned.to_csv("dados_fraude_limpos.csv",index=False,encoding='utf-8-sig')
    print("💾 Arquivo 'dados_fraude_limpos.csv' pronto para o Tableau!")

    # Salvando em Parquet (Alta performance)
    df_cleaned.to_parquet("dados_fraude_limpos.parquet", index=False)
    print("💎 Arquivo Parquet gerado com sucesso!")
    
    return df_cleaned

if __name__ == "__main__":
    df_final = executar_etl()
    # Mostra um pouco do resultado
    print("\nAmostra dos dados limpos:")
    print(df_final.head())