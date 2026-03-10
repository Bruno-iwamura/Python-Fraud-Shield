import pandas as pd
import time

print("⏳ Iniciando Benchmark PANDAS (1M registros)...")
inicio = time.time()

df = pd.read_csv("dados_gigantes_sujos.csv")

df['valor'] = pd.to_numeric(df['valor'], errors='coerce').fillna(0)

resultado = df[df['valor'] > 1000].groupby('estado_loja').agg({
    'is_fraud': 'sum',
    'valor': 'sum'
}).rename(columns={'is_fraud': 'total_fraudes', 'valor': 'volume_financeiro'})

fim = time.time()
tempo_total = fim - inicio

print(f"\n✅ PANDAS FINALIZADO!")
print(f"⏱️ Tempo total: {tempo_total:.2f} segundos")
print(resultado.head(5))