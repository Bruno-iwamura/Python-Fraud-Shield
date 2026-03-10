import streamlit as st
import pandas as pd
import plotly.express as px

# Configuração da Página
st.set_page_config(page_title="SumUp | Fraud Shield Analytics", layout="wide")

# Customização de Estilo (Opcional, mas dá um ar profissional)
st.markdown("""
    <style>
    .main { background-color: #f5f7f9; }
    .stMetric { background-color: #ffffff; padding: 15px; border-radius: 10px; box-shadow: 0 2px 4px rgba(0,0,0,0.05); }
    </style>
    """, unsafe_allow_html=True)

# 1. CARREGAMENTO DE DADOS
@st.cache_data
def load_data():
    df_limpo = pd.read_csv("data/dados_fraude_limpos.csv")
    df_sujo = pd.read_csv("data/dados_fraude_sujos.csv") # Para comparar a limpeza
    df_parquet = pd.read_parquet("data/dados_fraude_limpos.parquet")
    return df_limpo, df_sujo

df, df_sujo = load_data()

# 2. HEADER E MÉTRICAS DE ENGENHARIA (DATA OPS)
st.title("🛡️ Python Fraud Shield Analytics")
st.subheader("Monitoramento de Risco e Integridade de Dados")

col_m1, col_m2, col_m3, col_m4 = st.columns(4)

with col_m1:
    st.metric("Total Transações (Limpo)", len(df))
with col_m2:
    duplicatas = len(df_sujo) - len(df)
    st.metric("Duplicatas Removidas", duplicatas, delta_color="inverse")
with col_m3:
    erros_valor = df_sujo[df_sujo['valor'] == "ERRO_VALOR"].shape[0]
    st.metric("Erros de Tipo Corrigidos", erros_valor)
with col_m4:
    taxa_fraude = (df['is_fraud'].sum() / len(df) * 100)
    st.metric("Taxa de Fraude", f"{taxa_fraude:.2f}%")

st.divider()

# 3. FILTROS LATERAIS
st.sidebar.header("🔍 Central de Investigação")
tipo_negocio = st.sidebar.multiselect("Filtrar por Negócio", df['tipo_negocio'].unique(), default=df['tipo_negocio'].unique())
score_corte = st.sidebar.slider("Score de Risco Mínimo", 0, 100, 75)

# Aplicando Filtros
df_filtrado = df[(df['tipo_negocio'].isin(tipo_negocio)) & (df['score_risco'] >= score_corte)]

# 4. VISUALIZAÇÕES ESTRATÉGICAS
col_v1, col_v2 = st.columns(2)

with col_v1:
    st.subheader("🚨 Maiores Vilões (Motivos de Alerta)")
    fig_motivos = px.bar(
        df[df['is_fraud'] == 1]['motivo_fraude'].value_counts().reset_index(),
        x='count', y='motivo_fraude', orientation='h',
        color='count', color_continuous_scale='Reds',
        labels={'count': 'Qtd. Alertas', 'motivo_fraude': 'Motivo'}
    )
    st.plotly_chart(fig_motivos, use_container_width=True)

with col_v2:
    st.subheader("💰 Distribuição de Valores Suspeitos")
    fig_hist = px.histogram(df_filtrado, x="valor", nbins=20, color="is_fraud", 
                            marginal="box", color_discrete_sequence=['#636EFA', '#EF553B'])
    st.plotly_chart(fig_hist, use_container_width=True)

# 5. TABELA DE AÇÃO PARA O ANALISTA
st.divider()
st.subheader("🕵️ Tabela de Investigação Prioritária")
st.write(f"Exibindo {len(df_filtrado)} transações de alto risco.")

# Formatação para a tabela ficar bonita
st.dataframe(
    df_filtrado[['transacao_id', 'data_hora', 'loja_id', 'valor', 'score_risco', 'motivo_fraude', 'cidade_loja', 'estado_loja']]
    .sort_values(by='score_risco', ascending=False),
    use_container_width=True
)

if st.button("📥 Exportar Relatório de Fraude (CSV)"):
    csv = df_filtrado.to_csv(index=False).encode('utf-8-sig')
    st.download_button("Confirmar Download", csv, "relatorio_investigacao.csv", "text/csv")