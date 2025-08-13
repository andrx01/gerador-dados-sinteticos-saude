"""
Gerador externo de dados sintéticos (Faker) - versão pt-BR com histórico.
- Gera datas entre DATE_START e DATE_END (default: 2015-01-01 a 2025-12-31).
- Distribuição temporal configurável: uniforme | recente.
- Colunas em português (separador ';' no CSV).
Execução:
  docker compose run --rm generator python -m src.generator
"""
import os
import random
import time
from datetime import datetime, timedelta, timezone
from pathlib import Path
import pandas as pd
from faker import Faker
from dotenv import load_dotenv

from src.utils import (
    N_ROWS, SEED, GEN_INTERVAL_SECONDS, SOURCE_NAME,
    LANDING_DIR, OUTPUT_FORMAT, CSV_COMPRESSION, ensure_dirs, info,
    DATE_START_DT, DATE_END_DT, DATE_DISTRIBUTION
)

# Carrega variáveis do .env
load_dotenv()

# Inicializa Faker em pt-BR
fake = Faker(locale="pt_BR")
random.seed(SEED)
Faker.seed(SEED)

# Categorias coerentes com domínio hospitalar
CATEGORIAS_PRODUTO = [
    "Medicamento",
    "Material Médico Hospitalar",
    "Serviço de Nutrição e Dietética (SND)",
    "Odontologia",
    "Serviço de Higiene e Limpeza (SHL)",
    "Material de Expediente"
]

METODOS_PAGAMENTO = ["PIX", "Cartão de Crédito", "Boleto Bancário"]
STATUS_PEDIDO = ["pedido", "enviado", "entregue", "devolvido"]

# Produtos mais realistas
PRODUTOS = [
    "Paracetamol 500mg", "Amoxicilina 500mg", "Dipirona 1g",
    "Seringa 5ml", "Agulha 25x7", "Luva Cirúrgica M", "Luva Cirúrgica G",
    "Máscara Descartável", "Álcool 70%", "Gaze Estéril", "Soro Fisiológico 0,9%",
    "Compressa de Algodão", "Esparadrapo 10cm", "Termômetro Digital",
    "Antisséptico Bucal", "Sabonete Antisséptico"
]

# Nomes de empresas brasileiras
SUFIXOS_EMPRESA = ["Comercial", "Distribuidora", "Farmacêutica", "Hospitalar", "Fornecedora"]
FORNECEDORES = [f"{random.choice(SUFIXOS_EMPRESA)} {fake.company()}" for _ in range(80)]
FABRICANTES = [f"{random.choice(SUFIXOS_EMPRESA)} {fake.company()}" for _ in range(40)]

# Colunas do CSV
CAMPOS_PT = [
    "versao_schema",
    "id_pedido",
    "datahora_pedido_utc",
    "status",
    "nome_cliente",
    "email_cliente",
    "cidade_cliente",
    "estado_cliente",
    "id_produto",
    "nome_produto",
    "categoria_produto",
    "nome_fornecedor",
    "nome_fabricante",
    "quantidade",
    "preco_unitario",
    "desconto",
    "valor_bruto",
    "valor_liquido",
    "metodo_pagamento",
    "datahora_pagamento_utc",
    "em_atraso"
]
VERSAO_SCHEMA = "v1"

# -------- Funções auxiliares de data --------
def rand_datetime_between(start_dt: datetime, end_dt: datetime, mode: str = "recente") -> datetime:
    """
    Sorteia uma data entre start_dt e end_dt.
    mode='uniforme' -> distribuição uniforme.
    mode='recente'  -> viés para datas mais próximas de end_dt (mais recentes).
    """
    assert start_dt <= end_dt
    total_sec = (end_dt - start_dt).total_seconds()
    u = random.random()
    if mode == "recente":
        # Potência < 1 puxa para o fim (recente). 0.35 ≈ viés moderado.
        # (u ** alpha) com alpha<1 concentra valores perto de 1.
        alpha = 0.35
        u = u ** alpha
    # desloca para o intervalo
    return start_dt + timedelta(seconds=total_sec * u)

def clamp_dt(dt: datetime, min_dt: datetime, max_dt: datetime) -> datetime:
    return max(min_dt, min(dt, max_dt))

# -------- Geração de linhas --------
def gerar_linha(i: int):
    qtd = random.randint(1, 8)
    preco_unit = round(random.uniform(5, 800), 2)
    desconto = random.choice([0, 0, 0.05, 0.10, 0.15])

    # Data do pedido com distribuição configurável no intervalo 2015→2025
    dt_pedido = rand_datetime_between(DATE_START_DT, DATE_END_DT, DATE_DISTRIBUTION)

    # Atraso de pagamento simulando 0, 1, 3 ou 7 dias
    atraso_dias = random.choice([0, 0, 0, 1, 3, 7])
    dt_pagamento = dt_pedido + timedelta(days=atraso_dias)

    # Garante que dt_pagamento não passe do limite superior em casos extremos
    dt_pagamento = clamp_dt(dt_pagamento, dt_pedido, DATE_END_DT + timedelta(days=14))

    valor_bruto = round(qtd * preco_unit, 2)
    valor_liquido = round(valor_bruto * (1 - desconto), 2)

    return {
        "versao_schema": VERSAO_SCHEMA,
        "id_pedido": f"P{i:07d}",
        "datahora_pedido_utc": dt_pedido.isoformat(),
        "status": random.choice(STATUS_PEDIDO),
        "nome_cliente": fake.name(),
        "email_cliente": fake.email(),
        "cidade_cliente": fake.city(),
        "estado_cliente": fake.estado_sigla(),
        "id_produto": f"PRD{random.randint(0, 9999):06d}",
        "nome_produto": random.choice(PRODUTOS),
        "categoria_produto": random.choice(CATEGORIAS_PRODUTO),
        "nome_fornecedor": random.choice(FORNECEDORES),
        "nome_fabricante": random.choice(FABRICANTES),
        "quantidade": qtd,
        "preco_unitario": preco_unit,
        "desconto": desconto,
        "valor_bruto": valor_bruto,
        "valor_liquido": valor_liquido,
        "metodo_pagamento": random.choice(METODOS_PAGAMENTO),
        "datahora_pagamento_utc": dt_pagamento.isoformat(),
        "em_atraso": atraso_dias > 0
    }

def gerar_uma_vez():
    ensure_dirs()
    ts = datetime.utcnow().strftime("%Y%m%d_%H%M%S")
    base_nome = f"{SOURCE_NAME}_transacoes_{ts}"

    df = pd.DataFrame(gerar_linha(i) for i in range(N_ROWS))
    df = df[[c for c in CAMPOS_PT if c in df.columns]]  # ordem estável

    if OUTPUT_FORMAT == "parquet":
        caminho = LANDING_DIR / f"{base_nome}.parquet"
        df.to_parquet(caminho, index=False)
    else:
        ext = "csv.gz" if CSV_COMPRESSION == "gzip" else "csv"
        caminho = LANDING_DIR / f"{base_nome}.{ext}"
        # CSV pt-BR: separador ';' e decimal ','
        df.to_csv(
            caminho,
            index=False,
            sep=";",
            decimal=",",
            compression=("gzip" if CSV_COMPRESSION == "gzip" else None),
            encoding="utf-8-sig"
        )

    info(f"Arquivo gerado: {caminho.name} | linhas={len(df):,} | período={DATE_START_DT.date()}→{DATE_END_DT.date()} | dist={DATE_DISTRIBUTION}")

def main():
    if GEN_INTERVAL_SECONDS <= 0:
        gerar_uma_vez()
        return
    intervalo = max(GEN_INTERVAL_SECONDS, 5)
    info(f"Execução contínua a cada {intervalo}s")
    while True:
        gerar_uma_vez()
        time.sleep(intervalo)

if __name__ == "__main__":
    main()