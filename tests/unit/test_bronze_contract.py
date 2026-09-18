from pix_fraud.transformations.bronze import BRONZE_COLUMNS


def test_bronze_excludes_derived_source_columns():
    excluded = {
        "hora_dia",
        "dia_semana",
        "dia_util",
        "horario_noturno",
        "acima_limite_noturno",
        "razao_saldo_residual",
        "proporcao_valor_recebedor",
        "score_risco",
        "nivel_risco",
    }

    assert not excluded.intersection(BRONZE_COLUMNS)


def test_transaction_id_is_part_of_contract():
    assert "transaction_id" in BRONZE_COLUMNS
