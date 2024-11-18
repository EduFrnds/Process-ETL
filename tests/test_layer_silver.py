import pandas as pd

from etl.layer_silver.transformation import DataTransformationSilver


def test_load_and_process_data(load_and_process_data):
    """
    Testa a fixture de carregamento e processamento de dados.
    """
    # GIVEN: Dados carregados e processados
    df = load_and_process_data
    # print(df)

    # THEN: Verificações no DataFrame tratado
    assert not df.empty, "O DataFrame está vazio após o processamento."


def test_aggregate_data(load_and_process_data):
    # GIVEN
    data_transformation = DataTransformationSilver

    # WHEN
    df_grouped = data_transformation.aggregate_data(load_and_process_data)

    # THEN
    # 1. Verifica se a coluna 'hours_production' está no formato datetime
    assert pd.api.types.is_datetime64_any_dtype(df_grouped['hours_production']), \
        "A coluna 'hours_production' não foi convertida corretamente para datetime."

    # 2. Verifica se a coluna 'hours_production' é do tipo datetime mais comum.
    assert df_grouped['hours_production'].dtype == 'datetime64[ns]', \
        "A coluna 'hours_production' não está no tipo datetime64[ns]."

    # 2. Verifica se as colunas 'month' e 'year' foram criadas
    assert 'month' in df_grouped.columns, "A coluna 'month' não foi criada."
    assert 'year' in df_grouped.columns, "A coluna 'year' não foi criada."

    # 3. Valida que 'month' e 'year' têm valores inteiros
    assert pd.api.types.is_integer_dtype(df_grouped['month']), "A coluna 'month' não contém valores inteiros."
    assert pd.api.types.is_integer_dtype(df_grouped['year']), "A coluna 'year' não contém valores inteiros."

    # 4. Valida o dataframe
    # print(df_grouped)


