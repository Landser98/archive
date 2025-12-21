import pandas as pd
import numpy as np


def get_ui_analysis_tables(df: pd.DataFrame):
    """
    Возвращает 3 таблицы без колонки Вкл/Искл.
    """
    if df.empty:
        return {"debit_top": [], "credit_top": [], "related_parties": []}

    df = df.copy()
    df['amount'] = pd.to_numeric(df['amount'], errors='coerce').fillna(0)

    # Фильтрация "Сам себе"
    self_transfer_keywords = [
        'со своего счета', 'между своими', 'перевод между своими',
        'own account', 'internal transfer', 'с карты другого банка'
    ]
    pattern = '|'.join(self_transfer_keywords)
    is_self_transfer = (
            df['details'].str.contains(pattern, case=False, na=False) |
            df.get('counterparty_name', pd.Series(dtype=str)).str.contains(pattern, case=False, na=False)
    )
    df = df[~is_self_transfer].copy()

    if df.empty:
        return {"debit_top": [], "credit_top": [], "related_parties": []}

    id_col = 'counterparty_id' if 'counterparty_id' in df.columns else 'details'
    name_col = 'counterparty_name' if 'counterparty_name' in df.columns else 'details'

    def get_top_9_with_others(data, is_debit=True):
        mask = data['amount'] < 0 if is_debit else data['amount'] > 0
        subset = data[mask].copy()
        subset['abs_amount'] = subset['amount'].abs()

        grouped = subset.groupby(id_col).agg({
            'abs_amount': 'sum',
            name_col: 'first'
        }).reset_index().sort_values('abs_amount', ascending=False)

        total_sum = grouped['abs_amount'].sum()
        top_9 = grouped.head(9).copy()

        if len(grouped) > 9:
            others_sum = grouped.iloc[9:]['abs_amount'].sum()
            others_row = pd.DataFrame([{
                id_col: 'OTHERS',
                name_col: 'Прочие',
                'abs_amount': others_sum
            }])
            top_9 = pd.concat([top_9, others_row], ignore_index=True)

        top_9['% от общ'] = (top_9['abs_amount'] / total_sum * 100).round(0).astype(int).astype(
            str) + '%' if total_sum != 0 else "0%"
        top_9['Коэф'] = 1

        label = "Ключевые поставщики" if is_debit else "Ключевые клиенты"
        result = top_9.rename(columns={name_col: label, 'abs_amount': 'Оборот'})

        final_cols = [label, 'Оборот', '% от общ', 'Коэф']
        return result[final_cols].to_dict(orient="records")

    df['turnover'] = df['amount'].abs()
    rp_grouped = df.groupby([id_col, name_col]).agg(
        Дебет=('amount', lambda x: x[x < 0].sum()),
        Кредит=('amount', lambda x: x[x > 0].sum()),
        Сальдо=('amount', 'sum'),
        Оборот=('turnover', 'sum')
    ).reset_index()

    rp_grouped['Коэф'] = 1

    rp_result = rp_grouped.rename(columns={name_col: 'Контрагент'})
    rp_final_cols = ['Контрагент', 'Дебет', 'Кредит', 'Сальдо', 'Оборот', 'Коэф']

    return {
        "debit_top": get_top_9_with_others(df, is_debit=True),
        "credit_top": get_top_9_with_others(df, is_debit=False),
        "related_parties": rp_result[rp_final_cols].to_dict(orient="records")
    }