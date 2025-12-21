import pandas as pd
import numpy as np


def get_ui_analysis_tables(df: pd.DataFrame):
    """
    Возвращает 3 таблицы без колонки Вкл/Искл.
    Устойчив к разным названиям колонок описания и сверхточный расчет процентов.
    """
    if df.empty:
        return {"debit_top": [], "credit_top": [], "related_parties": []}

    df = df.copy()
    # Преобразуем суммы в float64 для максимальной точности
    df['amount'] = pd.to_numeric(df['amount'], errors='coerce').fillna(0.0).astype(float)

    # Определяем колонку с описанием транзакции
    purpose_cols = ['details', 'Назначение платежа', 'Назначение', 'operation']
    purpose_col = next((c for c in purpose_cols if c in df.columns), None)

    # --- ФИЛЬТРАЦИЯ "САМ СЕБЕ" ---
    if purpose_col:
        self_transfer_keywords = [
            'со своего счета', 'между своими', 'перевод между своими',
            'own account', 'internal transfer', 'с карты другого банка'
        ]
        pattern = '|'.join(self_transfer_keywords)

        mask_purpose = df[purpose_col].str.contains(pattern, case=False, na=False)
        mask_name = df.get('counterparty_name', pd.Series([False] * len(df), index=df.index)).str.contains(pattern,
                                                                                                           case=False,
                                                                                                           na=False)

        df = df[~(mask_purpose | mask_name)].copy()

    if df.empty:
        return {"debit_top": [], "credit_top": [], "related_parties": []}

    id_col = 'counterparty_id' if 'counterparty_id' in df.columns else (purpose_col or 'amount')
    name_col = 'counterparty_name' if 'counterparty_name' in df.columns else (purpose_col or 'amount')

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

        # --- СВЕРХТОЧНЫЙ РАСЧЕТ ПРОЦЕНТОВ ---
        def format_pct_precise(val, total):
            if total == 0 or val == 0: return "0%"
            p = (val / total) * 100

            if p < 0.1:
                return "<0.1%"  # Если сумма есть, но она ничтожно мала
            if p < 1:
                return f"{p:.1f}%"  # Например "0.2%"
            return f"{round(p)}%"  # "25%"

        top_9['% от общ'] = top_9['abs_amount'].apply(lambda x: format_pct_precise(x, total_sum))
        # ------------------------------------

        top_9['Коэф'] = 1
        label = "Ключевые поставщики" if is_debit else "Ключевые клиенты"
        result = top_9.rename(columns={name_col: label, 'abs_amount': 'Оборот'})

        return result[[label, 'Оборот', '% от общ', 'Коэф']].to_dict(orient="records")

    # Таблица аффилированных лиц
    df['turnover'] = df['amount'].abs()
    rp_grouped = df.groupby([id_col, name_col]).agg(
        Дебет=('amount', lambda x: x[x < 0].sum()),
        Кредит=('amount', lambda x: x[x > 0].sum()),
        Сальдо=('amount', 'sum'),
        Оборот=('turnover', 'sum')
    ).reset_index()

    rp_grouped['Коэф'] = 1
    rp_result = rp_grouped.rename(columns={name_col: 'Контрагент'})
    final_rp_cols = ['Контрагент', 'Дебет', 'Кредит', 'Сальдо', 'Оборот', 'Коэф']

    return {
        "debit_top": get_top_9_with_others(df, is_debit=True),
        "credit_top": get_top_9_with_others(df, is_debit=False),
        "related_parties": rp_result[final_rp_cols].to_dict(orient="records")
    }