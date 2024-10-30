import pandas as pd

def align_factor_data_with_knowledge_dates(factor_df, knowledge_dates_df):
    # Align the factor data with the knowledge dates
    factor_df = factor_df.merge(knowledge_dates_df, on=['ticker', 'year', 'quarter'], how='left')
    factor_df['metricDate'] = factor_df['earningsCallDate']
    return factor_df
