import dask.dataframe as dd
from constants import *

def read_raw_file(raw_path, cols_names):
    print(f'Reading raw files from: {raw_path}...')
    df = dd.read_csv(raw_path,  
                    storage_options = {TOKEN_STR : TOKEN_JSON},
                    delimiter = DELIMITER_TYPE, 
                    encoding = ENCODING_TYPE, 
                    low_memory = False, 
                    dtype = str, 
                    compression = COMPRESSION_TYPE, 
                    blocksize = None, 
                    names = cols_names)
    print(f'Raw files were read!')
    return df

def upload_dd_to_parquet(df, path):
    print(f'Writting files in parquet formats to {path}...')
    df.to_parquet(path, storage_options = {TOKEN_STR : TOKEN_JSON}, )
    print(f'Parquet files were written!')

def process_data_empresa():

    empresa_df = read_raw_file(FILE_PATH_RAW_EMPRESA, COLS_LST_EMPRESA)
    empresa_df[COL_CAPITAL_SOCIAL] = empresa_df[COL_CAPITAL_SOCIAL].map(lambda x : x.replace(DECIMAL_TYPE_CAPITAL_SOCIAL_OLD, 
                                                                                            DECIMAL_TYPE_CAPITAL_SOCIAL_NEW))
    empresa_df[COL_CAPITAL_SOCIAL] = dd.to_numeric(empresa_df[COL_CAPITAL_SOCIAL], 
                                                errors = ERRORS_COERCE)
    empresa_df[COL_PORTE] = empresa_df[COL_PORTE].fillna(value = FILL_NAN_VALUE_PORTE)
    empresa_df = empresa_df.repartition(npartitions = 150)
    upload_dd_to_parquet(empresa_df, FILE_PATH_PARQUET_EMPRESA)


def process_data_estabelecimento():
    estabelecimento_df = read_raw_file(FILE_PATH_RAW_ESTABELECIMENTO, 
                                    COLS_LST_ESTABELECIMENTO)
    estabelecimento_df[COL_CNPJ_COMPLETO] = estabelecimento_df[COL_CNPJ_BASICO] + estabelecimento_df[COL_CNPJ_ORDEM] + estabelecimento_df[COL_CNPJ_DV]
    estabelecimento_df[COL_PAIS] = estabelecimento_df[COL_PAIS].fillna(value = FILL_NAN_VALUE_PAIS)
    estabelecimento_df = estabelecimento_df.repartition(npartitions = 200)
    upload_dd_to_parquet(estabelecimento_df, FILE_PATH_PARQUET_ESTABELECIMENTO)

def process_data_simples():
    simples_df = read_raw_file(FILE_PATH_RAW_SIMPLES, COLS_SIMPLES)
    simples_df = simples_df.repartition(npartitions = 10)
    upload_dd_to_parquet(simples_df, FILE_PATH_PARQUET_SIMPLES)

def process_data_default(file_path_raw, cols, file_path_parquet):
    df = read_raw_file(file_path_raw, cols)
    upload_dd_to_parquet(df, file_path_parquet)


def main():
    process_data_empresa()
    process_data_estabelecimento()
    process_data_simples()
    process_data_default(FILE_PATH_RAW_CNAE, COLS_DEFAULT, FILE_PATH_PARQUET_CNAE)
    process_data_default(FILE_PATH_RAW_MUNICIPIO, COLS_DEFAULT, FILE_PATH_PARQUET_MUNICIPIO)
    process_data_default(FILE_PATH_RAW_NATUREZA_JURIDICA, COLS_DEFAULT, FILE_PATH_PARQUET_NATUREZA_JURIDICA)
    process_data_default(FILE_PATH_RAW_PAIS, COLS_DEFAULT, FILE_PATH_PARQUET_PAIS)
    
    
main()