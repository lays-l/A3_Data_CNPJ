HTTP_REQUEST = 'https://www.gov.br/receitafederal/pt-br/assuntos/orientacao-tributaria/cadastros/consultas/dados-publicos-cnpj'

COLS_LST_EMPRESA = ["CNPJ", 
                    "Razao_Social", 
                    "Natureza_Juridica", 
                    "Qualificacao_do_Responsavel", 
                    "Capital_Social", 
                    "Porte", 
                    "Ente_Federativo_Responsavel"]

COLS_LST_ESTABELECIMENTO = ["CNPJ_Basico", 
                            "CNPJ_Ordem", 
                            "CNPJ_DV", 
                            "Identificador", 
                            "Nome_Fantasia", 
                            "Situacao_Cadastral", 
                            "Data_Situacao_Cadastral", 
                            "Motivo_Situacao_Cadastral", 
                            "Nome_Cidade_Exterior", 
                            "Pais", 
                            "Data_Inicio_Atividad", 
                            "CNAE_Fiscal_Principal", 
                            "CNAE_Fiscal_Secundaria",
                            "Tipo_Logradouro", 
                            "Logradouro", 
                            "Numero", 
                            "Complemento", 
                            "Bairro", 
                            "CEP", 
                            "UF", 
                            "Municipio", 
                            "DDD_1", 
                            "Telefone_1", 
                            "DDD_2", 
                            "Telefone_2", 
                            "DDD_Fax", 
                            "Fax", 
                            "Correio_Eletronico", 
                            "Situacao_Especial", 
                            "Data_Situacao_Especial"]

COLS_SIMPLES = ['CNPJ_Basico', 
                'Opcao_Simples', 
                'Data_Opcao_Simples', 
                'Data_Exclusao_Simples', 
                'Opcao_MEI', 
                'Data_Opcao_MEI', 
                'Data_Exclusao_MEI']

COLS_DEFAULT = ['Codigo', 
                'Descricao']


DOWNLOAD_LST = ['EMPRECSV',
                    'ESTABELE',
                    'SIMPLES.CSV', 
                    'CNAECSV', 
                    'MOTICSV', 
                    'MUNICCSV', 
                    'NATJUCSV', 
                    'PAISCSV',
                    'QUALSCSV']

COL_CAPITAL_SOCIAL = 'Capital_Social'
COL_PORTE = 'Porte'
COL_PAIS = 'Pais'
COL_CNPJ_BASICO = 'CNPJ_Basico'
COL_CNPJ_ORDEM = 'CNPJ_Ordem'
COL_CNPJ_DV = 'CNPJ_DV'
COL_CNPJ_COMPLETO = 'CNPJ'

DECIMAL_TYPE_CAPITAL_SOCIAL_OLD = ','
DECIMAL_TYPE_CAPITAL_SOCIAL_NEW = '.'
ERRORS_COERCE = 'coerce'
FILL_NAN_VALUE_PORTE = '01'
FILL_NAN_VALUE_PAIS = '999'

FILE_PATH_PARQUET_EMPRESA = 'gs://dna_public_cnpj_files/trusted_parquet/empresa'
FILE_PATH_PARQUET_ESTABELECIMENTO = 'gs://dna_public_cnpj_files/trusted_parquet/estabelecimento'
FILE_PATH_PARQUET_SIMPLES = 'gs://dna_public_cnpj_files/trusted_parquet/simples'
FILE_PATH_PARQUET_CNAE = 'gs://dna_public_cnpj_files/trusted_parquet/cnae'
FILE_PATH_PARQUET_MUNICIPIO = 'gs://dna_public_cnpj_files/trusted_parquet/municipio'
FILE_PATH_PARQUET_NATUREZA_JURIDICA = 'gs://dna_public_cnpj_files/trusted_parquet/nat_juridica'
FILE_PATH_PARQUET_PAIS = 'gs://dna_public_cnpj_files/trusted_parquet/pais'

FILE_PATH_RAW_EMPRESA = 'gs://dna_public_cnpj_files/raw/*.EMPRECSV.zip'
FILE_PATH_RAW_ESTABELECIMENTO = 'gs://dna_public_cnpj_files/raw/*.ESTABELE.zip'
FILE_PATH_RAW_SIMPLES = 'gs://dna_public_cnpj_files/raw/*.SIMPLES.CSV*.zip'
FILE_PATH_RAW_CNAE = 'gs://dna_public_cnpj_files/raw/*.CNAECSV.zip'
FILE_PATH_RAW_MUNICIPIO = 'gs://dna_public_cnpj_files/raw/*.MUNICCSV.zip'
FILE_PATH_RAW_NATUREZA_JURIDICA = 'gs://dna_public_cnpj_files/raw/*.NATJUCSV.zip'
FILE_PATH_RAW_PAIS = 'gs://dna_public_cnpj_files/raw/*.PAISCSV.zip'

'''
FILE_PATH_PARQUET_EMPRESA = 'gs://bucket-isabella/cnpj/empresa'
FILE_PATH_PARQUET_ESTABELECIMENTO = 'gs://bucket-isabella/cnpj/estabelecimento'
FILE_PATH_PARQUET_SIMPLES = 'gs://bucket-isabella/cnpj/simples'
FILE_PATH_PARQUET_CNAE = 'gs://bucket-isabella/cnpj/cnae'
FILE_PATH_PARQUET_MUNICIPIO = 'gs://bucket-isabella/cnpj/municipio'
FILE_PATH_PARQUET_NATUREZA_JURIDICA = 'gs://bucket-isabella/cnpj/nat_juridica'
FILE_PATH_PARQUET_PAIS = 'gs://bucket-isabella/cnpj/pais'

FILE_PATH_RAW_EMPRESA = 'gs://bucket-isabella/raw/*.EMPRECSV.zip'
FILE_PATH_RAW_ESTABELECIMENTO = 'gs://bucket-isabella/raw/*.ESTABELE.zip'
FILE_PATH_RAW_SIMPLES = 'gs://bucket-isabella/raw/*.SIMPLES.CSV*.zip'
FILE_PATH_RAW_CNAE = 'gs://bucket-isabella/raw/*.CNAECSV.zip'
FILE_PATH_RAW_MUNICIPIO = 'gs://bucket-isabella/raw/*.MUNICCSV.zip'
FILE_PATH_RAW_NATUREZA_JURIDICA = 'gs://bucket-isabella/raw/*.NATJUCSV.zip'
FILE_PATH_RAW_PAIS = 'gs://bucket-isabella/raw/*.PAISCSV.zip'
'''

TOKEN_JSON = '/opt/airflow/dags/auth/dados-publicos-326316-e16540376290.json'

ENCODING_TYPE = 'latin1'
COMPRESSION_TYPE = 'zip'
DELIMITER_TYPE = ';'
TOKEN_STR = 'token'
OS_ENVIRONMENT_GOOGLE = 'GOOGLE_APPLICATION_CREDENTIALS'

DIRS = ['estabelecimento', 'cnae', 'empresa', 'municipio', 'nat_juridica', 'pais', 'simples']
DATASET = 'dna_dataset'