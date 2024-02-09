from datetime import datetime
import logging
import os

import json

_ATHENA_CLIENT = None

PATH = os.path.dirname(os.path.abspath(__file__))
LOG_FOLDERNAME = "logs"
SCHEMA_FILENAME = "schema.json"
TYPE_MAPPING = {
    "string": "varchar",
    "integer": "tinyint", 
    "object": "struct", 
    "boolean": "boolean"
}

def create_hive_table_with_athena(query):
    '''
    Função necessária para criação da tabela HIVE na AWS
    :param query: Script SQL de Create Table (str)
    :return: None
    '''
    
    print(f"Query: {query}")
    _ATHENA_CLIENT.start_query_execution(
        QueryString=query,
        ResultConfiguration={
            'OutputLocation': f's3://iti-query-results/'
        }
    )

def create_logger(log_level: logging = logging.INFO) -> logging:
    '''
    Responsável pela criação do logger
    :param log_level: Menor level severidade a ser registrado
    :return: logger (logging)
    '''
        
    logger = logging.getLogger(__name__)
    logger.setLevel(log_level)

    log_file = f"logger_{datetime.now().strftime('%Y%m%d')}.log"

    folder_path = os.path.join(PATH,LOG_FOLDERNAME)
    if not os.path.exists(folder_path):
        os.mkdir(folder_path)
    
    file_handler = logging.FileHandler(os.path.join(folder_path,log_file), mode='a')
    file_handler.setLevel(log_level)

    formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
    file_handler.setFormatter(formatter)

    logger.addHandler(file_handler)

    print("Logger criado com sucesso!")

    return logger


def read_schema(logger: logging, file: str = SCHEMA_FILENAME, path: str = PATH) -> dict:
    '''
    Responsável pela leitura do schema
    :param file: Nome do arquivo (str)
    :param path: Caminho do arquivo (str)
    :return: schema (dict)
    '''

    try:
        with open(os.path.join(path, file), "r", encoding="utf-8") as json_file:
            return json.load(json_file)

    except FileNotFoundError:
        logger.error("Erro: Arquivo não encontrado.")
     
    except json.JSONDecodeError:
        logger.error("Erro: Estrutura de dado JSON inválido.")


class Query:
    '''Reponsável pela geração da query'''

    def __init__(self, tb_name : str, db_name : str, logger: logging) -> None:
        '''
        Inicializa a classe
        :param tb_name: Nome da tabela (str)
        :param db_name: Nome da database (str)
        :param logger: Logger (logging)
        :return: None
        '''

        self.tb_name = tb_name
        self.db_name = db_name
        self.logger = logger


    def _get_custom_type(self, custom_type: str, custom_type_mapping: dict = None):
        '''
        Responsável pelo retorno da tipagem em python da tipagem customizada. Ex: string -> str
        :param custom_type: Tipagem customizada (str)
        :param custom_type_mapping: Mapeamento da tipagem customizada (str)
        :return: Tipagem em python (str)
        '''
        if not custom_type_mapping:
            custom_type_mapping = TYPE_MAPPING

        try:
            return custom_type_mapping[custom_type]
        except KeyError:
            self.logger.error(
                f"Erro: Tipagem customizada do campo não encontrada. Tipo: {custom_type}. "
                "Favor atualizar a lista: TYPE_MAPPING."
            )
    

    def create_table(
            self,
            col_params : dict = None,
            tb_desc : str = None,
            partition_params : dict = None,
            clustering_params : list = None,
            num_buckets : int = None,
            row_format : str = None,
            file_format : str = None,
            serde_properties : dict = None,
            location : str = None,
            tbl_properties : dict = None
        ):
        '''
        Responsável escrita da query de criação de tabela
        :param col_params: Informação das colunas: nome, tipo, comentário (dict)
        :param tb_desc: Comentário da tabela (str)
        :param partition_params: Informação das colunas de partição: nome, tipo, comentário (dict)
        :param clustering_params: Informação das colunas de clusterização: nome (list)
        :param num_buckets: Número de buckets (int)
        :param row_format: Informação do formato de linha (str)
        :param file_format: Informação do formato do arquivo (str)
        :param serde_properties: Informação das propriedades de linha (dict)
        :param location: Local de armazenamento (str)
        :param tbl_properties: Informação das propriedades da tabela (dict)
        :return: query (str)
        '''
        
        self.logger.info("Info: Início da montagem da query de criação de tabelas.")
        
        query = f"CREATE EXTERNAL TABLE IF NOT EXISTS {self.db_name}.{self.tb_name}"
        self.logger.info("Info: Incluido: Nome do database e da tabela.")
        
        if col_params:
            col_params = self.format_table_columns(col_params)
            query+=f"\n({col_params})"
            self.logger.info("Info: Incluido: Informação das colunas.")
        
        if tb_desc:
            query+=f"\nCOMMENT '{tb_desc}'"
            self.logger.info("Info: Incluido: Comentário da tabela.")
        
        if partition_params:
            partition_params = self.format_table_columns(partition_params) 
            query+=f"\nPARTITIONED BY ({partition_params})"
            self.logger.info("Info: Incluido: Configuração de partição da tabela.")
        
        if clustering_params or num_buckets:
            if clustering_params and num_buckets:
                clustering_params = ",".join(clustering_params)
                query+=f"\nCLUSTERED BY ({clustering_params}) INTO {num_buckets} BUCKETS"
                self.logger.info("Info: Incluido: Configuração de clusterização da tabela.")
            else:
                self.logger.warning("""Atenção: Configuração de clusterização necessita de ambos 
                                parâmetros: Coluna/s: {clustering_params},
                                Número de buckets: {num_buckets}.""")
        
        if row_format:
            query+=f"\nROW FORMAT '{row_format}'"
            self.logger.info("Info: Incluido: Configuração de formato de linha da tabela.")
        
        if file_format:
            query+=f"\nSTORED AS {file_format}"
            self.logger.info("Info: Incluido: Configuração de formato de arquivo da tabela.")
        
        if serde_properties:
            serde_properties = self.format_table_properties(serde_properties)
            query+=f"\nWITH SERDEPROPERTIES ({serde_properties})"
            self.logger.info("Info: Incluido: Propriedades da linha da tabela.")
        
        if location:
            query+=f"\nLOCATION '{location}'"
            self.logger.info("Info: Incluido: Configuração de local da tabela.")
        
        if tbl_properties:
            tbl_properties = self.format_table_properties(tbl_properties)
            query+=f"\nTBLPROPERTIES ({tbl_properties})"
            self.logger.info("Info: Incluido: Propriedades da tabela.")
        
        self.logger.info("Info: Fim da montagem da query de criação de tabelas")
        self.logger.info(f"Info: Query: {query}")
        return query
    

    def format_table_columns(self, col_dict: dict, separator: str = "") -> str:
        '''
        Responsável pela formatação dos dados de coluna: 
            nome separator tipo<nome tipo> COMMENT comentário
        :param col_dict: Informações de colunas: nome, tipo, comentário (dict)
        :param separator: Separador entre o nome e tipo (str). Ex: nome tipo, nome: tipo
        :return: Formatação das colunas (str)
        '''
        
        col_data_list = []
        for key, value in col_dict.items():
        
            try:
                col_data = f"{key}{separator} {self._get_custom_type(value['type'])}"
            except NameError:
                self.logger.error("Erro: Dicionário de colunas não possui o campo: 'type'")
        
            if value['type'] == 'object':
                try:
                    obj_columns = self.format_table_columns(value['properties'], separator=':')
                except NameError:
                    self.logger.error("Erro: Dicionário de colunas não possui o campo: 'properties'")
                    
                col_data += f" <{obj_columns}>"
            
            try:
                comment = value['description']
            except NameError:
                self.logger.error("Erro: Dicionário de colunas não possui o campo: 'description'")
                
            if comment:
                col_data += f" COMMENT '{comment}'"
                
            col_data_list.append(col_data)
            
        return ",\n".join(col_data_list)
    

    def format_table_properties(self, tb_properties: dict) -> str:
        '''
        Responsável pela formatação das propriedades da tabela 
        :param tb_properties: Propriedades da tabela
        :return: Formatação das propriedades (str)
        '''
        return ",".join([f"'{key}' = '{value}'" for key, value in tb_properties.items()])

    
class Schema():
    '''Responsável pela obtenção de dados do schema'''

    def __init__(self, schema: dict, logger: logging) -> None:
        '''
        Inicializa a classe
        :param schema: Schema (dict)
        :param logger: logger (logging)
        :return: None
        '''

        self.schema = schema
        self.logger = logger


    def get_col_data(self, schema: dict = None, filtered_col: list = None) -> dict:
        '''
        Responsável pela obtenção dos dados das colunas (aninhadas)
        :param schema: Schema (dict)
        :param filtered_col: Filtra as colunas a serem obtidas (list)
        :return: Dados das colunas (dict)
        '''

        self.logger.info("Info: Inicio coleta de dados de coluna do schema")
        if not schema:
            schema = self.schema

        if filtered_col:
            schema["properties"] = {key: schema["properties"][key] for key in filtered_col}
            self.logger.info(f"Info: Filtrado a coleta de dados para a/s coluna/s: {filtered_col}")

        col_dict = {}
        for key, value in schema["properties"].items():
            col_dict[key] = {"type":value["type"], "description":value["description"]}
            if value["type"] == "object":
                col_dict[key] = dict(col_dict[key], **{"properties":self.get_col_data(schema=value)})
        
        self.logger.info("Info: Termino da coleta de dados de coluna do schema.")
        return col_dict
    
    
    def get_tb_comment(self) -> str:
        '''
        Responsável pela obtenção do comentário da tabela
        :return: Comentário da tabela
        '''
        self.logger.info("Info: Coleta de comentário da tabela")
        return self.schema["description"]
    
            
def handler():
    '''
    Responsável pelas chamadas dos processos necessários para a geração da query
    de criação de tabelas, e sua execução
    :return: None
    '''

    TB_NAME = "tb_user"
    DB_NAME = "db_people"
    PARTITION_PARAMS = []
    CLUSTERING_PARAMS = ["age"]
    NUM_BUCKETS = 32
    ROW_FORMAT = "org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe"
    FILE_FORMAT = "PARQUET"
    SERDE_PROPERTIES = {'parquet.compress':'SNAPPY', 'serialization.format':'1'}
    LOCATION = "s3://iti-query-results/"
    TBL_PROPERTIES = {"has_encrypted_data": True}

    logger = create_logger()

    schema_dict = read_schema(logger)
    
    if schema_dict:
    
        schema = Schema(schema_dict, logger)
    
        query = Query(tb_name=TB_NAME, db_name=DB_NAME, logger=logger)
    
        query_str = query.create_table(
            col_params = schema.get_col_data(),
            tb_desc = schema.get_tb_comment(),
            partition_params = schema.get_col_data(filtered_col=PARTITION_PARAMS),
            clustering_params = CLUSTERING_PARAMS,
            num_buckets = NUM_BUCKETS,
            row_format = ROW_FORMAT,
            file_format = FILE_FORMAT,
            serde_properties = SERDE_PROPERTIES,
            location = LOCATION,
            tbl_properties = TBL_PROPERTIES,
        )

        create_hive_table_with_athena(query_str)
        