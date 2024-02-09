from datetime import datetime
import logging
import os

import json
import boto3

PATH = os.path.dirname(os.path.abspath(__file__))
LOG_FOLDERNAME = "logs"
SCHEMA_FILENAME = "schema.json"
TYPE_MAPPING = {"string": str, "integer": int, "object": dict, "boolean": bool}

def send_event_to_queue(event, queue_name):
    '''
     Responsável pelo envio do evento para uma fila
    :param event: Evento (dict)
    :param queue_name: Nome da fila (str)
    :return: None
    '''

    sqs_client = boto3.client("sqs", region_name="us-east-1")
    response = sqs_client.get_queue_url(
        QueueName=queue_name
    )
    queue_url = response['QueueUrl']
    response = sqs_client.send_message(
        QueueUrl=queue_url,
        MessageBody=json.dumps(event)
    )
    print(f"Response status code: [{response['ResponseMetadata']['HTTPStatusCode']}]")


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
        with open(os.path.join(path, file), "r", encoding='utf-8') as json_file:
            return json.load(json_file)

    except FileNotFoundError:
        logger.error("Erro: Arquivo não encontrado.")
     
    except json.JSONDecodeError:
        logger.error("Erro: Estrutura de dado JSON inválido.")


class Validation:
    '''Responsável pela realização das validações do evento'''
    
    def __init__(self, event: dict, logger: logging) -> None:
        '''
        Inicializa a classe.
        :param event: Evento (dict)
        :param logger: Logger (logging)
        :return: None
        '''

        self.event = event
        self.logger = logger
    

    def _get_custom_type(self, custom_type: str, custom_type_mapping: list = None) -> str:
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


    def validate_event_not_empty(self) -> bool:
        '''
        Responsável por validar se o evento está vazio
        :return: Evento não vazio? (bool)
        '''
        
        if not self.event:
            self.logger.error("Erro: Evento vazio.")
            return False
        
        self.logger.info("Info: Evento não vazio.")
        return True
    

    def validate_event_data_structure(self) -> bool:
        '''
        Responsável pela validação da estrutura de dado do event
        :return: Evento possui a estrutura de dados correta? (bool)
        '''

        if not isinstance(self.event, dict):
            self.logger.error(
                "Erro: Estrutura de dados do evento errada. Esperado: dict. "
                f"Recebido: {type(self.event).__name__}."
            )
            return False
        
        self.logger.info("Info: Evento com a estrutura de dados correta.")
        return True
    

    def compare_event_fields(self, schema: dict, event:dict = None) -> bool:
        '''
        Responsável pela comparação dos campos entre o evento e o schema
        Valida no evento a existência de todos os campos obrigatórios
        Valida no evento a existẽncia de campos não registrados no schema
        :param schema: Schema (dict)
        :param event: Event (dict)
        :return: Evento passou por ambas validações? (bool)
        '''

        if not event:
            event = self.event

        try:
            required_fields = set(schema["required"])
        except KeyError:
            self.logger.error("Erro: Evento não possui o parâmetro: required.")
            return False
        
        events_fields = set(event.keys())
        
        missing_required_fields = required_fields.difference(events_fields)
        fields_not_required = events_fields.difference(required_fields)

        if missing_required_fields:
            self.logger.error(f"Erro: Evento não possui o/s campo/s necessário/s: {missing_required_fields}")
            return False
        if fields_not_required:
            self.logger.error(f"Erro: Campo/s não registrado/s no schema: {fields_not_required}")
            return False
        
        self.logger.info("Info: Campos registrados no schema.")
        return True
        

    def validate_event_content(self, schema: dict, event:dict = None) -> bool:
        '''
        Responsável pela validação do conteúdo do evento, inclusive em dados aninhados 
        Valida no evento a tipagem dos campos corresponde ao que foi registrado no schema
        Realiza as validações da funcão: compare_event_fields
        :param schema: Schema (dict)
        :param event: Event (dict)
        :return: Evento passou por todas validações? (bool)
        '''

        if not event:
            event = self.event

        if not self.compare_event_fields(schema, event):
            return False
        
        for key, value in event.items():
            custom_type = self._get_custom_type(custom_type=schema["properties"][key]["type"])
            if not custom_type:
                return False
            if not isinstance(value, custom_type):
                self.logger.error(
                    f"Erro: Tipagem do campo divergente do schema. Campo: {key}. "
                    f"Esperado: {schema['properties'][key]['type']}, Recebido: {type(value).__name__}."
                )
                return False

            if isinstance(value, dict):
                return self.validate_event_content(schema["properties"][key], value)
    
        self.logger.info("Info: Conteúdo do evento validado.")
        return True
    

def handler(event):
    '''
    Responsável pelas chamadas dos processos necessários para a validação do evento,
    e caso seja aprovado por todas as validações, envia o evento para a fila.
    :param event: Evento (dict)
    :return: None
    '''
   
    logger = create_logger()

    validation = Validation(event, logger)

    if validation.validate_event_not_empty() and validation.validate_event_data_structure():
        schema = read_schema(logger)     
        if schema:
            if validation.validate_event_content(schema):
                send_event_to_queue(event, "valid-events-queue")
                logger.info("Sucesso: Evento enviado com sucesso.")
