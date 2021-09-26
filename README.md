# Projeto Hackathon A3 Data - DNA

### Este repositório contém arquivos do projeto A3Data realizado pela equipe DNA composta por Isabella e Lays.

Os times devem implementar pipeline de extração, transformação e disponibilização de dados. Após extração, limpeza, organização e estruturação dos dados, as perguntas chave do desafio devem ser respondidas de maneira visual.

1. Número de indústrias ativas por mês/ano entre 2010 - 2021, discriminado por MEI 
ou Simples, em cada município brasileiro
2. Número de comércios que fecharam por mês/ano entre 2010 - 2021, discriminado 
por MEI ou Simples, em cada município brasileiro
3. Número de CNPJ novos por mês/ano entre 2010 - 2021, discriminado por MEI ou 
Simples, em cada município brasileiro
4. Qual o número de CNPJ que surgiram do grupo de educação superior, entre 2015 
e 2021, discriminado por ano, em cada estado brasileiro?
5. Qual a classe de CNAE com o maior capital social médio no Brasil no último ano?
6. Qual a média do capital social das empresas de pequeno porte por natureza 
jurídica no último ano?

## Proposta

O projeto consiste na construção de um Pipeline de dados abertos de CNPJ disponibilizados pelo governo em https://www.gov.br/receitafederal/pt-br/assuntos/orientacao-tributaria/cadastros/consultas/dados-publicos-cnpj do Ministério da Economia junto com a Receita Federal.
Desenvolvimento

A automatização do processo foi feito em Python através de arquivos .py (Python) e a esteira dos dados feita no Airflow. O armazenamento foi realizado no Bucket Cloud Storage do Google. O processamento dos dados foram feitos utilizando a biblioteca Dask em um container Docker com uma imagem do Airflow. Foram criadas tabelas no BigQuery onde foi alimentado com os dados do Bucket após tratados e convertidos para arquivos parquet para serem visualizados em Dashboad via Power BI.

O vídeo de apresentação pode ser acompanhado aqui no youtube: 2min - https://youtu.be/v8SJeva1AHs | 5 min - https://youtu.be/oFjQACs78Jk

## Etapas realizadas:

Extração dos arquivos da página do Ministério da Economia
Limpeza, tratamentos dos dados e conversão para parquet via Python
Armazenamento da base completa em formato Parquet no BigQuery, bem como criação das tabelas
Power BI para visualizção dos dados no BigQuery

## Procedimentos

### Arquitetura:


![](images/MicrosoftTeams-image.png)

As tecnologias trabalhadas nesse projeto são baseadas na Cloud da Azure:

### DAG Airflow:

![](images/dag.PNG)


Airflow com os steps de processamentos de dados com o Python:

![](images/airflow.PNG)

Bucket com os arquivos parquet:

![](images/dados-salvos-formato-parquet.PNG)


### Power BI - Visualização dos dados:

![image](https://user-images.githubusercontent.com/35038689/134792038-f09de045-d064-4964-95d7-3861b96da3b5.png)

![image](https://user-images.githubusercontent.com/35038689/134792056-bafcd6a6-ff83-4280-aaf2-e9cdf4efd119.png)

![image](https://user-images.githubusercontent.com/35038689/134792071-2dcd2d76-92d6-4b1d-a324-c8dc7c863989.png)

![image](https://user-images.githubusercontent.com/35038689/134792082-08c91e8f-be56-4d9a-80ad-42ee2c725e0d.png)

![image](https://user-images.githubusercontent.com/35038689/134792169-3112f8fa-72ce-4c33-a16a-a3c432d9dbcc.png)

![image](https://user-images.githubusercontent.com/35038689/134792184-fdb090dc-1d6b-45c8-b8e2-c5a120ce4ebb.png)

![image](https://user-images.githubusercontent.com/35038689/134792288-e140f8bc-6187-4325-904d-16e7445c680b.png)

![image](https://user-images.githubusercontent.com/35038689/134792305-f762ae7f-f13a-41c1-a97a-4cf8e10580fb.png)

![image](https://user-images.githubusercontent.com/35038689/134792318-a9eadabd-de71-47c5-a050-5abf09d3ed40.png)
    

### Estrutura do Bucket:

Decidimos estruturar o Bucket em 2 camadas:

A camada RAW onde fica os dados brutos baixados compactados.
A camada TRUSTED onde fica os dados brutos armazenados em Parquet.

## Custos do Projeto
Foi utilizado o Free Trial oferecido pelo Google para o uso dos serviços
![image](https://user-images.githubusercontent.com/35038689/134792466-bac80a07-e194-4bcd-898c-4d30311276e0.png)
