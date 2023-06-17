# Simple DB

Simple Database engine written in Python 3.9. Supports basic SQL operations.

Created as a project for the Database course at the Federal Technological University of Paraná (UTFPR).

Imports data from CSV files, MySQL or PostgreSQL databases.

## Supported operations

- `SELECT` - select data from table
  - `WHERE` - filter data
  - `ORDER BY` - sort data
  - `INNER JOIN` - join tables
- `INSERT` - insert data into table
- `UPDATE` - update data in table
- `DELETE` - delete data from table

## Assignment

O trabalho consiste no desenvolvimento de uma ferramenta de gerenciamento de bancos de dados, baseada em ingestão de dados de fontes externas e operações e consultas processadas nas tabelas.

Importação de dados:

    CSV e Importação de bancos de dados existentes

    Bancos de Dados Existentes
    conexão a um banco de dados existente (MySQL ou PostgreSQL)
    seleção do banco de dados
    seleção das tabelas para importação

    CSV
    selecionar um diretório onde estarão os arquivos de dados em formato CSV
    carregar um arquivo para cada tabela, com o nome do arquivo dando o nome à tabela

O FORMATO DE ARQUIVO PARA ARMAZENAMENTO INTERNO DOS DADOS SERÁ DE ESCOLHA DA EQUIPE

Gerenciamento e manipulação de dados

    permitir a consulta aos dados, em formato SQL, com as seguintes cláusulas possíveis:

    -projeção (lista de campos ou *)
    -filtros (where)
    -ordenação (order by)


    os filtros e ordenação poderão ser feitos por um ou dois campos, com modificadores AND e OR
    o gerenciador deverá ser capaz de implementar inner joins, permitindo a sintaxe USING e ON.

    deverão ser implementados os comandos de manipulação INSERT, UPDATE e DELETE

Os testes deverão ser feitos com a base de dados de exemplo Employee, disponível em https://github.com/datacharmer/test_db

O trabalho deverá ser executado nas linguagens Python (versão 3) ou Java (versão 11 ou superior). Se for desenvolvido em Java, deverá ser um projeto do NetBeans, se for em Python, um diretório com scripts.

NÃO deverão ser usadas bibliotecas específicas para parseamento do comando SQL ou tratamento dos dados, todas as tarefas dentro do programa deverão ser implementadas pelos alunos.

A entrega do trabalho será o código-fonte e um relatório com a descrição das soluções de ingestão, processamento das queries, armazenamento e outros aspectos.
