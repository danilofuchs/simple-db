# Simple DB

Simple Database engine written in Python 3.9. Supports basic SQL operations.

Created as a project for the Database course at the Federal Technological University of Paraná (UTFPR).

Imports data from CSV files, MySQL or PostgreSQL databases.

## Import from CSV

```
poetry run python simple_db.py --import-csv "employees"

```

## Import from Postgres

```
poetry run python simple_db.py --import-pg postgresql://postgres:123456@localhost/employees
```

## Import from MySQL

```
poetry run python simple_db.py --import-mysql --database employees --password 123456
```

## Example queries

```
poetry run python simple_db.py --execute "SELECT * FROM EMPLOYEES LIMIT 10"

  emp_no  birth_date    first_name    last_name    gender    hire_date
--------  ------------  ------------  -----------  --------  -----------
   10001  1953-09-02    Georgi        Facello      M         1986-06-26
   10002  1964-06-02    Bezalel       Simmel       F         1985-11-21
   10003  1959-12-03    Parto         Bamford      M         1986-08-28
   10004  1954-05-01    Chirstian     Koblick      M         1986-12-01
   10005  1955-01-21    Kyoichi       Maliniak     M         1989-09-12
   10006  1953-04-20    Anneke        Preusig      F         1989-06-02
   10007  1957-05-23    Tzvetan       Zielinski    F         1989-02-10
   10008  1958-02-19    Saniya        Kalloufi     M         1994-09-15
   10009  1952-04-19    Sumant        Peac         F         1985-02-18
   10010  1963-06-01    Duangkaew     Piveteau     F         1989-08-24
```

```
poetry run python simple_db.py --execute 'SELECT * FROM EMPLOYEES WHERE birth_date > "1950-01-01" ORDER BY birth_date ASC LIMIT 10'

  emp_no  birth_date           first_name    last_name     gender    hire_date
--------  -------------------  ------------  ------------  --------  -------------------
   65308  1952-02-01 00:00:00  Jouni         Pocchiola     M         1985-03-10 00:00:00
   87461  1952-02-01 00:00:00  Moni          Decaestecker  M         1986-10-06 00:00:00
   91374  1952-02-01 00:00:00  Eishiro       Kuzuoka       M         1992-02-12 00:00:00
  207658  1952-02-01 00:00:00  Kiyokazu      Whitcomb      M         1988-07-26 00:00:00
  237571  1952-02-01 00:00:00  Ronghao       Schaad        M         1988-07-10 00:00:00
  406121  1952-02-01 00:00:00  Supot         Remmele       M         1989-01-27 00:00:00
   12282  1952-02-02 00:00:00  Tadahiro      Delgrange     M         1997-01-09 00:00:00
   13944  1952-02-02 00:00:00  Takahito      Maierhofer    M         1989-01-18 00:00:00
   22614  1952-02-02 00:00:00  Dung          Madeira       M         1989-01-24 00:00:00
```

```
poetry run python simple_db.py --execute "SELECT * FROM employees WHERE gender = 'M' AND hire_date > '1989-01-01' LIMIT 10"

  emp_no  birth_date    first_name    last_name    gender    hire_date
--------  ------------  ------------  -----------  --------  -----------
   10001  1953-09-02    Georgi        Facello      M         1986-06-26
   10002  1964-06-02    Bezalel       Simmel       F         1985-11-21
   10003  1959-12-03    Parto         Bamford      M         1986-08-28
   10004  1954-05-01    Chirstian     Koblick      M         1986-12-01
   10005  1955-01-21    Kyoichi       Maliniak     M         1989-09-12
   10006  1953-04-20    Anneke        Preusig      F         1989-06-02
   10007  1957-05-23    Tzvetan       Zielinski    F         1989-02-10
   10008  1958-02-19    Saniya        Kalloufi     M         1994-09-15
   10009  1952-04-19    Sumant        Peac         F         1985-02-18
   10010  1963-06-01    Duangkaew     Piveteau     F         1989-08-24
```

```
poetry run python simple_db.py --execute "SELECT * FROM employees WHERE hire_date > birth_date LIMIT 10"

  __id    emp_no  birth_date           first_name    last_name    gender    hire_date
------  --------  -------------------  ------------  -----------  --------  -------------------
     0     10001  1953-09-02 00:00:00  Georgi        Facello      M         1986-06-26 00:00:00
     1     10002  1964-06-02 00:00:00  Bezalel       Simmel       F         1985-11-21 00:00:00
     2     10003  1959-12-03 00:00:00  Parto         Bamford      M         1986-08-28 00:00:00
     3     10004  1954-05-01 00:00:00  Chirstian     Koblick      M         1986-12-01 00:00:00
     4     10005  1955-01-21 00:00:00  Kyoichi       Maliniak     M         1989-09-12 00:00:00
     5     10006  1953-04-20 00:00:00  Anneke        Preusig      F         1989-06-02 00:00:00
     6     10007  1957-05-23 00:00:00  Tzvetan       Zielinski    F         1989-02-10 00:00:00
     7     10008  1958-02-19 00:00:00  Saniya        Kalloufi     M         1994-09-15 00:00:00
     8     10009  1952-04-19 00:00:00  Sumant        Peac         F         1985-02-18 00:00:00
     9     10010  1963-06-01 00:00:00  Duangkaew     Piveteau     F         1989-08-24 00:00:00
```

```
poetry run python simple_db.py --execute 'SELECT departments.dept_no, dept_manager.emp_no FROM departments JOIN dept_manager ON dept_manager.dept_no = departments.dept_no WHERE dept_manager.dept_no = "d006"'

departments.dept_no      dept_manager.emp_no
---------------------  ---------------------
d006                                  110725
d006                                  110765
d006                                  110800
d006                                  110854
```

```
poetry run python simple_db.py --execute "INSERT INTO departments(dept_no, dept_name) VALUES ('d999', 'Test department')"

Inserted row
```

```
poetry run python simple_db.py --execute "UPDATE departments SET dept_name = 'Test department 2' WHERE dept_no = 'd999'"

Updated 1 row
```

```
poetry run python simple_db.py --execute "DELETE FROM departments WHERE dept_no = 'd999'"

Deleted 1 row
```

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
