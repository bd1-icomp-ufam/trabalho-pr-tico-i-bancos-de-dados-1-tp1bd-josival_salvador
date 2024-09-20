Trabalho Prático 1

Autor: Josival Salvador Monteiro Júnior
Matrícula: 22052932

OBS: Não Consegui utilizar o PostgreSQL espero que considere o trabalho.

Descrição Geral

Este projeto contém dois scripts principais, tp1.3.2.py e tp1.3.3.py, que fazem o processamento de dados do arquivo amazon-meta.txt. Esse arquivo contém informações sobre produtos e avaliações, sendo necessário descompactar o conteúdo baixado para chegar no formato correto (amazon-meta.txt).

Estrutura dos Scripts
1. tp1.3.2.py

    Linguagem: Python
    Bibliotecas utilizadas: pandas, sqlite3
    Objetivo: Criar um banco de dados SQLite (banco_de_dados.db) a partir das informações contidas no arquivo amazon-meta.txt.
    Saída: Um arquivo de banco de dados SQLite (.db) com tabelas estruturadas para armazenar dados sobre produtos, categorias, avaliações, etc.

2. tp1.3.3.py

    Linguagem: Python
    Objetivo: Realizar consultas no banco de dados gerado e exibir os resultados diretamente no terminal.
    Saída: Resultados das consultas exibidos no terminal.


Entrada de Dados

    Arquivo de entrada: amazon-meta.txt
        Esse arquivo deve ser descompactado a partir do conteúdo baixado do site de origem, até chegar no formato exato de amazon-meta.txt.


Instruções

    Preparação do ambiente:
        Certifique-se de ter o Python instalado em sua máquina.
        Instale as bibliotecas necessárias (pandas).