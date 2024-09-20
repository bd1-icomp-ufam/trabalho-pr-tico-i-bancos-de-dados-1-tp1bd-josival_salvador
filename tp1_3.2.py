import sqlite3
import re
import threading

def criar_banco_de_dados():
    # Conectar ao banco de dados (ou criar, se não existir)
    conn = sqlite3.connect('banco_de_dados.db')
    cursor = conn.cursor()

    # Criação da tabela 'produto'
    cursor.execute('''
    CREATE TABLE IF NOT EXISTS produto (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        asin VARCHAR(20) UNIQUE NOT NULL,
        title TEXT,
        group_name VARCHAR(50),
        salesrank INTEGER
    );
    ''')

    # Criação da tabela 'categoria'
    cursor.execute('''
    CREATE TABLE IF NOT EXISTS categoria (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        category_name TEXT,
        parent_id INTEGER,
        FOREIGN KEY (parent_id) REFERENCES categoria(id) ON DELETE SET NULL
    );
    ''')

    # Criação da tabela de relacionamento 'produto_categoria'
    cursor.execute('''
    CREATE TABLE IF NOT EXISTS produto_categoria (
        produto_id INTEGER,
        categoria_id INTEGER,
        PRIMARY KEY (produto_id, categoria_id),
        FOREIGN KEY (produto_id) REFERENCES produto(id) ON DELETE CASCADE,
        FOREIGN KEY (categoria_id) REFERENCES categoria(id) ON DELETE CASCADE
    );
    ''')

    # Criação da tabela 'produto_similar'
    cursor.execute('''
    CREATE TABLE IF NOT EXISTS produto_similar (
        produto_id INTEGER,
        similar_asin VARCHAR(20),
        PRIMARY KEY (produto_id, similar_asin),
        FOREIGN KEY (produto_id) REFERENCES produto(id) ON DELETE CASCADE
    );
    ''')

    # Criação da tabela 'avaliacao'
    cursor.execute('''
    CREATE TABLE IF NOT EXISTS avaliacao (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        produto_id INTEGER,
        data DATE,
        cliente_id VARCHAR(20),
        rating INTEGER,
        votos INTEGER,
        votos_uteis INTEGER,
        FOREIGN KEY (produto_id) REFERENCES produto(id) ON DELETE CASCADE
    );
    ''')

    # Salvar (commit) e fechar a conexão
    conn.commit()
    conn.close()

# versão anterior da função de inserir
def inserir_no_banco_assincrono2(itens):

    for dado in itens:
        num_reviews = len(dado.get('reviews', []))
        total_summary_reviews = dado.get('review_summary', {}).get('total', 0)

        if num_reviews != total_summary_reviews:
            missing_reviews = total_summary_reviews - num_reviews
            if missing_reviews > 0:
                for _ in range(missing_reviews):
                    dado['reviews'].append({
                        'date': None,
                        'cutomer': None,
                        'rating': None,
                        'votes': None,
                        'helpful': None
                    })
            else:
                dado['reviews'] = sorted(dado['reviews'], key=lambda x: x['helpful'], reverse=True)
                dado['reviews'] = dado['reviews'][:total_summary_reviews]

    conn = sqlite3.connect('banco_de_dados.db')
    cursor = conn.cursor()

    # Preparar as listas para inserções em lote
    produtos = []
    categorias_produto = []
    produtos_similares = []
    avaliacoes = []
    categorias_para_inserir = []

    for item in itens:
        # Inserir produto
        produtos.append((item['id'], item['asin'], item['title'], item['group_name'], item['salesrank']))

        # Inserir categorias (todas as subcategorias) e criar relacionamento com o produto apenas na última
        for categoria in item['categories']:
            categorias = categoria.split('|')[1:]  # Ignora o primeiro item, que é vazio
            parent_id = None  # Variável para rastrear o parent_id

            # Percorre todas as categorias do caminho e popula a tabela de categorias
            for i, cat in enumerate(categorias):
                if cat:
                    cat_name = cat.split('[')[0]
                    cat_id = cat.split('[')[-1].split(']')[0]

                    # Inserir a categoria, mantendo o parent_id para a hierarquia
                    categorias_para_inserir.append((cat_id, cat_name, parent_id))

                    # Atualizar o parent_id para o próximo nível da hierarquia
                    parent_id = cat_id

            # Pegar somente a última categoria de cada caminho e associá-la ao produto
            if categorias:
                ultima_categoria = categorias[-1]
                cat_id = ultima_categoria.split('[')[-1].split(']')[0]

                # Relacionar o produto com a última categoria
                categorias_produto.append((item['id'], cat_id))

        # Inserir produtos similares
        for similar_asin in item['similar']:
            produtos_similares.append((item['id'], similar_asin))

        # Inserir avaliações
        for review in item['reviews']:
            avaliacoes.append(
                (item['id'], review['date'], review['cutomer'], review['rating'], review['votes'], review['helpful']))

    # Inserir dados em lote
    cursor.executemany('''
        INSERT OR IGNORE INTO produto (id, asin, title, group_name, salesrank)
        VALUES (?, ?, ?, ?, ?)
    ''', produtos)

    cursor.executemany('''
        INSERT OR IGNORE INTO categoria (id, category_name, parent_id)
        VALUES (?, ?, ?)
    ''', categorias_para_inserir)

    cursor.executemany('''
        INSERT OR IGNORE INTO produto_categoria (produto_id, categoria_id)
        VALUES (?, ?)
    ''', categorias_produto)

    cursor.executemany('''
        INSERT OR IGNORE INTO produto_similar (produto_id, similar_asin)
        VALUES (?, ?)
    ''', produtos_similares)

    cursor.executemany('''
        INSERT INTO avaliacao (produto_id, data, cliente_id, rating, votos, votos_uteis)
        VALUES (?, ?, ?, ?, ?, ?)
    ''', avaliacoes)

    print("Foi inserido: ", len(itens))
    # Salvar (commit) e fechar a conexão
    conn.commit()
    conn.close()


# Essa função lida com os problemas da review.
# Depois junta os dados para serem inseridos no banco de uma unica so vez.
# A quantidade é determinada pelo tamanho do buffer da função de processar arquivo.
# O ideal é ser um valor alto pq o sqlite lida com inserção melhor se for varios de uma so vez.
def inserir_no_banco_assincrono(itens):

    for dado in itens:
        num_reviews = len(dado.get('reviews', []))
        total_summary_reviews = dado.get('review_summary', {}).get('total', 0)

        if num_reviews != total_summary_reviews:
            missing_reviews = total_summary_reviews - num_reviews
            if missing_reviews > 0:
                for _ in range(missing_reviews):
                    dado['reviews'].append({
                        'date': None,
                        'cutomer': None,
                        'rating': None,
                        'votes': None,
                        'helpful': None
                    })
            else:
                dado['reviews'] = sorted(dado['reviews'], key=lambda x: x['helpful'], reverse=True)
                dado['reviews'] = dado['reviews'][:total_summary_reviews]

    conn = sqlite3.connect('banco_de_dados.db')
    cursor = conn.cursor()

    # Preparar as listas para inserções em lote
    produtos = []
    categorias_produto = []
    produtos_similares = []
    avaliacoes = []
    categorias_para_inserir = []

    for item in itens:
        # Inserir produto
        produtos.append((item['id'], item['asin'], item['title'], item['group_name'], item['salesrank']))

        # Inserir categorias (todas as subcategorias) e criar relacionamento com o produto apenas na última
        for categoria in item['categories']:
            categorias = categoria.split('|')[1:]  # Ignora o primeiro item, que é vazio
            parent_id = None  # Variável para rastrear o parent_id

            # Percorre todas as categorias do caminho e popula a tabela de categorias
            for i, cat in enumerate(categorias):
                if cat:
                    cat_name = cat.split('[')[0]
                    cat_id = cat.split('[')[-1].split(']')[0]

                    # Inserir a categoria, mantendo o parent_id para a hierarquia
                    categorias_para_inserir.append((cat_id, cat_name, parent_id))

                    # Atualizar o parent_id para o próximo nível da hierarquia
                    parent_id = cat_id

            # Pegar somente a última categoria de cada caminho e associá-la ao produto
            if categorias:
                ultima_categoria = categorias[-1]
                cat_id = ultima_categoria.split('[')[-1].split(']')[0]

                # Relacionar o produto com a última categoria
                categorias_produto.append((item['id'], cat_id))

        # Inserir produtos similares
        for similar_asin in item['similar']:
            produtos_similares.append((item['id'], similar_asin))

        # Inserir avaliações
        for review in item['reviews']:
            avaliacoes.append(
                (item['id'], review['date'], review['cutomer'], review['rating'], review['votes'], review['helpful']))

    # Inserir dados em lote
    cursor.executemany('''
        INSERT OR IGNORE INTO produto (id, asin, title, group_name, salesrank)
        VALUES (?, ?, ?, ?, ?)
    ''', produtos)

    cursor.executemany('''
        INSERT OR IGNORE INTO categoria (id, category_name, parent_id)
        VALUES (?, ?, ?)
    ''', categorias_para_inserir)

    cursor.executemany('''
        INSERT OR IGNORE INTO produto_categoria (produto_id, categoria_id)
        VALUES (?, ?)
    ''', categorias_produto)

    cursor.executemany('''
        INSERT OR IGNORE INTO produto_similar (produto_id, similar_asin)
        VALUES (?, ?)
    ''', produtos_similares)

    cursor.executemany('''
        INSERT INTO avaliacao (produto_id, data, cliente_id, rating, votos, votos_uteis)
        VALUES (?, ?, ?, ?, ?, ?)
    ''', avaliacoes)

    print("Foi inserido: ", len(itens), " no Banco de Dados.")
    # Salvar (commit) e fechar a conexão
    conn.commit()
    conn.close()


# Essas funções são responsaveis por gerenciar os formatos de entrada.
# Elas n tem uma lógica complicada. Dessa forma, são usadas somente pra ajustar a entrada
def processar_id(linha, entrada_atual, buffer):
    if entrada_atual:  # Se houver uma entrada atual, adiciona ao buffer
        buffer.append(entrada_atual.copy())  # Usa cópia para garantir que a entrada não seja sobrescrita
    entrada_atual.clear()  # Limpa para a nova entrada
    entrada_atual['id'] = int(linha[4:])  # Captura o ID corretamente
    entrada_atual['asin'] = None
    entrada_atual['title'] = None
    entrada_atual['group_name'] = None
    entrada_atual['salesrank'] = None
    entrada_atual['similar'] = []
    entrada_atual['categories'] = []
    entrada_atual['reviews'] = []
    entrada_atual['discontinued'] = None
def processar_asin(linha, entrada_atual, _):
    entrada_atual['asin'] = linha[6:]
def processar_title(linha, entrada_atual, _):
    entrada_atual['title'] = linha[7:]
def processar_group(linha, entrada_atual, _):
    entrada_atual['group_name'] = linha[7:]
def processar_salesrank(linha, entrada_atual, _):
    entrada_atual['salesrank'] = int(linha[11:])
def processar_similar(linha, entrada_atual, _):
    entrada_atual['similar'] = linha.split()[2:]
def processar_categories(linha, entrada_atual, _):
    entrada_atual['categories'] = []
def processar_category_line(linha, entrada_atual, _):
    if 'categories' in entrada_atual:
        entrada_atual['categories'].append(linha)
def processar_reviews(linha, entrada_atual, _):
    partes = linha.split()
    try:
        entrada_atual['review_summary'] = {
            'total': int(partes[2]),
            'downloaded': int(partes[4]),
            'avg_rating': float(partes[7])
        }
    except (IndexError, ValueError) as e:
        print(f"Erro ao processar sumário de reviews: {linha}. Erro: {e}")
# Não faz nada somente é uma versão de teste
def processar_review_line2(linha, entrada_atual, _):
    if 'reviews' in entrada_atual:
        try:
            partes = linha.split()
            review = {
                'date': partes[0],  # Data da review
                'cutomer': None,
                'rating': None,
                'votes': None,
                'helpful': None
            }

            for i, parte in enumerate(partes):
                if 'cutomer:' in parte:
                    review['cutomer'] = partes[i + 1]  # O valor após "customer:"
                elif 'rating:' in parte:
                    review['rating'] = int(partes[i + 1])  # O valor após "rating:"
                elif 'votes:' in parte:
                    review['votes'] = int(partes[i + 1])  # O valor após "votes:"
                elif 'helpful:' in parte:
                    review['helpful'] = int(partes[i + 1])  # O valor após "helpful:"

            entrada_atual['reviews'].append(review)  # Adiciona a review à lista

        except (IndexError, ValueError) as e:
            print(f"Erro ao processar linha de review: {linha}. Erro: {e}")
def processar_review_line(linha, entrada_atual, _):
    if 'reviews' in entrada_atual:
        try:
            # Remove espaços extras
            partes = linha.split()
            review = {
                'date': partes[0],  # Data da review
                'cutomer': None,
                'rating': None,
                'votes': None,
                'helpful': None
            }

            # Itera pelas partes processadas, agora sem espaços em branco extras
            for i, parte in enumerate(partes):
                if 'cutomer:' in parte:
                    review['cutomer'] = partes[i + 1].strip()  # Remove espaços do valor
                elif 'rating:' in parte:
                    review['rating'] = int(partes[i + 1].strip())  # Remove espaços do valor
                elif 'votes:' in parte:
                    review['votes'] = int(partes[i + 1].strip())  # Remove espaços do valor
                elif 'helpful:' in parte:
                    review['helpful'] = int(partes[i + 1].strip())  # Remove espaços do valor

            entrada_atual['reviews'].append(review)  # Adiciona a review à lista

        except (IndexError, ValueError) as e:
            print(f"Erro ao processar linha de review: {linha}. Erro: {e}")
def processar_descontinuado(linha, entrada_atual, _):
    entrada_atual['discontinued'] = True

# Essa função processa o arquivo.
# A cada buffer (que pode ser alterado no código) envia para a função de inserir no banco
# Da maneira q está o buffer tem tamanho de 60000, ou seja, manda 60000 produtos para serem inseridos.
# Essa função tem uma parte assincrona. Enquanto a função de inserção está rodando, ela continua juntando mais 60000 para a proxima.
# Entretanto, ela so manda para função de inserção quando acaba a inserção no banco. (devido aos problemas do sqlite)
def processar_arquivo(arquivo):

    # pode alterar para ter um melhor desempenho
    q_buffer = 60000

    buffer = []
    entrada_atual = {}
    inserindo = False  # Flag para controlar se está ocorrendo inserção
    lock = threading.Lock()  # Lock para proteger o buffer
    condition = threading.Condition(lock)

    # Função para inserir no banco
    def inserir_no_banco_thread(buffer_copy):
        nonlocal inserindo
        inserir_no_banco_assincrono(buffer_copy)
        with lock:
            inserindo = False  # Libera para nova inserção
            condition.notify_all()  # Notifica que a inserção terminou

    # Mapeamento dos prefixos para funções de processamento
    prefix_map = {
        "Id:": processar_id,
        "ASIN:": processar_asin,
        "title:": processar_title,
        "group:": processar_group,
        "salesrank:": processar_salesrank,
        "similar:": processar_similar,
        "categories:": processar_categories,
        "|": processar_category_line,
        "reviews:": processar_reviews,
        "discontinued product": processar_descontinuado
    }

    with open(arquivo, 'r', encoding='utf-8') as f:
        for linha in f:
            linha = linha.strip()

            date_pattern = r'\d{4}-(0?[1-9]|1[0-2])-(0?[1-9]|[12][0-9]|3[01])'
            date_match = re.search(date_pattern, linha)

            for prefix, func in prefix_map.items():
                if date_match:
                    processar_review_line(linha, entrada_atual, buffer)
                    break
                if linha.startswith(prefix):
                    func(linha, entrada_atual, buffer)
                    break  # Sai do loop assim que encontrar o prefixo

            # Verifica se o buffer atingiu o limite para inserção
            if len(buffer) >= q_buffer:
                with condition:
                    while inserindo:
                        condition.wait()  # Espera a inserção anterior terminar
                    inserindo = True  # Marca que está inserindo
                    buffer_copy = buffer.copy()
                    buffer = []  # Reseta o buffer para novos dados
                    thread = threading.Thread(target=inserir_no_banco_thread, args=(buffer_copy,))
                    thread.start()

    # Insere o último buffer se sobrar algo
    if entrada_atual:
        buffer.append(entrada_atual.copy())
    if buffer:
        with condition:
            while inserindo:
                condition.wait()  # Espera a inserção anterior terminar
            inserir_no_banco_assincrono(buffer)

def main():
    # Cria o banco de dados e tabelas, se não existirem
    criar_banco_de_dados()

    # Processa o arquivo de entrada
    arquivo = 'amazon-meta.txt'  # Exemplo de arquivo, substitua pelo caminho do arquivo correto
    processar_arquivo(arquivo)
    print("Banco de Dados Pronto")

# Verifica se este arquivo está sendo executado diretamente
if __name__ == '__main__':
    main()
