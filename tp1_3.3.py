import sqlite3
import pandas as pd


# Função para executar uma consulta SQL e retornar o resultado como um DataFrame
def executar_consulta(query, params=()):
    conn = sqlite3.connect('banco_de_dados.db')
    df = pd.read_sql_query(query, conn, params=params)
    conn.close()
    return df


# 1. Dado um produto, listar os 5 comentários mais úteis e com maior avaliação e os 5 comentários mais úteis e com menor avaliação.
def listar_comentarios_produto(produto_id):
    query = '''
    SELECT * 
    FROM avaliacao
    WHERE produto_id = ?
        AND votos_uteis > 0 -- Filtra para que apenas comentários com votos úteis maiores que zero sejam considerados
    ORDER BY rating DESC, votos_uteis DESC
    LIMIT 5;
    '''
    melhores_comentarios = executar_consulta(query, (produto_id,))

    query = '''
    SELECT * 
    FROM avaliacao
    WHERE produto_id = ?
        AND votos_uteis > 0 -- Filtra para que apenas comentários com votos úteis maiores que zero sejam considerados
    ORDER BY rating ASC, votos_uteis DESC
    LIMIT 5;
    '''
    piores_comentarios = executar_consulta(query, (produto_id,))

    return melhores_comentarios, piores_comentarios


# 2. Dado um produto, listar os produtos similares com maiores vendas do que ele.
def listar_produtos_similares_maiores_vendas(produto_id):
    query = '''
    SELECT ps.similar_asin, p.title, p.salesrank
    FROM produto_similar ps
    JOIN produto p ON ps.similar_asin = p.asin
    WHERE ps.produto_id = ?
    AND p.salesrank < (
        SELECT salesrank FROM produto WHERE id = ?
    )
    ORDER BY p.salesrank ASC;
    '''
    return executar_consulta(query, (produto_id, produto_id))


# 3. Dado um produto, mostrar a evolução diária das médias de avaliação ao longo do tempo.
def evolucao_avaliacao_produto(produto_id):
    query = '''
    SELECT a.data, AVG(a.rating) AS media_avaliacao
    FROM avaliacao a
    WHERE a.produto_id = ?
    GROUP BY a.data
    ORDER BY a.data ASC;
    '''
    return executar_consulta(query, (produto_id,))


# 4. Listar os 10 produtos líderes de venda em cada grupo de produtos.
def listar_produtos_lideres_venda():
    query = '''
    WITH TopProdutos AS (
    SELECT p.id, p.group_name, p.title, p.salesrank,
           ROW_NUMBER() OVER (PARTITION BY p.group_name ORDER BY p.salesrank ASC) AS rank
    FROM produto p
    WHERE p.salesrank IS NOT NULL
    )
    SELECT group_name, title, salesrank
    FROM TopProdutos
    WHERE rank <= 10
    ORDER BY group_name, salesrank ASC;

    '''
    return executar_consulta(query)


# 5. Listar os 10 produtos com a maior média de avaliações úteis positivas por produto.
def listar_produtos_maior_media_avaliacoes_uteis():
    query = '''
    SELECT p.title, AVG(a.votos_uteis) AS media_votos_uteis
    FROM avaliacao a
    JOIN produto p ON a.produto_id = p.id
    GROUP BY p.id
    ORDER BY media_votos_uteis DESC
    LIMIT 10;

    '''
    return executar_consulta(query)


# 6. Listar as 5 categorias de produto com a maior média de avaliações úteis positivas por produto.
def listar_categorias_maior_media_avaliacoes_uteis():
    query = '''
    WITH MediaVotosPorProduto AS (
    SELECT
        p.id AS produto_id,
        AVG(a.votos_uteis) AS media_votos_uteis
    FROM
        avaliacao a
    JOIN
        produto p ON p.id = a.produto_id
    GROUP BY
        p.id
    )
    SELECT
        c.category_name,
        AVG(mvp.media_votos_uteis) AS media_votos_uteis_categoria
    FROM
        MediaVotosPorProduto mvp
    JOIN
        produto_categoria pc ON mvp.produto_id = pc.produto_id
    JOIN
        categoria c ON pc.categoria_id = c.id
    GROUP BY
        c.category_name
    ORDER BY
        media_votos_uteis_categoria DESC
    LIMIT 5;

    '''
    return executar_consulta(query)


# 7. Listar os 10 clientes que mais fizeram comentários por grupo de produto.
def listar_clientes_comentarios_por_grupo():
    query = '''
    SELECT group_name, cliente_id, total_comentarios
    FROM (
    SELECT p.group_name, a.cliente_id, COUNT(a.id) AS total_comentarios,
           ROW_NUMBER() OVER (PARTITION BY p.group_name ORDER BY COUNT(a.id) DESC) AS rank
    FROM avaliacao a
    JOIN produto p ON a.produto_id = p.id
    GROUP BY p.group_name, a.cliente_id
    ) AS ranked
    WHERE rank <= 10
    ORDER BY group_name, rank;
    '''
    return executar_consulta(query)


# Testando as funções
if __name__ == "__main__":
    pd.set_option('display.max_columns', None)

    # Exibir todas as linhas
    pd.set_option('display.max_rows', None)
    produto_id = 28282  # Exemplo de produto_id

    # 1. Listar os 5 melhores e piores comentários de um produto
    melhores, piores = listar_comentarios_produto(produto_id)
    print("Melhores Comentários:")
    print(melhores)
    print("\nPiores Comentários:")
    print(piores)
    print('-------------------------------------------------------------------------------------')

    # 2. Listar produtos similares com maiores vendas
    similares = listar_produtos_similares_maiores_vendas(produto_id)
    print("\nProdutos Similares com Maiores Vendas:")
    print(similares)
    print('-------------------------------------------------------------------------------------')


    # 3. Evolução diária das médias de avaliação
    evolucao = evolucao_avaliacao_produto(produto_id)
    print("\nEvolução Diária das Médias de Avaliação:")
    print(evolucao)
    print('-------------------------------------------------------------------------------------')

    # 4. Listar os 10 produtos líderes de venda em cada grupo
    lideres = listar_produtos_lideres_venda()
    print("\nProdutos Líderes de Venda por Grupo:")
    print(lideres)
    print('-------------------------------------------------------------------------------------')

    # 5. Listar os 10 produtos com a maior média de avaliações úteis
    melhores_produtos = listar_produtos_maior_media_avaliacoes_uteis()
    print("\nProdutos com Maior Média de Avaliações Úteis:")
    print(melhores_produtos)
    print('-------------------------------------------------------------------------------------')

    # 6. Listar as 5 categorias com a maior média de avaliações úteis
    melhores_categorias = listar_categorias_maior_media_avaliacoes_uteis()
    print("\nCategorias com Maior Média de Avaliações Úteis:")
    print(melhores_categorias)
    print('-------------------------------------------------------------------------------------')

    # 7. Listar os 10 clientes que mais fizeram comentários por grupo
    clientes = listar_clientes_comentarios_por_grupo()
    print("\nClientes que Mais Fizeram Comentários por Grupo:")
    print(clientes)
