import pandas as pd
import os


def verifica_igualdade(row):
    if row['hash_origem'] == row['hash_destino']:
        return 'Igual'
    else:
        return 'Diferente'
    
def checar_md5(nome_arquivo_md5_origem:str, nome_arquivo_md5_destino:str):
    """ Essa função analisa se um arquivo de saida do comando:
        --> md5sum *.fastq.gz > arquivo_saida.txt
    dentro de uma pasta, salvando em arquivo_saida.txt é igual
    
    Args:
        nome_arquivo_md5_origem (str): arquivo dos dados de origem contendo o hash e o nome do arquivo.
        nome_arquivo_md5_destino (str): arquivo dos dados de destino contendo o hash e o nome do arquivo.
    """
    print('================================')
    print('********** Checar MD5 **********')
    print('================================\n')
    #print('Linhas com problemas do arquivo de origem:\n')
    
    df_origem = pd.read_csv(nome_arquivo_md5_origem, sep=' ',names=['hash_origem', 'kk','nome_arquivo'], on_bad_lines='warn')
    df_origem.drop(columns='kk', inplace=True)
    print(f'Quantidade de linhas do arquivo origem --> {len(df_origem)}\n')
    #print(df_origem)
    #print('Linhas com problemas do arquivo de destino:\n')

    df_destino = pd.read_csv(nome_arquivo_md5_destino, sep=' ',names=['hash_destino', 'kk','nome_arquivo'],on_bad_lines='warn')
    df_destino.drop(columns='kk', inplace=True)
    #print(df_destino)

    print(f'Quantidade de linhas do arquivo destino --> {len(df_destino)}\n')


    df = pd.merge(df_origem, df_destino, on='nome_arquivo', how ='right')
    print(f'Quantidade de linhas dos dataframes acoplados --> {len(df)}\n')

    df = df[['nome_arquivo', 'hash_origem', 'hash_destino']]
    df['status'] = df.apply(verifica_igualdade, axis =1)
    #print(df)

    df_dif = df[df['status']=='Diferente']
    #print(df[df['status']=='Diferente'])
    #df_igual = df[df['status']=='Igual']
    #print(df[df['status']=='Diferente'])
    print(f'\nQuantidade de arquivos com Hash diferentes --> {len(df_dif)}')
    df.to_csv('comparacao_md5.csv', index=False)
    
    
    df_igual = df[df['status']=='Igual']
    #print(df[df['status']=='Igual'])
    df_igual['nome'] = df['nome_arquivo'].apply(lambda x: x.split('.')[0])
    print(df_igual['nome'].unique())
    #print(df_igual)

    nome_arquivos = [nome.split('.')[0] for nome in df_igual['nome_arquivo']]
    #print(nome_arquivos)

#nome_arquivo_md5_origem = input('Entre com o caminho do arquivo de origem: ')
#nome_arquivo_md5_destino = input('Entre com o caminho do arquivo de destino: ')


checar_md5('/home/mirian/Documentos/Mirian/CPOM/Ariane/output-files/servidor_concat.md5', '/home/mirian/Documentos/Mirian/CPOM/Ariane/baixar_servidor_pc.md5')
