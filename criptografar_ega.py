#Importações
import pandas as pd
import os
import shutil
import sys
import re
import subprocess
import glob

#Funções
def entrar_no_diretorio(diretorio):
    """Essa função entra no diretório indicado

    Args:
        diretorio (str): 
    """
    try:
        os.chdir(diretorio)
        print(f"Diretório alterado para {diretorio}. \n")
    except FileNotFoundError:
        print(f"O diretório {diretorio} não foi encontrado.\n")
    except NotADirectoryError:
        print(f"{diretorio} não é um diretório.\n")
    except PermissionError:
        print(f"Permissão negada para entrar no diretório {diretorio}.\n")
    except Exception as e:
        print(f"Erro ao entrar no diretório: {e}\n")    
        

def tamanho_pasta(caminho):
    total_size = 0
    for dirpath, dirnames, filenames in os.walk(caminho):
        for f in filenames:
            fp = os.path.join(dirpath, f)
            # Ignorar se for um link simbólico
            if not os.path.islink(fp):
                total_size += os.path.getsize(fp)
                
    tamanho = converter_tamanho(total_size)
                
    print(f"O tamanho da pasta '{caminho}' é {tamanho}.\n")
    return total_size

def converter_tamanho(tamanho_bytes):
    for x in ['bytes', 'KB', 'MB', 'GB', 'TB']:
        if tamanho_bytes < 1024.0:
            return f"{tamanho_bytes:3.1f} {x}"
        tamanho_bytes /= 1024.0

def espaco_livre_em_disco(caminho='/'):
    total, usado, livre = shutil.disk_usage(caminho)
    livre_GB =  converter_tamanho(livre)
    print(f"Espaço livre em disco: {livre_GB}")
    return livre

def criar_arquivo_com_mensagem(nome_arquivo:str, mensagem:str):
    try:
        with open(nome_arquivo, 'w') as arquivo:
            arquivo.write(mensagem)
        print(f"Arquivo '{nome_arquivo}' criado com sucesso.\n")
    except Exception as e:
        print(f"Erro ao criar o arquivo: {e}\n")


def criar_pasta(caminho):
    try:
        os.makedirs(caminho, exist_ok=True)
        print(f"Pasta '{caminho}' criada com sucesso.\n")
    except Exception as e:
        print(f"Erro ao criar a pasta: {e}\n")

def listar_arquivos_diretorio(caminho):
    try:
        arquivos = [arquivo for arquivo in os.listdir(caminho) if os.path.isfile(os.path.join(caminho, arquivo))]
        return arquivos
    except Exception as e:
        print(f"Erro ao listar arquivos do diretório: {e}\n")
        return arquivos
    
def extrair_nomes_arquivos(lista_arquivos):
    padrao = r'^(.+?)(?:-.*|_.*)_?(R[12])_'
    nomes_extraídos = []

    for arquivo in lista_arquivos:
        correspondencia = re.match(padrao, arquivo)
        if correspondencia:
            nome = correspondencia.group(1)
            r1_r2 = correspondencia.group(2)
            nome_completo = f"{nome}_{r1_r2}"
            nomes_extraídos.append(nome_completo)

    return nomes_extraídos

def encontrar_duplicados(lista_nomes):
    nomes_unicos = set()
    duplicados = []

    for nome in lista_nomes:
        if nome in nomes_unicos:
            duplicados.append(nome)
        else:
            nomes_unicos.add(nome)

    return duplicados, nomes_unicos

def concatenar_fastqs_duplicados(duplicados):
    nomes = []
    for nome_dup in duplicados:
        nome = nome_dup.split('_')[0]
        r = nome_dup.split('_')[1].split('.')[0]
        nome2 = nome.lower()
        # Construindo o comando
        comando = f"cat {nome}*{r}*.fastq.gz > {nome2}_{r}_join.fastq.gz"

        # Executando o comando usando subprocess
        try:
            subprocess.run(comando, shell=True, check=True)
            print(f"Arquivos concatenados com sucesso: {nome2}_{r}_join.fastq.gz. \n")
            nomes.append(f'{nome2}_{r}_join')
        except subprocess.CalledProcessError as e:
            print(f"Erro ao concatenar arquivos para {nome}_{r}: {e}")
    return nomes    

def criptografar(lista_arquivos:list):
    for arquivo in lista_arquivos:
        nome = arquivo.split('_')[0]
        nome2 = nome.lower()
        r = arquivo.split('_')[1].split('.')[0]
        padrao = f"{nome}*{r}*.fastq.gz"
        file = glob.glob(padrao)[0]
        print(file)
       
        comando = f'crypt4gh encrypt --recipient_pk ingestion.pubkey < {file} > "crypted/{nome2}_{r}_crypted.c4gh"'
        
        try:
            subprocess.run(comando, shell=True, check=True)
            print(f"Arquivos criptografado com sucesso: {nome}_{r}_crypted.c4gh. \n")
        except:
            print(f'Erro ao criptografar o arquivo {arquivo}.\n')

#Entrar na pasta
diretorio = input('Digite o caminho completo para o diretório onde se encontram os arquivos FASTQs que serão criptografados:')
entrar_no_diretorio(diretorio)

#Analisar se tem  tamnho disponível em disco
espaco_pasta = tamanho_pasta(diretorio)
espaco_livre = espaco_livre_em_disco()


#if espaco_livre > 2*espaco_pasta:
#    print('Tem espaço suficiente para a concatenação dos arquivos e encriptografa-los.\n')
#    status = 0
#elif tamanho_pasta < espaco_livre_em_disco < 2*tamanho_pasta:
#    print('Espaço em disco suficiente somente para encriptografar os arquivos.\n')
#    status = 1
    
#elif espaco_livre_em_disco<tamanho_pasta:
#    print('Não tem espaço em disco para a operação, finalizando o programa.\n')
#    sys.exit()
status = 0
#Criar arquivo ingestion para a pasta selecinada
mensagem = """
-----BEGIN CRYPT4GH PUBLIC KEY-----
SUtKgXbC5tBCzM69wvGvFl5qY5OR/+20s5ZyNSebRFw=
-----END CRYPT4GH PUBLIC KEY-----
"""
criar_arquivo_com_mensagem('ingestion.pubkey', mensagem)


#Criar pasta para colocar os arquivos criptografados
criar_pasta('crypted')

#Analisar quais fastqs serão concatenados e concatenar
nome_arquivos= listar_arquivos_diretorio('.')
#print(nome_arquivos)
nomes=extrair_nomes_arquivos(nome_arquivos)
#print(nomes)
#print('\n')

if status ==0:
    duplicados, unicos = encontrar_duplicados(nomes)
    #print(duplicados)
    #print(unicos)
    #Concatenar fastqs duplicados:
    arquivos_concatenados = concatenar_fastqs_duplicados(duplicados)
    teste=[item for item in list(unicos) if item not in duplicados]
    #print(teste)
    arquivos_prontos_criptografar = teste + arquivos_concatenados
    #print(arquivos_prontos_criptografar)
elif status ==1:
    duplicados, arquivos_prontos_criptografar = encontrar_duplicados(nomes)
    
print(arquivos_prontos_criptografar)
    

#criptografar e slavar na pasta crypted, segundo o padrao de arquivo nomeAmostra_R1ou2_crypted.c4gh
criptografar(arquivos_prontos_criptografar)

print('Processo finalizado.')

