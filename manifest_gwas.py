### COLOCAR INDICE NO EXCEL DO GWAS_PRONON
#PEGANDO INFO DOS ARQUIVOS E SALVANDO EM UM DATAFRAME

import os
import pandas as pd
import xml.etree.ElementTree as ET
import shutil

def move_files(source_dir, target_dir, filename:str):
    """
    Move arquivos listados em um arquivo de um diretório para outro.

    Args:
    - file_list_path: Caminho para o arquivo contendo a lista de nomes de arquivos.
    - source_dir: Diretório de origem.
    - target_dir: Diretório de destino.
    """
    
    # Certificando-se de que os diretórios de origem e destino existem
    if not os.path.exists(source_dir):
        print(f"O diretório de origem '{source_dir}' não existe!")
        return
    
    if not os.path.exists(target_dir):
        os.makedirs(target_dir)  # Cria o diretório de destino se ele não existir

    # Abrindo o arquivo com a lista de arquivos
    source_path = os.path.join(source_dir, filename)
    target_path = os.path.join(target_dir, filename)

    # Verificando se o arquivo existe no diretório de origem
    if os.path.exists(source_path):
        shutil.copy2(source_path, target_path)
        print(f"Arquivo '{filename}' copiado com sucesso!")
    else:
        print(f"Arquivo '{filename}' não encontrado no diretório de origem!")
        
def get_all_folders(directory_path):
    """
    Retorna uma lista contendo os nomes de todos os diretórios no caminho especificado.

    Args:
    - directory_path: Caminho do diretório do qual você deseja obter os nomes das pastas.
    
    Returns:
    - Lista contendo os nomes dos diretórios.
    """
    
    # Listar todos os itens no diretório especificado
    all_items = os.listdir(directory_path)
    
    # Filtrar apenas diretórios
    folders = [item for item in all_items if os.path.isdir(os.path.join(directory_path, item))]
    
    return folders


def extract_info_from_xml(file_path):
    tree = ET.parse(file_path)
    root = tree.getroot()

    # Extraindo 'Sample ID'
    sample_id_node = root.find(".//UserAttribute[@Name='Sample ID']/UserAttributeValue")
    if sample_id_node is not None:
        sample_id_full = sample_id_node.text
        sample_id = "_".join(sample_id_full.split('_')[:-1])  # Pega tudo menos a última parte
        a01 = sample_id_full.split('_')[-1]  # Pega a última parte
    else:
        sample_id_node = root.find(".//UserAttribute[@Name='Source 96-Plate Barcode']/UserAttributeValue")
        if sample_id_node is not None:
            sample_id_full = sample_id_node.text
            sample_id = "_".join(sample_id_full.split('_')[:-1])  # Pega tudo menos a última parte
            a01 = sample_id_full.split('_')[-1]  # Pega a última parte
        else:
            sample_id = None
            a01 = None

def extract_info_from_xml2(file_path):
    tree = ET.parse(file_path)
    root = tree.getroot()

    # Extraindo 'Sample ID'
    sample_id_node = root.find(".//UserAttribute[@Name='Source 96-Plate Barcode']/UserAttributeValue")
    sample_id_full = sample_id_node.text
    #print(sample_id_full)
    #sample_id = "_".join(sample_id_full.split('_')[:-1])  # Pega tudo menos a última parte
    #print(sample_id)
    
    # Extraindo 'Array Barcode'
    array_barcode_node = root.find(".//UserAttribute[@Name='Array Barcode']/UserAttributeValue")
    array_barcode = array_barcode_node.text
    #print(array_barcode)
    #array_barcode = array_barcode_node.text.split('-')[-1].split('_')[0]

    a01 = array_barcode.split('_')[-1]  # Pega a última parte
    #print(a01)
    
    return {"Filename": os.path.basename(file_path), "Sample ID": sample_id_full, "A01": a01, "Array Barcode": array_barcode}

def extract_info_from_xml3(file_path):
    tree = ET.parse(file_path)
    root = tree.getroot()
    try:
        # Extraindo 'Sample ID'
        sample_id_node = root.find(".//UserAttribute[@Name='Source 96-Plate Barcode']/UserAttributeValue")
        sample_id_full = sample_id_node.text
        print(sample_id_full)
        #sample_id = "_".join(sample_id_full.split('_')[:-1])  # Pega tudo menos a última parte
        #print(sample_id)
    except:
        sample_id_full = 'None'
    # Extraindo 'Array Barcode'
    array_barcode_node = root.find(".//UserAttribute[@Name='Array Barcode']/UserAttributeValue")
    array_barcode = array_barcode_node.text
    print(array_barcode)
    #array_barcode = array_barcode_node.text.split('-')[-1].split('_')[0]

    # Extraindo 'Array Barcode'
    a01_node = root.find(".//UserAttribute[@Name='Well Location']/UserAttributeValue")
    a01 = a01_node.text
    #a01 = array_barcode.split('_')[-1]  # Pega a última parte
    print(a01)
    
    return {"Filename": os.path.basename(file_path), "Sample ID": sample_id_full, "A01": a01, "Array Barcode": array_barcode}


# Caminho para o diretório principal que contém as pastas dos resultados
print('Criando a tabela com Filename, SampleID, Codigo e Barcode')
directory_path = '/home/mirian/Documentos/Mirian/Utils/raw_data/'

# Iterar sobre todas as pastas e arquivos XML
data = []

for foldername in os.listdir(directory_path):
    folder_path = os.path.join(directory_path, foldername)
    #print(folder_path)
    if os.path.isdir(folder_path):
        for filename in os.listdir(folder_path):
            if filename.endswith('.ARR'):  # Assumindo que seus arquivos tenham extensão .xml
                file_path = os.path.join(folder_path, filename)
                data.append(extract_info_from_xml3(file_path))
#print(data)
# Convertendo os dados coletados para um DataFrame pandas
df = pd.DataFrame(data)
print(df)
# Verificando o resultado
#print(df)

df.to_csv(f'tabela_de_para.csv', index=False)

lote3 = pd.read_csv('/home/mirian/Documentos/Mirian/Utils/manifest_lote1.csv', sep=';')
#print(lote3)

#de_para = pd.read_csv('tabela_de_para.csv')
#print(df)
df.rename(columns={'Sample ID': 'PlateName', 'A01': 'Well', 'Array Barcode':'Array_Barcode'}, inplace=True)
#print(df)


df_final = pd.merge(lote3, df, on= ['PlateName', 'Well'])
df_final['SampleName']=df_final['SampleName'].astype(int)
df_final['SampleName']=df_final['SampleName'].astype(str)
df_final.rename(columns={'Tipo amostra': 'ID'}, inplace=True)
df_final.to_csv('correlacao_arquivos_lote1.csv', index=False)
print(df_final)

#Pego listas códigos:
lista_codigos =df_final['Array_Barcode'].unique()
caminho_pastas_df_final = '/media/mirian/Expansion/PRONON_GWAS/amostras_lote3/raw_data'

pastas_lote2 = get_all_folders(caminho_pastas_df_final)

#Criando as pastas de destino
tipos_cancer = df_final['ID'].unique()
for nome in tipos_cancer:
    caminho_pasta_destino = os.path.join(caminho_pastas_df_final, nome)
    # Verifica se o diretório já existe. Se não existir, cria.
    if not os.path.exists(caminho_pasta_destino):
        os.makedirs(caminho_pasta_destino)
        print(f"Pasta {caminho_pasta_destino} criada!")
    else:
        print(f"Pasta {caminho_pasta_destino} já existe!")

for codigo in lista_codigos:
    df_codigo = df_final[(df_final['Array_Barcode'] == codigo)]
    print(codigo)
    #Vejo cancers que aparecem nesse código
    lista_cancer = df_codigo['ID'].unique().tolist()
    print(lista_cancer)

    for cancer in lista_cancer:
        #Crio pastas para cada tipo de cancer na lista
        caminho_pasta_destino = os.path.join(caminho_pastas_df_final, cancer)
        # Verifica se o diretório já existe. Se não existir, cria.
        if not os.path.exists(caminho_pasta_destino):
            os.makedirs(caminho_pasta_destino)
            print(f"Pasta {caminho_pasta_destino} criada!")
        else:
            print(f"Pasta {caminho_pasta_destino} já existe!")

        #Filtro o df pelo tipo de cancer
        ids_codigo_cancer = df_codigo[df_codigo['ID']==cancer]['Well'].to_list()
        
        for pasta in pastas_lote2:
            try:
                codigo_pasta = int(pasta.split('-')[-1])
                if codigo_pasta == codigo:
                    caminho_origem = os.path.join(caminho_pastas_df_final, pasta)                       

                    
                    for id in ids_codigo_cancer:
                        nome = f'{pasta}_{id}.CEL'
                        move_files(caminho_origem, caminho_pasta_destino, nome)

                    #Pegar nome de cada arquivo que está no código:
                    
                    move_files(caminho_origem, caminho_pasta_destino, nome)
            
            except:
                continue
