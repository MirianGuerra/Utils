import hashlib
import pandas as pd
import paramiko
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
    df_origem = pd.read_csv(nome_arquivo_md5_origem, sep=' ',names=['hash_origem', 'kk','nome_arquivo'])
    df_origem.drop(columns='kk', inplace=True)

    df_destino = pd.read_csv(nome_arquivo_md5_destino, sep=' ',names=['hash_destino', 'kk','nome_arquivo'])
    df_destino.drop(columns='kk', inplace=True)

    df = pd.merge(df_origem, df_destino, on='nome_arquivo', how ='outer')
    df = df[['nome_arquivo', 'hash_origem', 'hash_destino']]
    df['status'] = df.apply(verifica_igualdade, axis =1)
    print(df[df['status']=='Diferente'])

def baixar_arquivos_servidor_backup_comecando_por_nome(host:str, port:int==22, username:str, password:str, remote_dir:str, local_dir:str, prefix:str):
    try:
        print('Conectando ao servidor...\n')
        transport = paramiko.Transport((host, port))
        transport.connect(username=username, password=password)
        sftp = paramiko.SFTPClient.from_transport(transport)    
        print('Conexão realizada com sucesso!\n')

        # Lista os arquivos no diretório remoto
        remote_files = sftp.listdir(remote_dir)
        #print(remote_files)

        # Cria o diretório local se ele não existir
        os.makedirs(local_dir, exist_ok=True)
        
        print('Baixando arquivos...\n')
        for file_name in remote_files:
            if file_name.startswith(prefix):
                remote_path = os.path.join(remote_dir, file_name)
                local_path = os.path.join(local_dir, file_name)
                
                # Obtém o tamanho do arquivo remoto
                remote_file_size = sftp.stat(remote_path).st_size
                bytes_downloaded = 0
                
                print(f'--> Começando download do arquivo {file_name}\n')
                with open(local_path, 'wb') as local_file:
                    def update_progress(transferred):
                        nonlocal bytes_downloaded
                        bytes_downloaded += transferred
                        progress = (bytes_downloaded / remote_file_size) * 100
                        print(f"\rProgresso: {progress:.2f}%")
                        #sys.stdout.flush()

                sftp.get(remote_path, local_path)


        sftp.close()
        transport.close()

        print("Download concluído com sucesso!")

    except Exception as e:
        print("Ocorreu um erro:", str(e))

if __name__ == "__main__":

    #lista_codigos = ['ZEI896', 'PRD802', 'YZI855', 'JFF940', 'GPR303', 'UMS786', 'BZU207', 'WCI481', 'ZXU595', 'NZT421', 'SPI831', 'PVF414', 'VNW718', 'MTR464', 
    #                 'GLL958', 'NFT967', 'DLM874', 'BTW990', 'DDC480', 'BPP237', 'ZQA321', 'THE976', 'FRM681', 'EWP736', 'AHR341', 'QBE714', 'JSD535', 'VXS318', 
    #                 'DPW776', 'VGE753', 'HRI748', 'TGB088', 'SLD504', 'TIL740', 'NFM165', 'GMS619', 'XTD686', 'ZAV124', 'HWX875', 'XNO647', 'DEJ139', 'CDX215',
    #                 'ICY366', 'CSL111', 'DCU160', 'VNW443', 'HNH905', 'RCP743', 'UOU747', 'KUP794', 'FVG163', 'INT209', 'VRF125', 'ECV940', 'YGR384', 'MSA557',
    #                 'ROY115', 'RZT494', 'ZEW370', 'NVN657', 'GFD767', 'KOO311', 'XIV316', 'WVZ637', 'OER573', 'ZAZ983', 'NOC015', 'TIA573', 'RES693', 'JKF916', 
    #                 'ENL774', 'LGY408', 'UBG444', 'QBN847', 'YXF906', 'UQA857', 'HKX456', 'UZQ758', 'PVV749', 'REE437', 'ZUC659', 'HYQ415', 'QLZ112', 'EQF740', 'HKG813', 'UDJ625',
    #                 'VFM522', 'QPZ258', 'PXI050', 'YDP121', 'RHJ192', 'JHO023', 'FRS087', 'VTM630', 'YLA605', 'IKK308']
    
    #LOTE_006 = ['PRD802', 'YZI855', 'JFF940', 'GPR303', 'UMS786', 'QBE714']
    #LOTE_008 = ['WCI481', 'MTR464', 'BPP237', 'ENL774', 'LGY408']
    #LOTE_009 = ['ZEI896', 'BZU207', 'TIL740', 'INT209', 'OER573']
    #LOTE_011 = ['HKX456', 'HYQ415', 'PVV749', 'QBN847', 'REE437', 'UBG444', 'UQA857', 'UZQ758', 'YXF906', 'ZUC659']
    #LOTE_013 = ['EQF740', 'HKG813', 'KSK272', 'PXI050', 'QLZ112', 'QPZ258', 'UDJ625', 'VFM522', 'ZUA330']
    #LOTE_017 = ['ZXU595', 'NZT421', 'FXT681', 'SPI831', 'PVF414', 'VNW718', 'RHJ192', 'JHO023', 'FRS087', 'VTM630', 'YLA605', 'IKK308','YDP121']
    #LOTE_020 = ['GLL958', 'NFT967', 'DDC480', 'ZQA321', 'FRM681', 'EWP736', 'AHR341', 'JSD535', 'VXS318', 'VGE753', 'HRI748', 'TGB088', 'SLD504', 'NFM165',
    #            'GMS619', 'XTD686', 'HWX875', 'XNO647', 'RWA553', 'ICY366', 'DCU160', 'HNH905', 'UOU747', 'ECV940', 'MSA557', 'ROY115', 'ZEW370', 'GFD767',
    #            'WVZ637', 'ZAZ983', 'TIA573', 'RES693', 'JKF916']
    #LOTE_021 = ['DLM874', 'BTW990', 'THE976', 'DPW776', 'ZAV124', 'DEJ139', 'CDX215', 'OUP996', 'CSL111', 'VNW443', 'RCP743', 'KUP794', 'FVG163', 'VRF125',
    #            'YGR384', 'RZT494', 'NVN657', 'KOO311', 'XIV316', 'NOC015']
    
    #lista_lotes = ['LOTE_006', 'LOTE_008', 'LOTE_009', 'LOTE_011', 'LOTE_013', 'LOTE_017', 'LOTE_020', 'LOTE_021']
    #AD_lote1 = ['AD222', 'AD223', 'AD225', 'AD226','AD228', 'AD229', 'AD230', 'AD231', 'AD232', 'AD234',
    #            'AD235', 'AD236', 'AD237', 'AD238','AD239', 'AD240', 'AD241', 'AD242']

    #AD_lote4 = ['AD272', 'AD273', 'AD274', 'AD275','AD276', 'AD280', 'AD281', 'AD283', 'AD284', 'AD286']
    AD_lote4 = [ 'AD273', 'AD274', 'AD275','AD276', 'AD280', 'AD281', 'AD283', 'AD284', 'AD286']
        
    """
    for ad in AD_lote1:
        host = "172.16.0.37"
        port = 22  # Porta padrão SSH
        username = "root"
        password = "IA*SB#PXhbiG"
        remote_dir = f"/fastq/sophia/somatic/Batch_1_T_N/{ad}/"
        local_dir = "/media/hacpom03/HD_00050747/Leticia/fastq/"
        prefix = 'AD'

        baixar_arquivos_servidor_backup_comecando_por_nome(host, port, username, password, remote_dir, local_dir, prefix)
        """

    for ad in AD_lote4:
        host = "172.16.0.37"
        port = 22  # Porta padrão SSH
        username = "root"
        password = "IA*SB#PXhbiG"
        remote_dir = f"/fastq/sophia/somatic/Batch_4_T_N/{ad}/*"
        local_dir = "/media/hacpom03/HD_00050747/Leticia/fastq/"
        prefix = 'AD'

    baixar_arquivos_servidor_backup_comecando_por_nome(host, port, username, password, remote_dir, local_dir, prefix)
