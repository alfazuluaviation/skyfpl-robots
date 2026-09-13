import os
import boto3
from botocore.exceptions import ClientError

def upload_file_to_r2(file_path, bucket_name, object_name=None):
    """
    Faz o upload do arquivo MBTiles pesado para o Cloudflare R2 com custo de tráfego zero.
    As senhas são puxadas diretamente e com segurança do GitHub Secrets!
    """
    
    # Credenciais Secretas que nós configuramos lá no portal do GitHub
    r2_endpoint = os.environ.get('R2_ENDPOINT')
    r2_access_key_id = os.environ.get('R2_ACCESS_KEY_ID')
    r2_secret_access_key = os.environ.get('R2_SECRET_ACCESS_KEY')
    
    if not all([r2_endpoint, r2_access_key_id, r2_secret_access_key]):
        print("ERRO: As chaves do Cloudflare R2 não foram encontradas no ambiente (Github Secrets).")
        exit(1)

    if object_name is None:
        object_name = os.path.basename(file_path)

    s3_client = boto3.client(
        's3',
        endpoint_url=r2_endpoint,
        aws_access_key_id=r2_access_key_id,
        aws_secret_access_key=r2_secret_access_key,
        region_name='wnam' 
    )

    print(f"Iniciando Mágica: Fazendo upload de {file_path} para o Cofre {bucket_name}/{object_name}...")
    
    try:
        response = s3_client.upload_file(file_path, bucket_name, object_name)
        print("✅ SUCESSO! A Carta (MBTiles) foi hospedada na Nuvem Gratuita do Cloudflare!")
        print(f"O App Mobile já pode baixá-la usando a URL Pública: https://pub-[sua-url-r2].dev/{object_name}")
    except ClientError as e:
        print(f"❌ ERRO CRÍTICO no Upload R2: {e}")
        exit(1)

if __name__ == '__main__':
    import sys
    # Configuração do Bucket R2
    R2_BUCKET = "skyfpl-charts"
    
    # Se passarmos o nome do arquivo via argumento, usamos ele (ex: python upload_to_r2.py SkyFPL_L1.mbtiles)
    if len(sys.argv) > 1:
        arquivo_da_carta = sys.argv[1]
    else:
        # Padrão para compatibilidade com o App atual
        arquivo_da_carta = "SkyFPL_Base.mbtiles"
    
    upload_file_to_r2(arquivo_da_carta, R2_BUCKET)
