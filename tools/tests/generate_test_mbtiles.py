import sqlite3
import os

def create_valid_test_mbtiles(file_name="L1.mbtiles"):
    """
    Cria um arquivo MBTiles (SQLite) válido, porém com mapa vazio, 
    apenas para que o App SkyFPL consiga validar o cabeçalho e os metadados.
    """
    if os.path.exists(file_name):
        os.remove(file_name)
    
    # Cria o banco de dados SQLite real
    conn = sqlite3.connect(file_name)
    cursor = conn.cursor()
    
    # Tabela obrigatória de Metadados
    cursor.execute('CREATE TABLE metadata (name text, value text)')
    
    # Inserindo dados de teste que o App vai ler
    test_metadata = [
        ('name', 'SkyNav Test Chart (Robô Fix)'),
        ('type', 'baselayer'),
        ('version', '1.1'),
        ('description', 'Carta de teste gerada para validar o ambiente SkyFPL Native'),
        ('format', 'pbf'),
        ('bounds', '-180,-85,180,85')
    ]
    cursor.executemany('INSERT INTO metadata VALUES (?,?)', test_metadata)
    
    # Tabela obrigatória de Tiles (mesmo que vazia)
    cursor.execute('CREATE TABLE tiles (zoom_level integer, tile_column integer, tile_row integer, tile_data blob)')
    
    conn.commit()
    conn.close()
    
    print(f"✅ SUCESSO! O arquivo '{file_name}' foi gerado como um SQLITE REAL.")
    print("Agora você pode subir este arquivo no Cloudflare R2 para ver o App funcionando!")

if __name__ == '__main__':
    create_valid_test_mbtiles()
