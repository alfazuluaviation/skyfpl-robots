import sys
import os, requests
from scripts.build_enrc import download_wms_tile
s = requests.Session()
data = download_wms_tile(751, 1115, 11, s, 'ICA:ENRCL2')
print(f'Size: {len(data) if data else 0}')
