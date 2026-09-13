import sys
sys.path.append('C:/Users/josemir/Desktop/skyfpl-robots_temp/navdata-robot')
import sync_navdata
res = sync_navdata.calculate_airac_cycle()
print("RESULTADO CALCULO AIRAC:", res)
