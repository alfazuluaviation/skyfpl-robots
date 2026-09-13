import fitz
import re
import math
import requests

def solve_cramer_3x3(points):
    n = len(points)
    if n < 3: return None
    sumX = sum(p['x'] for p in points)
    sumY = sum(p['y'] for p in points)
    sumV = sum(p['v'] for p in points)
    sumXX = sum(p['x']*p['x'] for p in points)
    sumYY = sum(p['y']*p['y'] for p in points)
    sumXY = sum(p['x']*p['y'] for p in points)
    sumXV = sum(p['x']*p['v'] for p in points)
    sumYV = sum(p['y']*p['v'] for p in points)
    
    det = n*(sumXX*sumYY - sumXY*sumXY) - sumX*(sumX*sumYY - sumY*sumXY) + sumY*(sumX*sumXY - sumY*sumXX)
    if abs(det) < 1e-12: return None
    
    a = (sumV*(sumXX*sumYY - sumXY*sumXY) - sumX*(sumXV*sumYY - sumYV*sumXY) + sumY*(sumXV*sumXY - sumYV*sumXX)) / det
    b = (n*(sumXV*sumYY - sumYV*sumXY) - sumV*(sumX*sumYY - sumY*sumXY) + sumY*(sumX*sumYV - sumY*sumXV)) / det
    c = (n*(sumXX*sumYV - sumXY*sumXV) - sumX*(sumX*sumYV - sumY*sumXV) + sumV*(sumX*sumXY - sumY*sumXX)) / det
    return {'a': a, 'b': b, 'c': c}

def solve_affine_4point(pdf_corners, geo_corners):
    lat_points = []
    lng_points = []
    for i in range(4):
        # Allow zero coordinates because 0,0 is a valid geographic coordinate and pdf coordinate
        lat_points.append({'x': pdf_corners[i*2], 'y': pdf_corners[i*2+1], 'v': geo_corners[i*2]})
        lng_points.append({'x': pdf_corners[i*2], 'y': pdf_corners[i*2+1], 'v': geo_corners[i*2+1]})
        
    lat_params = solve_cramer_3x3(lat_points)
    lng_params = solve_cramer_3x3(lng_points)
    if not lat_params or not lng_params: return None
    
    def solver(px, py):
        lat = lat_params['a']*px + lat_params['b']*py + lat_params['c']
        lng = lng_params['a']*px + lng_params['b']*py + lng_params['c']
        return [lat, lng]
    return solver

def from_meters(x, y):
    lng = (x / 20037508.34) * 180.0
    lat = (y / 20037508.34) * 180.0
    lat = (180.0 / math.pi) * (2 * math.atan(math.exp((lat * math.pi) / 180.0)) - math.pi / 2)
    return [lat, lng]

def test():
    # URL de uma carta IAC de Salvador SBSV (Pegar a url do DECEA do seu cache ou public url)
    # Como não tenho a URL exata, vou usar uma simulada ou se não der baixar de algum lugar.
    # Vou fazer um dump de `index_proc_charts.py` mas rodar passando a URL.
    pass

test()
