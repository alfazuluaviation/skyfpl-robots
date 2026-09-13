import re
import json

with open('getcap.xml', 'r', encoding='utf-8') as f:
    xml = f.read()

# Pattern for Layer Name and LatLonBoundingBox
# <Name>WAC_3141_SALVADOR</Name> ... <LatLonBoundingBox minx="-43.00" miny="-16.00" maxx="-37.00" maxy="-12.00"/>
pattern = re.compile(r'<Name>(WAC_(\d+)_.*?)</Name>.*?<LatLonBoundingBox minx="([^"]+)" miny="([^"]+)" maxx="([^"]+)" maxy="([^"]+)"', re.DOTALL)

matches = pattern.findall(xml)
new_bboxes = {}

for full_name, code, minx, miny, maxx, maxy in matches:
    key = f"WAC{code}"
    # Python dict needs floats. We'll round slightly for clean code.
    new_bboxes[key] = (float(minx), float(miny), float(maxx), float(maxy))

print(json.dumps(new_bboxes, indent=4))
