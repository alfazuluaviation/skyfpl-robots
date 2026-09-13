# 🌍 SkyFPL — WorldMap & BaseMap Processor Robot

Robô oficial para compilação do basemap raster global e geração de MBTiles com cobertura mundial para os modos offline do SkyFPL / SkyNav Pro.

## Scripts Integrados
- `build_world.py`: Compilador de basemap mundial de baixa e média altitude (Zooms 0 a 7).
- `build_basemap.py`: Pipeline de composição raster e emendas geográficas.

## Execução Local
```bash
python build_world.py --min-zoom 0 --max-zoom 7
```
