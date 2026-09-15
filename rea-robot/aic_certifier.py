#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
🛰️ SkyFPL / SkyNav Pro — AIC Document Certifier & Ground Truth Extractor
Módulo de Inteligência para extração, validação e certificação geodésica de fixos REA/REH
a partir de Publicações Oficiais do DECEA (Circulares de Informação Aeronáutica - AIC).

Doutrina de Certificação:
1. Quando a carta REA/REH não possui tabela marginal de coordenadas (ex: Ribeirão Preto, Bacia de Santos),
   o WFS do GeoServer DECEA frequentemente contém polígonos aproximados com discrepâncias de centenas de metros.
2. Este módulo atua como Document Certifier (Ground Truth Oficial), consultando as AICs específicas publicadas
   pelo DECEA que regulamentam o voo visual na CTR/TMA em questão.
3. Se houver discrepância geodésica (> 20 metros), a coordenada da publicação oficial (AIC) SEMPRE prevalece
   sobre o GeoServer WFS.
"""

import os
import re
import json
import urllib.request
import fitz  # PyMuPDF

def dms_to_dec(deg: float, minutes: float, sec: float = 0.0, hemisphere: str = 'S') -> float:
    """Converte graus, minutos e segundos geodésicos para graus decimais WGS-84."""
    val = deg + minutes / 60.0 + sec / 3600.0
    return -val if hemisphere.upper() in ['S', 'W'] else val

# Catálogo Canônico de Publicações Oficiais DECEA por Terminal VFR
AIC_OFFICIAL_REGISTRY = {
    'RIBEIRÃO PRETO': {
        'aic_id': 'AIC N 13/20',
        'title': 'Rotas Especiais de Aeronaves em Voo Visual na Zona de Controle (CTR) de Ribeirão Preto',
        'url': 'https://publicacoes.decea.mil.br/publicacao/aic-n-1320',
        'slug': 'aic-n-1320',
        'effective_date': '2020-04-23'
    },
    'XQ-RIBEIRÃO PRETO': {
        'aic_id': 'AIC N 13/20',
        'title': 'Rotas Especiais de Aeronaves em Voo Visual na Zona de Controle (CTR) de Ribeirão Preto',
        'url': 'https://publicacoes.decea.mil.br/publicacao/aic-n-1320',
        'slug': 'aic-n-1320',
        'effective_date': '2020-04-23'
    },
    'BELO HORIZONTE': {
        'aic_id': 'AIC N 20/24',
        'title': 'Rotas Especiais de Aeronaves e Helicópteros em Voo Visual nas Zonas de Controle de Belo Horizonte',
        'url': 'https://publicacoes.decea.mil.br/publicacao/aic-n-2024',
        'slug': 'aic-n-2024',
        'effective_date': '2024-05-16'
    },
    'WH-BELO HORIZONTE': {
        'aic_id': 'AIC N 20/24',
        'title': 'Rotas Especiais de Aeronaves e Helicópteros em Voo Visual nas Zonas de Controle de Belo Horizonte',
        'url': 'https://publicacoes.decea.mil.br/publicacao/aic-n-2024',
        'slug': 'aic-n-2024',
        'effective_date': '2024-05-16'
    },
    'CURITIBA': {
        'aic_id': 'AIC N 22/21',
        'title': 'Rotas Especiais de Aeronaves em Voo Visual na Área de Controle Terminal de Curitiba (TMA-WT)',
        'url': 'https://publicacoes.decea.mil.br/publicacao/aic-n-2221',
        'slug': 'aic-n-2221',
        'effective_date': '2021-12-02'
    },
    'WT-CURITIBA': {
        'aic_id': 'AIC N 22/21',
        'title': 'Rotas Especiais de Aeronaves em Voo Visual na Área de Controle Terminal de Curitiba (TMA-WT)',
        'url': 'https://publicacoes.decea.mil.br/publicacao/aic-n-2221',
        'slug': 'aic-n-2221',
        'effective_date': '2021-12-02'
    },
    'FLORIANÓPOLIS': {
        'aic_id': 'AIC N 22/24',
        'title': 'Circulação Visual na Terminal Florianópolis (TMA-XF)',
        'url': 'https://publicacoes.decea.mil.br/publicacao/aic-n-2224',
        'slug': 'aic-n-2224',
        'effective_date': '2024-10-03'
    },
    'XF-FLORIANÓPOLIS': {
        'aic_id': 'AIC N 22/24',
        'title': 'Circulação Visual na Terminal Florianópolis (TMA-XF)',
        'url': 'https://publicacoes.decea.mil.br/publicacao/aic-n-2224',
        'slug': 'aic-n-2224',
        'effective_date': '2024-10-03'
    },
    'SÃO LUÍS': {
        'aic_id': 'AIC N 18/25',
        'title': 'Rotas Especiais de Aeronaves em Voo Visual na Terminal São Luís (TMA-WS)',
        'url': 'https://publicacoes.decea.mil.br/publicacao/aic-n-1825',
        'slug': 'aic-n-1825',
        'effective_date': '2025-06-12'
    },
    'WS-SÃO LUÍS': {
        'aic_id': 'AIC N 18/25',
        'title': 'Rotas Especiais de Aeronaves em Voo Visual na Terminal São Luís (TMA-WS)',
        'url': 'https://publicacoes.decea.mil.br/publicacao/aic-n-1825',
        'slug': 'aic-n-1825',
        'effective_date': '2025-06-12'
    },
    'NATAL': {
        'aic_id': 'AIC N 18/21',
        'title': 'Rotas Especiais de Aeronaves em Voo Visual na Terminal Natal (TMA-XT)',
        'url': 'https://publicacoes.decea.mil.br/publicacao/aic-n-1821',
        'slug': 'aic-n-1821',
        'effective_date': '2021-08-12'
    },
    'XT-NATAL': {
        'aic_id': 'AIC N 18/21',
        'title': 'Rotas Especiais de Aeronaves em Voo Visual na Terminal Natal (TMA-XT)',
        'url': 'https://publicacoes.decea.mil.br/publicacao/aic-n-1821',
        'slug': 'aic-n-1821',
        'effective_date': '2021-08-12'
    },
    'SALVADOR': {
        'aic_id': 'AIC N 06/22',
        'title': 'Rotas Especiais de Aeronaves em Voo Visual na Terminal Salvador (TMA-XS)',
        'url': 'https://publicacoes.decea.mil.br/publicacao/aic-n-0622',
        'slug': 'aic-n-0622',
        'effective_date': '2022-03-24'
    },
    'XS-SALVADOR': {
        'aic_id': 'AIC N 06/22',
        'title': 'Rotas Especiais de Aeronaves em Voo Visual na Terminal Salvador (TMA-XS)',
        'url': 'https://publicacoes.decea.mil.br/publicacao/aic-n-0622',
        'slug': 'aic-n-0622',
        'effective_date': '2022-03-24'
    },
    'SÃO PAULO': {
        'aic_id': 'AIC N 32/25',
        'title': 'Rotas Especiais de Aeronaves em Voo Visual na Terminal São Paulo (TMA-XP)',
        'url': 'https://publicacoes.decea.mil.br/publicacao/aic-n-3225',
        'slug': 'aic-n-3225',
        'effective_date': '2025-08-07'
    },
    'XP1-SÃO PAULO': {
        'aic_id': 'AIC N 32/25',
        'title': 'Rotas Especiais de Aeronaves em Voo Visual na Terminal São Paulo (TMA-XP)',
        'url': 'https://publicacoes.decea.mil.br/publicacao/aic-n-3225',
        'slug': 'aic-n-3225',
        'effective_date': '2025-08-07'
    },
    'VITÓRIA': {
        'aic_id': 'AIC N 09/22',
        'title': 'Rotas Especiais em Voo Visual na Terminal Vitória (TMA-XR)',
        'url': 'https://publicacoes.decea.mil.br/publicacao/aic-n-0922',
        'slug': 'aic-n-0922',
        'effective_date': '2022-05-19'
    },
    'XR-VITÓRIA': {
        'aic_id': 'AIC N 09/22',
        'title': 'Rotas Especiais em Voo Visual na Terminal Vitória (TMA-XR)',
        'url': 'https://publicacoes.decea.mil.br/publicacao/aic-n-0922',
        'slug': 'aic-n-0922',
        'effective_date': '2022-05-19'
    }
}

# Tabela Canônica Homologada de Coordenadas Oficiais extraídas das AICs
# Serve como Ground Truth Absoluto e Offline Cache certificado
CERTIFIED_AIC_FIXES = {
    'RIBEIRÃO PRETO': {
        'BRODOWSKI': {
            'lat': -20.991389,
            'lng': -47.658611,
            'dms': '20º59\'29" S / 047º39\'31" W',
            'source': 'AIC N 13/20',
            'ceiling': '4500 ft',
            'floor': '3700 ft',
            'mandatory_alt': None,
            'magnetic_heading': '210',
            'remarks': 'Portão de entrada compulsório para aeronaves procedentes dos setores N, NE e E. Rumo 210° para PIRIPAU.'
        },
        'PIRIPAU': {
            'lat': -21.128611,
            'lng': -47.679722,
            'dms': '21º07\'43" S / 047º40\'47" W',
            'source': 'AIC N 13/20',
            'ceiling': '3700 ft',
            'floor': '3700 ft',
            'mandatory_alt': '3700 ft',
            'magnetic_heading': None,
            'remarks': 'Posição de espera/confluência compulsória a 3700 ft AMSL na vertical antes do contato com TWR-RP (118.00 MHz).'
        },
        'SERRANA': {
            'lat': -21.211389,
            'lng': -47.595556,
            'dms': '21º12\'41" S / 047º35\'44" W',
            'source': 'AIC N 13/20',
            'ceiling': '4500 ft',
            'floor': '3700 ft',
            'mandatory_alt': None,
            'magnetic_heading': '338',
            'remarks': 'Portão de entrada compulsório para aeronaves procedentes dos setores S, SE e E. Rumo 338° para PIRIPAU.'
        },
        'DUMONT': {
            'lat': -21.236389,
            'lng': -47.973333,
            'dms': '21º14\'11" S / 047º58\'24" W',
            'source': 'AIC N 13/20',
            'ceiling': '4500 ft',
            'floor': '3500 ft',
            'mandatory_alt': None,
            'magnetic_heading': '075',
            'remarks': 'Portão de entrada compulsório para aeronaves procedentes dos setores S, SW e W. Rumo 075° para USP.'
        },
        'USP': {
            'lat': -21.156944,
            'lng': -47.854167,
            'dms': '21º09\'25" S / 047º51\'15" W',
            'source': 'AIC N 13/20',
            'ceiling': '3500 ft',
            'floor': '3500 ft',
            'mandatory_alt': '3500 ft',
            'magnetic_heading': None,
            'remarks': 'Posição de confluência compulsória a 3500 ft AMSL na vertical antes do contato com TWR-RP (118.00 MHz).'
        },
        'PONTAL': {
            'lat': -21.022500,
            'lng': -48.037222,
            'dms': '21º01\'21" S / 048º02\'14" W',
            'source': 'AIC N 13/20',
            'ceiling': '4500 ft',
            'floor': '3500 ft',
            'mandatory_alt': None,
            'magnetic_heading': '149',
            'remarks': 'Portão de entrada compulsório para aeronaves procedentes dos setores N, NW e W. Rumo 149° para USP.'
        }
    },
    'BELO HORIZONTE': {
        'OLHOS': {
            'lat': -19.648667,
            'lng': -43.910000,
            'dms': '19º38\'55" S / 043º54\'36" W',
            'source': 'CCV REH WH BELO HORIZONTE',
            'frequency': '122.550 MHz',
            'remarks': '[REH] Portao Oficial DECEA',
            'type': 'REH'
        },
        'MANNESMANN': {
            'REA': {
                'lat': -19.978000,
                'lng': -44.008000,
                'dms': '19º58\'41" S / 044º00\'29" W',
                'source': 'AIC N 20/24 (AISWEB CCV WH)',
                'frequency': '120.200 MHz',
                'ceiling': '5000 ft',
                'floor': '3500 ft',
                'remarks': '[REA] Portão Oficial DECEA (Aviões)'
            },
            'REH': {
                'lat': -19.964500,
                'lng': -44.002000,
                'dms': '19º57\'52" S / 044º00\'07" W',
                'source': 'CCV REH WH BELO HORIZONTE',
                'frequency': '122.550 MHz',
                'remarks': '[REH] Portao Oficial DECEA (Helicópteros)'
            }
        },
        'BRANCA': {
            'lat': -20.094500,
            'lng': -44.081667,
            'dms': '20º05\'40" S / 044º04\'54" W',
            'source': 'AIC N 20/24 (AISWEB CCV WH)',
            'ceiling': '5500 ft',
            'floor': '3800 ft',
            'remarks': '[REA] Posição Visual Exclusiva REA'
        },
        'CHAPÉU': {
            'lat': -20.119500,
            'lng': -43.922500,
            'dms': '20º07\'10" S / 043º55\'21" W',
            'source': 'AIC N 20/24 (AISWEB CCV WH)',
            'ceiling': '6500 ft',
            'floor': '4500 ft',
            'remarks': '[REA] Posição Visual Exclusiva REA'
        },
        'CHAPEU': {
            'lat': -20.119500,
            'lng': -43.922500,
            'dms': '20º07\'10" S / 043º55\'21" W',
            'source': 'AIC N 20/24 (AISWEB CCV WH)',
            'ceiling': '6500 ft',
            'floor': '4500 ft',
            'remarks': '[REA] Posição Visual Exclusiva REA'
        },
        'ANDIROBA': {
            'lat': -19.650000,
            'lng': -44.230333,
            'dms': '19º39\'00" S / 044º13\'49" W',
            'source': 'AIC N 20/24 (AISWEB CCV WH)',
            'ceiling': '5500 ft',
            'floor': '4000 ft',
            'remarks': '[REA] Portão Oficial Exclusivo REA'
        },
        'CIRRUS': {
            'lat': -19.428667,
            'lng': -43.899167,
            'dms': '19°25\'43" S / 043°53\'57" W',
            'source': 'AIC N 20/24 (AISWEB CCV WH)',
            'ceiling': '5500 ft',
            'floor': '3800 ft',
            'remarks': '[REA] Portão Oficial Exclusivo REA'
        },
        'LAGOA': {
            'lat': -19.639167,
            'lng': -43.893500,
            'dms': '19º38\'21" S / 043º53\'37" W',
            'source': 'CCV REH WH BELO HORIZONTE',
            'remarks': '[REA] [REH] Ponto VFR DECEA'
        },
        'MARAVILHAS': {
            'lat': -19.513056,
            'lng': -44.678333,
            'dms': '19º30\'47" S / 044º40\'42" W',
            'source': 'AIC N 20/24 (AISWEB CCV WH)',
            'frequency': '122.550 MHz',
            'ceiling': '6000 ft',
            'floor': '4000 ft',
            'mandatory_alt': None,
            'magnetic_heading': None,
            'remarks': '[REA] Portão Oficial Exclusivo REA (Corredor REA MIKE)'
        }
    },
    'CURITIBA': {
        'TAMANDARÉ': {
            'REA': {
                'lat': -25.320667,
                'lng': -49.299333,
                'dms': '25º19\'14" S / 049º17\'58" W',
                'source': 'AIC N 22/21 (DECEA Oficial)',
                'frequency': '120.350 MHz',
                'ceiling': '5500 ft',
                'floor': '3500 ft',
                'remarks': '[REA] Portão Oficial DECEA (Híbrido REA/REH)'
            },
            'REH': {
                'lat': -25.320667,
                'lng': -49.299333,
                'dms': '25º19\'14" S / 049º17\'58" W',
                'source': 'CCV REH WT CURITIBA',
                'frequency': '122.550 MHz',
                'ceiling': '4500 ft',
                'floor': '3500 ft',
                'remarks': '[REH] Portao Oficial DECEA (Híbrido REA/REH)'
            }
        },
        'TAMANDARE': {
            'REA': {
                'lat': -25.320667,
                'lng': -49.299333,
                'dms': '25º19\'14" S / 049º17\'58" W',
                'source': 'AIC N 22/21 (DECEA Oficial)',
                'frequency': '120.350 MHz',
                'ceiling': '5500 ft',
                'floor': '3500 ft',
                'remarks': '[REA] Portão Oficial DECEA (Híbrido REA/REH)'
            },
            'REH': {
                'lat': -25.320667,
                'lng': -49.299333,
                'dms': '25º19\'14" S / 049º17\'58" W',
                'source': 'CCV REH WT CURITIBA',
                'frequency': '122.550 MHz',
                'ceiling': '4500 ft',
                'floor': '3500 ft',
                'remarks': '[REH] Portao Oficial DECEA (Híbrido REA/REH)'
            }
        },
        'COLOMBO': {
            'REA': {
                'lat': -25.292167,
                'lng': -49.222833,
                'dms': '25º17\'32" S / 049º13\'22" W',
                'source': 'AIC N 22/21 (DECEA Oficial)',
                'frequency': '120.350 MHz',
                'ceiling': '5500 ft',
                'floor': '3500 ft',
                'remarks': '[REA] Portão Oficial DECEA (Híbrido REA/REH)'
            },
            'REH': {
                'lat': -25.292167,
                'lng': -49.222833,
                'dms': '25º17\'32" S / 049º13\'22" W',
                'source': 'CCV REH WT CURITIBA',
                'frequency': '122.550 MHz',
                'ceiling': '4500 ft',
                'floor': '3500 ft',
                'remarks': '[REH] Portao Oficial DECEA (Híbrido REA/REH)'
            }
        },
        'BARIGUI': {
            'REA': {
                'lat': -25.428500,
                'lng': -49.312667,
                'dms': '25º25\'43" S / 049º18\'46" W',
                'source': 'AIC N 22/21 (DECEA Oficial)',
                'frequency': '120.350 MHz',
                'ceiling': '5500 ft',
                'floor': '3500 ft',
                'remarks': '[REA] Portão Oficial DECEA (Híbrido REA/REH)'
            },
            'REH': {
                'lat': -25.428500,
                'lng': -49.312667,
                'dms': '25º25\'43" S / 049º18\'46" W',
                'source': 'CCV REH WT CURITIBA',
                'frequency': '129.200 MHz',
                'ceiling': '4500 ft',
                'floor': '3500 ft',
                'remarks': '[REH] Posicao Oficial DECEA (Híbrido REA/REH)'
            }
        },
        'SANEPAR': {
            'REA': {
                'lat': -25.563333,
                'lng': -49.245500,
                'dms': '25º33\'48" S / 049º14\'44" W',
                'source': 'AIC N 22/21 (DECEA Oficial)',
                'frequency': '120.350 MHz',
                'ceiling': '5500 ft',
                'floor': '3500 ft',
                'remarks': '[REA] Portão Oficial DECEA (Híbrido REA/REH)'
            },
            'REH': {
                'lat': -25.563333,
                'lng': -49.245500,
                'dms': '25º33\'48" S / 049º14\'44" W',
                'source': 'CCV REH WT CURITIBA',
                'frequency': '129.200 MHz',
                'ceiling': '4500 ft',
                'floor': '3500 ft',
                'remarks': '[REH] Posicao Oficial DECEA (Híbrido REA/REH)'
            }
        },
        'ATUBA': {
            'REA': {
                'lat': -25.385979,
                'lng': -49.203369,
                'dms': '25º23\'10" S / 049º12\'12" W',
                'source': 'AIC N 22/21 (DECEA Oficial)',
                'frequency': '120.350 MHz',
                'ceiling': '5500 ft',
                'floor': '3500 ft',
                'remarks': '[REA] Portão Oficial DECEA (Híbrido REA/REH)'
            },
            'REH': {
                'lat': -25.388667,
                'lng': -49.205500,
                'dms': '25º23\'19" S / 049º12\'20" W',
                'source': 'CCV REH WT CURITIBA',
                'frequency': '122.550 MHz',
                'ceiling': '4500 ft',
                'floor': '3500 ft',
                'remarks': '[REH] Portao Oficial DECEA (Híbrido REA/REH)'
            }
        }
    },
    'FLORIANÓPOLIS': {
        'ITAJAÍ-AÇU': {
            'lat': -27.113167,
            'lng': -49.517167,
            'dms': '27º06\'47" S / 049º31\'01" W',
            'source': 'Carta CCV REA XF Florianópolis (Tabela Oficial AISWEB / AIC N 22/24)',
            'frequency': '122.850 MHz',
            'ceiling': '2500 ft',
            'floor': '0900 ft',
            'mandatory_alt': None,
            'magnetic_heading': '108',
            'remarks': 'Portão Oficial DECEA no Rio Itajaí-Açu no través de Ibirama'
        },
        'ITAJAI-ACU': {
            'lat': -27.113167,
            'lng': -49.517167,
            'dms': '27º06\'47" S / 049º31\'01" W',
            'source': 'Carta CCV REA XF Florianópolis (Tabela Oficial AISWEB / AIC N 22/24)',
            'frequency': '122.850 MHz',
            'ceiling': '2500 ft',
            'floor': '0900 ft',
            'mandatory_alt': None,
            'magnetic_heading': '108',
            'remarks': 'Portão Oficial DECEA no Rio Itajaí-Açu no través de Ibirama'
        }
    },
    'XF-FLORIANÓPOLIS': {
        'ITAJAÍ-AÇU': {
            'lat': -27.113167,
            'lng': -49.517167,
            'dms': '27º06\'47" S / 049º31\'01" W',
            'source': 'Carta CCV REA XF Florianópolis (Tabela Oficial AISWEB / AIC N 22/24)',
            'frequency': '122.850 MHz',
            'ceiling': '2500 ft',
            'floor': '0900 ft',
            'mandatory_alt': None,
            'magnetic_heading': '108',
            'remarks': 'Portão Oficial DECEA no Rio Itajaí-Açu no través de Ibirama'
        },
        'ITAJAI-ACU': {
            'lat': -27.113167,
            'lng': -49.517167,
            'dms': '27º06\'47" S / 049º31\'01" W',
            'source': 'Carta CCV REA XF Florianópolis (Tabela Oficial AISWEB / AIC N 22/24)',
            'frequency': '122.850 MHz',
            'ceiling': '2500 ft',
            'floor': '0900 ft',
            'mandatory_alt': None,
            'magnetic_heading': '108',
            'remarks': 'Portão Oficial DECEA no Rio Itajaí-Açu no través de Ibirama'
        }
    },
    'SÃO LUÍS': {
        'ILHA DO CAJUAL': {
            'lat': -2.441333,
            'lng': -44.467500,
            'dms': '02º26\'29" S / 044º28\'03" W',
            'source': 'Carta CCV REA WS São Luís (Tabela Oficial AISWEB / AIC N 18/25)',
            'frequency': 'APP-SL 119.45 MHz',
            'ceiling': '1500 ft',
            'floor': '1000 ft',
            'mandatory_alt': None,
            'magnetic_heading': '150',
            'remarks': 'Portão Oficial DECEA Ilha do Cajual (Corredor Echo)'
        },
        'CAJUAL': {
            'lat': -2.441333,
            'lng': -44.467500,
            'dms': '02º26\'29" S / 044º28\'03" W',
            'source': 'Carta CCV REA WS São Luís (Tabela Oficial AISWEB / AIC N 18/25)',
            'frequency': 'APP-SL 119.45 MHz',
            'ceiling': '1500 ft',
            'floor': '1000 ft',
            'mandatory_alt': None,
            'magnetic_heading': '150',
            'remarks': 'Portão Oficial DECEA Ilha do Cajual (Corredor Echo)'
        }
    },
    'WS-SÃO LUÍS': {
        'ILHA DO CAJUAL': {
            'lat': -2.441333,
            'lng': -44.467500,
            'dms': '02º26\'29" S / 044º28\'03" W',
            'source': 'Carta CCV REA WS São Luís (Tabela Oficial AISWEB / AIC N 18/25)',
            'frequency': 'APP-SL 119.45 MHz',
            'ceiling': '1500 ft',
            'floor': '1000 ft',
            'mandatory_alt': None,
            'magnetic_heading': '150',
            'remarks': 'Portão Oficial DECEA Ilha do Cajual (Corredor Echo)'
        },
        'CAJUAL': {
            'lat': -2.441333,
            'lng': -44.467500,
            'dms': '02º26\'29" S / 044º28\'03" W',
            'source': 'Carta CCV REA WS São Luís (Tabela Oficial AISWEB / AIC N 18/25)',
            'frequency': 'APP-SL 119.45 MHz',
            'ceiling': '1500 ft',
            'floor': '1000 ft',
            'mandatory_alt': None,
            'magnetic_heading': '150',
            'remarks': 'Portão Oficial DECEA Ilha do Cajual (Corredor Echo)'
        }
    },
    'NATAL': {
        'POTENGI': {
            'lat': -5.805979,
            'lng': -35.263415,
            'dms': '05º48\'22" S / 035º15\'48" W',
            'source': 'Carta CCV REA XT Natal (Voo Visual / Corredor Bravo)',
            'frequency': 'APP NATAL 1 119,30 / 120,65 MHZ APP NATAL 2 119,65 / 120,65 MHZ',
            'ceiling': '2000 ft',
            'floor': '1100 ft',
            'mandatory_alt': None,
            'magnetic_heading': '75',
            'remarks': 'Portão Oficial DECEA Potengi (Corredor Bravo)'
        },
        'PONTEGI': {
            'lat': -5.805979,
            'lng': -35.263415,
            'dms': '05º48\'22" S / 035º15\'48" W',
            'source': 'Carta CCV REA XT Natal (Voo Visual / Corredor Bravo)',
            'frequency': 'APP NATAL 1 119,30 / 120,65 MHZ APP NATAL 2 119,65 / 120,65 MHZ',
            'ceiling': '2000 ft',
            'floor': '1100 ft',
            'mandatory_alt': None,
            'magnetic_heading': '75',
            'remarks': 'Portão Oficial DECEA Potengi (Corredor Bravo)'
        }
    },
    'XT-NATAL': {
        'POTENGI': {
            'lat': -5.805979,
            'lng': -35.263415,
            'dms': '05º48\'22" S / 035º15\'48" W',
            'source': 'Carta CCV REA XT Natal (Voo Visual / Corredor Bravo)',
            'frequency': 'APP NATAL 1 119,30 / 120,65 MHZ APP NATAL 2 119,65 / 120,65 MHZ',
            'ceiling': '2000 ft',
            'floor': '1100 ft',
            'mandatory_alt': None,
            'magnetic_heading': '75',
            'remarks': 'Portão Oficial DECEA Potengi (Corredor Bravo)'
        },
        'PONTEGI': {
            'lat': -5.805979,
            'lng': -35.263415,
            'dms': '05º48\'22" S / 035º15\'48" W',
            'source': 'Carta CCV REA XT Natal (Voo Visual / Corredor Bravo)',
            'frequency': 'APP NATAL 1 119,30 / 120,65 MHZ APP NATAL 2 119,65 / 120,65 MHZ',
            'ceiling': '2000 ft',
            'floor': '1100 ft',
            'mandatory_alt': None,
            'magnetic_heading': '75',
            'remarks': 'Portão Oficial DECEA Potengi (Corredor Bravo)'
        }
    },
    'SALVADOR': {
        'PRAIA DO FORTE': {
            'lat': -12.5656,
            'lng': -38.0200,
            'dms': '12º33\'56" S / 038º01\'12" W',
            'source': 'Carta CCV REA XS Salvador (Posição Vetorial Canônica Rodovia BA-099)',
            'frequency': 'APP SALVADOR 119,80 / 120,80 / 119,35 / 129,45 MHZ',
            'ceiling': '1500 ft',
            'floor': '1200 ft',
            'mandatory_alt': None,
            'magnetic_heading': '245',
            'remarks': 'Portão de Notificação Compulsório VFR sobre a Rodovia BA-099 (Linha Verde)'
        },
        'FORTE': {
            'lat': -12.5656,
            'lng': -38.0200,
            'dms': '12º33\'56" S / 038º01\'12" W',
            'source': 'Carta CCV REA XS Salvador (Posição Vetorial Canônica Rodovia BA-099)',
            'frequency': 'APP SALVADOR 119,80 / 120,80 / 119,35 / 129,45 MHZ',
            'ceiling': '1500 ft',
            'floor': '1200 ft',
            'mandatory_alt': None,
            'magnetic_heading': '245',
            'remarks': 'Portão de Notificação Compulsório VFR sobre a Rodovia BA-099 (Linha Verde)'
        }
    },
    'XS-SALVADOR': {
        'PRAIA DO FORTE': {
            'lat': -12.5656,
            'lng': -38.0200,
            'dms': '12º33\'56" S / 038º01\'12" W',
            'source': 'Carta CCV REA XS Salvador (Posição Vetorial Canônica Rodovia BA-099)',
            'frequency': 'APP SALVADOR 119,80 / 120,80 / 119,35 / 129,45 MHZ',
            'ceiling': '1500 ft',
            'floor': '1200 ft',
            'mandatory_alt': None,
            'magnetic_heading': '245',
            'remarks': 'Portão de Notificação Compulsório VFR sobre a Rodovia BA-099 (Linha Verde)'
        },
        'FORTE': {
            'lat': -12.5656,
            'lng': -38.0200,
            'dms': '12º33\'56" S / 038º01\'12" W',
            'source': 'Carta CCV REA XS Salvador (Posição Vetorial Canônica Rodovia BA-099)',
            'frequency': 'APP SALVADOR 119,80 / 120,80 / 119,35 / 129,45 MHZ',
            'ceiling': '1500 ft',
            'floor': '1200 ft',
            'mandatory_alt': None,
            'magnetic_heading': '245',
            'remarks': 'Portão de Notificação Compulsório VFR sobre a Rodovia BA-099 (Linha Verde)'
        }
    },
    'SÃO PAULO': {
        'FURNAS': {
            'lat': -23.670833,
            'lng': -47.106389,
            'dms': '23º40\'15" S / 047º06\'23" W',
            'source': 'Carta CCV REA XP1 São Paulo (AIC N 32/25)',
            'frequency': 'SUL 126.650 MHZ',
            'ceiling': '6000 ft',
            'floor': '4100 ft',
            'mandatory_alt': None,
            'magnetic_heading': '142',
            'remarks': 'Portão Oficial DECEA (TMA São Paulo / REA Aviões)'
        }
    },
    'XP1-SÃO PAULO': {
        'FURNAS': {
            'lat': -23.670833,
            'lng': -47.106389,
            'dms': '23º40\'15" S / 047º06\'23" W',
            'source': 'Carta CCV REA XP1 São Paulo (AIC N 32/25)',
            'frequency': 'SUL 126.650 MHZ',
            'ceiling': '6000 ft',
            'floor': '4100 ft',
            'mandatory_alt': None,
            'magnetic_heading': '142',
            'remarks': 'Portão Oficial DECEA (TMA São Paulo / REA Aviões)'
        }
    },
    'VITÓRIA': {
        'PACOTES': {
            'lat': -20.352000,
            'lng': -40.250500,
            'dms': '20º21\'07" S / 040º15\'02" W',
            'source': 'CCV REH XR VITÓRIA',
            'type': 'REH',
            'frequency': '123.450 MHz',
            'mandatory_alt': '500 ft',
            'magnetic_heading': '223',
            'remarks': '[REH] Portão Oficial Exclusivo de Helicópteros (REH Litoral)'
        },
        'ITAPARICA': {
            'lat': -20.394667,
            'lng': -40.313667,
            'dms': '20º23\'41" S / 040º18\'49" W',
            'source': 'CCV REH XR VITÓRIA',
            'type': 'REH',
            'frequency': '122.550 MHz',
            'mandatory_alt': '500 ft',
            'remarks': '[REH] Portão Oficial Exclusivo de Helicópteros (REH Litoral)'
        },
        'SIVU': {
            'lat': -20.423100,
            'lng': -40.332500,
            'dms': '20º25\'23" S / 040º19\'57" W',
            'source': 'CCV REH XR VITÓRIA',
            'type': 'REH',
            'frequency': '122.550 MHz',
            'mandatory_alt': '1000 ft',
            'remarks': '[REH] Portão Oficial Exclusivo de Helicópteros (REH Litoral)'
        }
    },
    'XR-VITÓRIA': {
        'PACOTES': {
            'lat': -20.352000,
            'lng': -40.250500,
            'dms': '20º21\'07" S / 040º15\'02" W',
            'source': 'CCV REH XR VITÓRIA',
            'type': 'REH',
            'frequency': '123.450 MHz',
            'mandatory_alt': '500 ft',
            'magnetic_heading': '223',
            'remarks': '[REH] Portão Oficial Exclusivo de Helicópteros (REH Litoral)'
        },
        'ITAPARICA': {
            'lat': -20.394667,
            'lng': -40.313667,
            'dms': '20º23\'41" S / 040º18\'49" W',
            'source': 'CCV REH XR VITÓRIA',
            'type': 'REH',
            'frequency': '122.550 MHz',
            'mandatory_alt': '500 ft',
            'remarks': '[REH] Portão Oficial Exclusivo de Helicópteros (REH Litoral)'
        },
        'SIVU': {
            'lat': -20.423100,
            'lng': -40.332500,
            'dms': '20º25\'23" S / 040º19\'57" W',
            'source': 'CCV REH XR VITÓRIA',
            'type': 'REH',
            'frequency': '122.550 MHz',
            'mandatory_alt': '1000 ft',
            'remarks': '[REH] Portão Oficial Exclusivo de Helicópteros (REH Litoral)'
        }
    }
}

def extract_coords_from_aic_text(text: str) -> dict:
    """
    Parser universal de coordenadas de fixos em textos de AICs do DECEA.
    Suporta múltiplos padrões de formatação:
    - Posição BRODOWSKI (20º59'29" S / 047º39'31" W)
    - Fixo BRODOWSKI (20°59'29"S / 047°39'31"W)
    - BRODOWSKI (205929S / 0473931W)
    """
    found_fixes = {}
    
    # Padrão 1: Nome (GGºMM'SS" S / GGGºMM'SS" W)
    pattern1 = re.compile(
        r'(?:Posição|Fixo|Portão)?\s*([A-Za-zÀ-ÿ\s]{2,25}?)\s*\(\s*(\d{1,2})\s*[º°\s]\s*(\d{1,2})\s*[\'’\s]\s*([\d.]+)?\s*["”]?\s*([SN])\s*[\/\-e]\s*(\d{1,3})\s*[º°\s]\s*(\d{1,2})\s*[\'’\s]\s*([\d.]+)?\s*["”]?\s*([WE])\s*\)',
        re.IGNORECASE
    )
    
    for m in pattern1.finditer(text):
        raw_name = m.group(1).strip()
        name = re.sub(r'^(Posição|Fixo|Portão|da|de|do)\s+', '', raw_name, flags=re.IGNORECASE).strip().upper()
        if len(name) < 2 or name in {'ÁREA', 'SETOR', 'ROTA', 'ALTITUDE', 'COORDENADAS'}:
            continue
            
        lat_deg = float(m.group(2))
        lat_min = float(m.group(3))
        lat_sec = float(m.group(4) or 0.0)
        lat_h = m.group(5)
        
        lon_deg = float(m.group(6))
        lon_min = float(m.group(7))
        lon_sec = float(m.group(8) or 0.0)
        lon_h = m.group(9)
        
        lat = dms_to_dec(lat_deg, lat_min, lat_sec, lat_h)
        lng = dms_to_dec(lon_deg, lon_min, lon_sec, lon_h)
        
        found_fixes[name] = {
            'lat': round(lat, 6),
            'lng': round(lng, 6),
            'dms': f"{int(lat_deg)}º{int(lat_min)}'{lat_sec}\" {lat_h} / {int(lon_deg)}º{int(lon_min)}'{lon_sec}\" {lon_h}"
        }
        
    return found_fixes

def download_and_extract_aic(url_or_slug: str) -> dict:
    """
    Baixa dinamicamente o PDF da AIC a partir do portal DECEA e extrai as coordenadas dos fixos.
    """
    try:
        url = url_or_slug if url_or_slug.startswith('http') else f"https://publicacoes.decea.mil.br/publicacao/{url_or_slug}"
        req = urllib.request.Request(url, headers={'User-Agent': 'Mozilla/5.0 (SkyFPL-AIC-Robot)'})
        with urllib.request.urlopen(req, timeout=20) as resp:
            html = resp.read().decode('utf-8', errors='ignore')
            
        # Localizar link para o arquivo PDF na CDN DECEA
        pdf_match = re.search(r'(https://static\.decea\.mil\.br/publicacoes/files/[^"\'\s<>]+)', html)
        if not pdf_match:
            print(f"⚠️ Não foi possível encontrar link PDF em {url}")
            return {}
            
        pdf_url = pdf_match.group(1).replace('&amp;', '&')
        req_pdf = urllib.request.Request(pdf_url, headers={'User-Agent': 'Mozilla/5.0 (SkyFPL-AIC-Robot)'})
        with urllib.request.urlopen(req_pdf, timeout=30) as pdf_resp:
            pdf_bytes = pdf_resp.read()
            
        doc = fitz.open(stream=pdf_bytes, filetype="pdf")
        full_text = ""
        for page in doc:
            full_text += page.get_text() + "\n"
            
        return extract_coords_from_aic_text(full_text)
    except Exception as e:
        print(f"⚠️ Falha ao baixar/extrair AIC ({url_or_slug}): {e}")
        return {}

def certify_fix_coordinates(terminal: str, fix_name: str, cur_lat: float, cur_lng: float, fix_type: str = 'REA') -> tuple:
    """
    Certifica e calibra as coordenadas de um fixo contra o repositório de documentos AIC.
    Retorna: (calibrated_lat, calibrated_lng, was_calibrated, source_doc, delta_meters, extra_props)
    """
    norm_term = (terminal or '').upper()
    norm_name = (fix_name or '').strip().upper()
    norm_type = (fix_type or 'REA').strip().upper()
    
    # 1. Procurar no catálogo de certificados
    for term_key, fixes_map in CERTIFIED_AIC_FIXES.items():
        if term_key in norm_term or norm_term in term_key:
            if norm_name in fixes_map:
                cert = fixes_map[norm_name]
                # Se houver distinção modal (ex: MANNESMANN com subchaves REA e REH)
                if isinstance(cert, dict) and ('REA' in cert or 'REH' in cert):
                    cert = cert.get(norm_type) or cert.get('REA') or cert
                if not isinstance(cert, dict) or 'lat' not in cert:
                    continue

                cert_lat = cert['lat']
                cert_lng = cert['lng']
                
                # Calcular delta em metros usando aproximação geodésica local
                dlat = (cur_lat - cert_lat) * 111139.0
                dlng = (cur_lng - cert_lng) * 111139.0 * 0.93
                delta_m = (dlat**2 + dlng**2)**0.5
                
                extra_props = {
                    'ceiling': cert.get('ceiling'),
                    'floor': cert.get('floor'),
                    'mandatory_alt': cert.get('mandatory_alt'),
                    'magnetic_heading': cert.get('magnetic_heading'),
                    'frequency': cert.get('frequency'),
                    'remarks': cert.get('remarks')
                }
                
                if delta_m > 20.0:  # Mais de 20 metros de discrepância
                    return cert_lat, cert_lng, True, cert['source'], round(delta_m, 1), extra_props
                else:
                    return cert_lat, cert_lng, False, cert['source'], round(delta_m, 1), extra_props
                    
    return cur_lat, cur_lng, False, None, 0.0, {}

if __name__ == '__main__':
    print("🧪 Testando Certificador de AICs...")
    test_lat, test_lng, changed, src, dist, props = certify_fix_coordinates('XQ-RIBEIRÃO PRETO', 'BRODOWSKI', -20.9850, -47.6578)
    print(f"BRODOWSKI: Calibrado={changed} | Coordenada=({test_lat}, {test_lng}) | Origem={src} | Delta={dist}m")
    
    test_lat, test_lng, changed, src, dist, props = certify_fix_coordinates('XQ-RIBEIRÃO PRETO', 'SERRANA', -21.2075, -47.6036)
    print(f"SERRANA: Calibrado={changed} | Coordenada=({test_lat}, {test_lng}) | Origem={src} | Delta={dist}m")
