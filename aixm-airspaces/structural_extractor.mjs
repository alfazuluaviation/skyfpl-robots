import fs from 'fs';
import axios from 'axios';
import JSZip from 'jszip';
import { XMLParser } from 'fast-xml-parser';
import { createClient } from '@supabase/supabase-js';
import { createRequire } from 'module';
const require = createRequire(import.meta.url);


const SUPABASE_URL = process.env.SUPABASE_URL;
const SUPABASE_KEY = process.env.SUPABASE_SERVICE_ROLE_KEY || process.env.SUPABASE_KEY;
const supabase = createClient(SUPABASE_URL, SUPABASE_KEY);

function extractVal(obj) {
    if (obj === null || obj === undefined) return null;
    if (typeof obj === 'number') return obj;
    if (typeof obj === 'string') {
        const parsed = parseInt(obj);
        return isNaN(parsed) ? null : parsed;
    }
    // Caso seja um objeto AIXM com #text ou val
    const val = obj['#text'] ?? obj.val ?? obj.value ?? obj;
    if (val && typeof val === 'object') return null;
    const parsed = parseInt(String(val));
    return isNaN(parsed) ? null : parsed;
}

function findDeep(obj, targetKey) {
    if (!obj || typeof obj !== 'object') return null;
    const entries = Object.entries(obj);
    
    // 1. Tentar encontrar no nível atual (Case Insensitive)
    for (const [key, value] of entries) {
        if (key.toLowerCase() === targetKey.toLowerCase()) return value;
    }
    
    // 2. Se não achou, mergulhar recursivamente
    for (const [key, value] of entries) {
        if (typeof value === 'object') {
            const found = findDeep(value, targetKey);
            if (found !== null) return found;
        }
    }
    return null;
}

function toTacticalCase(str) {
    if (!str) return '';
    
    // Lista de acrônimos que devem permanecer em maiúsculo
    const PROTECTED_ACRONYMS = [
        'APP', 'TMA', 'CTR', 'FIR', 'ATZ', 'CTA', 'AFIL', 'PLN', 'VFR', 'IFR', 
        'RNAV', 'RNP', 'RWY', 'TKOF', 'HEL', 'REA', 'REH', 'NM', 'FT', 'FL', 
        'UTC', 'MSL', 'STD', 'SFC', 'GND', 'AGL', 'VMC', 'SID', 'OMNI', 'TWR', 'ACC'
    ];

    return str.toLowerCase().split(' ').map(word => {
        const upper = word.toUpperCase().replace(/[^A-Z]/g, '');
        if (PROTECTED_ACRONYMS.includes(upper)) return upper;
        
        // Tratar casos com hífen ou barra
        if (word.includes('/') || word.includes('-')) {
            return word.split(/([/-])/).map(part => {
                if (PROTECTED_ACRONYMS.includes(part.toUpperCase())) return part.toUpperCase();
                return part.charAt(0).toUpperCase() + part.slice(1);
            }).join('');
        }

        return word.charAt(0).toUpperCase() + word.slice(1);
    }).join(' ');
}

function cleanRtf(str) {
    if (!str || typeof str !== 'string') return '';
    // Limpar HTML entities (&#13; &#10; etc)
    let clean = str.replace(/&#\d+;/g, ' ');
    if (!clean.includes('\\rtf') && !clean.includes('{\\')) {
        return clean.replace(/\s+/g, ' ').trim();
    }
    // Decodificar caracteres hex RTF (\' seguido de hex)
    clean = clean.replace(/\\'([0-9a-f]{2})/gi, (_, hex) => String.fromCharCode(parseInt(hex, 16)));
    // Estratégia: Extrair texto após \fsNN (marcador de tamanho de fonte = início do conteúdo no RTF DECEA)
    const contentMatch = clean.match(/\\fs\d+\s+([\s\S]+?)(?:\\par|$)/);
    if (contentMatch && contentMatch[1].length > 5) {
        clean = contentMatch[1];
    }
    // Remover control words restantes
    clean = clean.replace(/\\[a-z]+\d*\s?/gi, ' ');
    clean = clean.replace(/[{}\\\*]/g, '').replace(/\s+/g, ' ').trim();
    return clean || '';
}

function findAipMatches(aipFrequencies, areaName, originalType, designator = '') {
    const normalize = (s) => s?.normalize('NFD').replace(/[\u0300-\u036f]/g, '').toUpperCase().replace(/[^A-Z0-9]/g, '') || '';
    const normName = normalize(areaName);
    const normType = normalize(originalType); // TMA, CTR, FIR, CTA, ATZ
    const allKeys = Object.keys(aipFrequencies);
    
    if (normName.length < 3) return [];

    // 1. Tentar match exato com o nome normalizado
    let candidates = allKeys.filter(k => normalize(k) === normName);
    
    // 2. Se falhar, tentar match parcial (contém o nome)
    if (candidates.length === 0) {
        candidates = allKeys.filter(k => normalize(k).includes(normName));
    }

    // 3. Se for um setor (termina com número ou _01), tentar match com a "área mãe"
    if (candidates.length === 0 && (designator.includes('_') || /\d+$/.test(areaName))) {
        // Extrair o nome base (ex: "Belo Horizonte" de "Belo Horizonte 1")
        const baseName = areaName.split(/(_|\d)/)[0].trim();
        const normBase = normalize(baseName);
        if (normBase.length >= 3) {
            candidates = allKeys.filter(k => normalize(k).includes(normBase));
        }
    }

    if (candidates.length === 0) return [];

    // Fase de filtragem por tipo para evitar confusão entre TMA e CTR com mesmo nome
    const sameType = candidates.filter(k => normalize(k).includes(normType));
    if (sameType.length > 0) return sameType;

    return candidates.slice(0, 1);
}

function extractAllNotes(timeSlice) {
    if (!timeSlice.annotation) return [];
    
    // Suportar tanto objeto único quanto array de anotações (comum no DECEA)
    const annotations = Array.isArray(timeSlice.annotation) ? timeSlice.annotation : [timeSlice.annotation];
    const extractedTexts = [];

    for (const ann of annotations) {
        if (!ann.Note?.translatedNote) continue;
        
        const tNotes = Array.isArray(ann.Note.translatedNote) 
            ? ann.Note.translatedNote 
            : [ann.Note.translatedNote];
        
        // Prioridade 1: Nota em Português (POR/PT)
        const porNote = tNotes.find(tn => {
            const lang = tn.LinguisticNote?.note?.['@_lang'] || '';
            return ['POR', 'PT'].includes(lang.toUpperCase());
        });

        if (porNote) {
            const text = porNote.LinguisticNote?.note?.['#text'] || porNote.LinguisticNote?.note || '';
            if (text) extractedTexts.push(text);
        } else {
            // Fallback: Primeira nota disponível se não houver POR
            const first = tNotes[0];
            const text = first?.LinguisticNote?.note?.['#text'] || first?.LinguisticNote?.note || '';
            if (text) extractedTexts.push(text);
        }
    }
    
    return extractedTexts;
}

async function runSync() {
    console.log('🚀 [ROBOT-STRUCTURAL] Iniciando Super Varredura de TMA, CTR, FIR, CTA...');
    
    try {
        console.log('🔍 [ROBOT] Consultando catálogo oficial via Edge Function...');
        const DISCOVERY_URL = `${SUPABASE_URL}/functions/v1/fetch-aisweb-data`;
        const discoveryRes = await axios.post(DISCOVERY_URL, 
            { area: 'pub', type: 'aixm' },
            { headers: { 'Authorization': `Bearer ${SUPABASE_KEY}` } }
        );

        if (!discoveryRes.data?.success) throw new Error('Falha ao descobrir links.');

        const parser = new XMLParser({ ignoreAttributes: false, removeNSPrefix: true });
        const jsonObj = parser.parse(discoveryRes.data.xml);
        const items = jsonObj.aisweb?.pub?.item || [];
        const itemsArray = Array.isArray(items) ? items : [items];
        
        console.log(`🚨 [AUDITORIA] Encontrados ${itemsArray.length} pacotes no catálogo.`);

        const serviceMap = {}; // designator/name -> frequencies[]
        const enrichedData = [];

        // PROCESSAR TODOS OS PACOTES DO CATÁLOGO
        for (let i = 0; i < itemsArray.length; i++) {
            const item = itemsArray[i];
            console.log(`📥 [ROBOT] Processando Pacote [${i}]: ${item.name}...`);
            
            let link = item.link || item.file || '';
            if (typeof link === 'object') link = link['#text'] || '';
            link = link.replace(']]>', '').replace('<![CDATA[', '').split('">')[0].trim();

            const tempPath = `./temp_aixm_${i}.zip`;
            const curlCmd = `curl -L -A "Mozilla/5.0" -o ${tempPath} "${link}"`;
            
            try {
                require('child_process').execSync(curlCmd);
                const zipData = fs.readFileSync(tempPath);
                const zip = await JSZip.loadAsync(zipData);
                fs.unlinkSync(tempPath);

                const xmlFiles = Object.keys(zip.files).filter(f => f.endsWith('.xml'));
                for (const xmlFileName of xmlFiles) {
                    console.log(`   📄 Lendo: ${xmlFileName}`);
                    const xmlText = await zip.files[xmlFileName].async('string');
                    const parser2 = new XMLParser({ 
                        ignoreAttributes: false, 
                        attributeNamePrefix: "@_", 
                        removeNSPrefix: true,
                        alwaysArray: ["hasMember", "timeSlice", "radioCommunicationChannel"]
                    });
                    const jsonObj2 = parser2.parse(xmlText);
                    const members = jsonObj2.AIXMBasicMessage?.hasMember || [];

                    members.forEach(member => {
                        // 1. Busca Literal de Frequências (Scanner de Deep Object)
                        const foundFreqs = [];
                        const deepScan = (obj) => {
                            if (!obj) return;
                            if (typeof obj === 'string' || typeof obj === 'number') {
                                const val = String(obj);
                                if (val.includes('.') || val.includes(',')) {
                                    const num = parseFloat(val.replace(',', '.'));
                                    if (num >= 108.0 && num <= 137.0) foundFreqs.push(`${num.toFixed(3)} MHz`);
                                }
                                return;
                            }
                            if (Array.isArray(obj)) { obj.forEach(deepScan); return; }
                            if (typeof obj === 'object') {
                                for (const k in obj) deepScan(obj[k]);
                            }
                        };
                        deepScan(member);

                        // 2. Indexar Frequências se for Serviço/Unidade
                        const entity = Object.values(member)[0];
                        const tSlices = Array.isArray(entity?.timeSlice) ? entity.timeSlice : (entity?.timeSlice ? [entity.timeSlice] : []);
                        
                        if (foundFreqs.length > 0) {
                            tSlices.forEach(ts => {
                                const data = ts?.ServiceTimeSlice || ts?.UnitTimeSlice || ts?.AirTrafficControlServiceTimeSlice || ts;
                                if (!data) return;
                                [data.designator, data.name].filter(Boolean).forEach(k => {
                                    const nk = k.toString().toUpperCase();
                                    if (!serviceMap[nk]) serviceMap[nk] = [];
                                    serviceMap[nk].push(...foundFreqs);
                                });
                            });
                        }

                        // 3. Coletar Espaços Aéreos
                        const airspace = member.Airspace;
        if (airspace) {
                            const aSlices = Array.isArray(airspace.timeSlice) ? airspace.timeSlice : (airspace.timeSlice ? [airspace.timeSlice] : []);
                            const ts = aSlices.find(s => ['BASELINE', 'PERMANENT', 'SNAPSHOT'].includes(s.AirspaceTimeSlice?.interpretation))?.AirspaceTimeSlice;
                            
                            // Adicionada ATZ à lista de interesse
                            if (ts && ['TMA', 'CTR', 'FIR', 'CTA', 'ATZ'].includes(ts.type)) {
                                const isCtr = ts.type === 'CTR';
                                const ident = isCtr ? `${ts.designator} CTR` : String(ts.designator || '');
                                
                                // Evitar duplicatas entre pacotes (manter o mais recente)
                                if (!enrichedData.find(e => e.ident === ident)) {
                                    // NOVO: Extrair limites de múltiplas camadas (class) e calcular envelope total
                                    const classes = Array.isArray(ts.class) ? ts.class : (ts.class ? [ts.class] : []);
                                    let absoluteMax = -Infinity;
                                    let absoluteMin = Infinity;
                                    let maxUom = 'FL';
                                    let minUom = 'FT';
                                    let maxRef = 'STD';
                                    let minRef = 'MSL';
                                    const layerDetails = [];

                                    if (classes.length > 0) {
                                        classes.forEach(c => {
                                            const lClass = c.AirspaceLayerClass;
                                            const classification = lClass?.classification || 'N/A';
                                            const layer = lClass?.associatedLevels?.AirspaceLayer;
                                            
                                            const up = layer?.upperLimit?.['#text'] ?? layer?.upperLimit?.val ?? layer?.upperLimit;
                                            const lo = layer?.lowerLimit?.['#text'] ?? layer?.lowerLimit?.val ?? layer?.lowerLimit;
                                            const uRef = layer?.upperLimitReference || 'STD';
                                            const lRef = layer?.lowerLimitReference || 'MSL';
                                            const uUom = layer?.upperLimit?.['@_uom'] || (uRef === 'STD' ? 'FL' : 'FT');
                                            const lUom = layer?.lowerLimit?.['@_uom'] || (lRef === 'MSL' ? 'FT' : 'FL');

                                            const upVal = extractVal(up);
                                            const loVal = extractVal(lo);

                                            if (upVal !== null || loVal !== null) {
                                                layerDetails.push(`Classe ${classification}: ${upVal || '?'}${uUom}/${loVal || '?'}${lUom}`);
                                            }

                                            if (upVal !== null && (absoluteMax === -Infinity || upVal > absoluteMax)) {
                                                absoluteMax = upVal;
                                                maxUom = uUom;
                                                maxRef = uRef;
                                            }
                                            if (loVal !== null && (absoluteMin === Infinity || loVal < absoluteMin)) {
                                                absoluteMin = loVal;
                                                minUom = lUom;
                                                minRef = lRef;
                                            }
                                        });
                                    }

                                    // BUSCA PROFUNDA (Deep Search) - A "Bomba Atômica" contra dados sumidos
                                    // Procura em todo o nó da AirspaceTimeSlice por limites (case-insensitive)
                                    const deepUp = findDeep(ts, 'upperLimit');
                                    const deepLo = findDeep(ts, 'lowerLimit');
                                    const deepUpRef = findDeep(ts, 'upperLimitReference');
                                    const deepLoRef = findDeep(ts, 'lowerLimitReference');

                                    if (deepUp !== null) {
                                        const val = extractVal(deepUp);
                                        if (val !== null && (absoluteMax === -Infinity || val > absoluteMax)) {
                                            absoluteMax = val;
                                            maxUom = deepUp['@_uom'] || (deepUpRef === 'STD' ? 'FL' : 'FT');
                                            maxRef = deepUpRef || maxRef;
                                        }
                                    }
                                    if (deepLo !== null) {
                                        const val = extractVal(deepLo);
                                        if (val !== null && (absoluteMin === Infinity || val < absoluteMin)) {
                                            absoluteMin = val;
                                            minUom = deepLo['@_uom'] || (deepLoRef === 'MSL' ? 'FT' : 'FL');
                                            minRef = deepLoRef || minRef;
                                        }
                                    }

                                    // Extrair horário de ativação
                                    let schedule = 'H24';
                                    const timesheet = ts.activation?.AirspaceActivation?.timeInterval?.Timesheet;
                                    if (timesheet) {
                                        if (timesheet.day === 'ANY' && (timesheet.startTime === '00:00' || !timesheet.startTime) && (timesheet.endTime === '00:00' || !timesheet.endTime)) {
                                            schedule = 'H24';
                                        } else {
                                            schedule = `${timesheet.startTime || '00:00'}–${timesheet.endTime || '00:00'} UTC`;
                                        }
                                    }

                                    const auditNotes = [];
                                    if (classes.length > 1) auditNotes.push('MULTIPLAS_CAMADAS');
                                    if (minRef === 'SFC') auditNotes.push('BASE_SFC');
                                    if (absoluteMax === -Infinity || absoluteMin === Infinity) {
                                        auditNotes.push('ALTITUDE_FALTANDO');
                                    }

                                    enrichedData.push({
                                        ident,
                                        nam: ts.name || '',
                                        type: (ts.type === 'CTA' || ts.type === 'FIR') ? 'TMA' : ts.type,
                                        originalType: ts.type,
                                        upperLimit: (absoluteMax === -Infinity || isNaN(absoluteMax)) ? null : absoluteMax,
                                        uom_upper: maxUom,
                                        lowerLimit: (absoluteMin === Infinity || isNaN(absoluteMin)) ? null : absoluteMin,
                                        uom_lower: minUom,
                                        upperRef: maxRef,
                                        lowerRef: minRef,
                                        classification: classes[0]?.AirspaceLayerClass?.classification || '',
                                        horario: schedule,
                                        observacoes: toTacticalCase(cleanRtf(extractAllNotes(ts).join(' / '))) || 'SEM OBSERVAÇÕES',
                                        layerSummary: layerDetails.join(' | '),
                                        auditNotes,
                                        raw: ts
                                    });
                                }
                            }
                        }
                    });
                }
            } catch (err) {
                console.error(`⚠️ Erro ao processar pacote [${i}]: ${err.message}`);
            }
        }

        console.log(`📡 [ROBOT] Varredura finalizada. ${Object.keys(serviceMap).length} órgãos indexados.`);
        console.log(`🌍 [ROBOT] Sincronizando ${enrichedData.length} áreas estruturais...`);

        // NOVO: Ler frequências do Extrator Auxiliar AIP (Arquitetura Híbrida)
        let aipFrequencies = {};
        try {
            if (fs.existsSync('./aip_frequencies.json')) {
                aipFrequencies = JSON.parse(fs.readFileSync('./aip_frequencies.json', 'utf8'));
                console.log(`📚 [ROBOT-AIP] Carregadas frequências estruturais do AIP para ${Object.keys(aipFrequencies).length} órgãos.`);
            }
        } catch (e) {
            console.log('⚠️ [ROBOT-AIP] Aviso: Arquivo aip_frequencies.json não encontrado ou inválido. Usando apenas dados do AIXM.');
        }

        const normalize = (str) => str?.normalize("NFD").replace(/[\u0300-\u036f]/g, "").toUpperCase().replace(/[^A-Z0-9]/g, "") || '';
        const processedIdents = new Set();

        for (const area of enrichedData) {
            processedIdents.add(area.ident);
            const normIdent = normalize(area.ident);
            const normName = normalize(area.nam);
            
            // Busca de Frequências (Match por Nome/Designador ou Prefixo)
            let freqs = serviceMap[area.ident?.toUpperCase()] || [];
            if (freqs.length === 0) {
                const matchingKey = Object.keys(serviceMap).find(k => {
                    const nk = normalize(k);
                    if (nk.includes(normIdent) || normIdent.includes(nk)) return true;
                    if (nk.includes(normName) || normName.includes(nk)) return true;
                    if (normName.length >= 5 && nk.startsWith(normName.substring(0, 5))) return true;
                    return false;
                });
                if (matchingKey) freqs = serviceMap[matchingKey];
            }

            const finalFrequencies = [...new Set(freqs)];

            // Fallback Regex nas Notas (já limpas de RTF)
            if (finalFrequencies.length === 0 && area.observacoes) {
                const freqRegex = /(\d{3}[.,]\d{2,3})/g;
                let m;
                while ((m = freqRegex.exec(area.observacoes)) !== null) {
                    const f = m[1].replace(',', '.');
                    const num = parseFloat(f);
                    if (num >= 108 && num <= 137) finalFrequencies.push(`${num.toFixed(3)} MHz`);
                }
            }

            // Enriquecimento Híbrido: Match Inteligente por Tipo + Agregação Multi-Setor
            const aipMatchKeys = findAipMatches(aipFrequencies, area.nam, area.originalType || area.type, area.ident);

            if (aipMatchKeys.length > 0) {
                const aggregatedFreqs = [];
                let bestHorario = '';
                let bestObs = '';

                aipMatchKeys.forEach(key => {
                    const data = aipFrequencies[key];
                    if (data.frequencias) {
                        aggregatedFreqs.push(...data.frequencias.map(f => f.includes('MHz') ? f : `${f} MHz`));
                    }
                    if (data.horario && !bestHorario) bestHorario = data.horario;
                    if (data.observacoes && !bestObs) bestObs = data.observacoes;
                });

                finalFrequencies.push(...aggregatedFreqs);
                if (bestHorario) area.horario = bestHorario;
                if (bestObs && area.observacoes === 'SEM OBSERVAÇÕES') area.observacoes = bestObs;

                if (aipMatchKeys.length === 1) {
                    console.log(`🔗 [MERGE] ${area.nam} (${area.originalType}) → ${aipMatchKeys[0]} (${[...new Set(aggregatedFreqs)].length} freqs)`);
                } else {
                    console.log(`🔗 [MERGE] ${area.nam} (${area.originalType}) → ${aipMatchKeys.length} setores agregados (${[...new Set(aggregatedFreqs)].length} freqs únicas)`);
                }
            }


            // Sincronizar Supabase
            const { data: existing } = await supabase.from('airspace_snapshots').select('id, raw_properties, status, upperlimit, lowerlimit, classrmklo').eq('ident', area.ident).eq('is_current', true).maybeSingle();

            if (!existing) {
                area.auditNotes.push('DADO_NOVO');
            } else {
                // Detectar alterações estruturais refinadas
                if (area.upperLimit !== null && existing.upperlimit !== area.upperLimit) {
                    area.auditNotes.push('LIMITE_SUPERIOR_ALTERADO');
                }
                if (area.lowerLimit !== null && existing.lowerlimit !== area.lowerLimit) {
                    area.auditNotes.push('LIMITE_INFERIOR_ALTERADO');
                }
                if (area.classification && existing.classrmklo !== area.classification) {
                    area.auditNotes.push('CLASSE_ALTERADA');
                }
                
                // Verificar se houve mudança significativa nas frequências
                const existingFreqs = existing.raw_properties?.aip_data?.frequencias || [];
                const hasNewFreq = finalFrequencies.some(f => !existingFreqs.includes(f));
                const lostFreq = existingFreqs.some(f => !finalFrequencies.includes(f));
                if (hasNewFreq || lostFreq) {
                    area.auditNotes.push('FREQUENCIAS_MODIFICADAS');
                }
            }

            if (finalFrequencies.length === 0) {
                area.auditNotes.push('FREQUENCIA_FALTANDO');
            }
            if (aipMatchKeys.length === 0) {
                area.auditNotes.push('AIP_DESCONHECIDO');
            }

            // Determinar Status de Auditoria
            const reason = [...new Set(area.auditNotes)].join(', ');
            const status = area.auditNotes.length > 0 ? 'PENDING' : 'AUDITED';

            const snapshotData = {
                ident: area.ident,
                nam: area.nam,
                type: area.type,
                upperlimit: area.upperLimit,
                uplimituni: area.uom_upper,
                lowerlimit: area.lowerLimit,
                lowerlimituni: area.uom_lower,
                is_current: true,
                status: (existing?.status === 'VALIDATED') ? 'VALIDATED' : status, // Preservar validação manual
                pending_reason: (existing?.status === 'VALIDATED') ? null : (reason || null),
                raw_properties: {
                    ...(existing?.raw_properties || {}),
                    aerodrome_icao: area.raw?.designator || null,
                    aip_data: {
                        horario: area.horario,
                        classificacao: area.classification || '',
                        upper_ref: area.upperRef || 'STD',
                        lower_ref: area.lowerRef || 'MSL',
                        observacoes: area.observacoes,
                        camadas: area.layerSummary,
                        audit_log: area.auditNotes,
                        frequencias: [...new Set(finalFrequencies)],
                        full_aixm_node: area.raw
                    }
                }
            };

            if (existing) {
                await supabase.from('airspace_snapshots').update(snapshotData).eq('id', existing.id);
            } else {
                await supabase.from('airspace_snapshots').insert(snapshotData);
            }
        }

        // FASE 4: Auditoria de Órfãos (Áreas que sumiram do DECEA)
        console.log('🕵️ [ROBOT-AUDIT] Iniciando busca por áreas removidas pelo DECEA...');
        const { data: currentItems } = await supabase.from('airspace_snapshots')
            .select('id, ident, type, nam')
            .eq('is_current', true);
        
        if (currentItems) {
            for (const item of currentItems) {
                if (!processedIdents.has(item.ident)) {
                    console.log(`⚠️ [ROBOT-ALERT] Área ${item.ident} (${item.nam}) não encontrada no novo ciclo. Marcando para auditoria.`);
                    await supabase.from('airspace_snapshots').update({
                        status: 'PENDING',
                        pending_reason: 'REMOVIDO_PELO_DECEA',
                        is_current: true // Mantemos como true para que o usuário veja a pendência no Dashboard
                    }).eq('id', item.id);
                }
            }
        }

        console.log('✅ [ROBOT] Super Varredura concluída com sucesso!');
    } catch (error) {
        console.error('❌ [ROBOT] Erro fatal:', error.message);
    }
}

runSync();
