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

function toTacticalCase(str) {
    if (!str) return '';
    return str.toLowerCase().split(' ').map(word => word.charAt(0).toUpperCase() + word.slice(1)).join(' ');
}

function cleanRtf(str) {
    if (!str || typeof str !== 'string') return '';
    // Limpar HTML entities primeiro (&#13; &#10; etc)
    let clean = str.replace(/&#\d+;/g, ' ');
    // Se não contém RTF, retorna limpo
    if (!clean.includes('\\rtf') && !clean.includes('{\\')) {
        return clean.replace(/\s+/g, ' ').trim();
    }
    // Decodificar caracteres hex RTF (\' seguido de hex)
    clean = clean.replace(/\\'([0-9a-f]{2})/gi, (_, hex) => String.fromCharCode(parseInt(hex, 16)));
    // Remover blocos de controle aninhados {\fonttbl...}, {\colortbl...}, etc
    let prev = '';
    while (prev !== clean) { prev = clean; clean = clean.replace(/\{[^{}]*\}/g, ''); }
    // Remover palavras de controle restantes (\par, \b0, \fs22, etc)
    clean = clean.replace(/\\[a-z]+\d*\s?/gi, ' ');
    // Remover chaves e espaços múltiplos
    clean = clean.replace(/[{}]/g, '').replace(/\s+/g, ' ').trim();
    return clean || '';
}

function findAipMatches(aipFrequencies, areaName, originalType) {
    const normalize = (s) => s?.normalize('NFD').replace(/[\u0300-\u036f]/g, '').toUpperCase().replace(/[^A-Z0-9]/g, '') || '';
    const normName = normalize(areaName);
    const normType = normalize(originalType); // TMA, CTR, FIR, CTA
    const allKeys = Object.keys(aipFrequencies);
    if (normName.length < 3) return [];

    // Fase 1: Encontrar TODOS os candidatos que contêm o nome
    const candidates = allKeys.filter(k => normalize(k).includes(normName));
    if (candidates.length === 0) return [];

    // Fase 2: Filtrar por mesmo tipo (TMA, FIR, CTR, CTA)
    const sameType = candidates.filter(k => normalize(k).includes(normType));

    // Fase 3: Separar exatos (sem SECT) de setores
    const exact = sameType.filter(k => !normalize(k).includes('SECT'));
    const sectors = sameType.filter(k => normalize(k).includes('SECT'));

    // Prioridade: exato > setores do mesmo tipo > primeiro candidato genérico
    if (exact.length > 0) return exact;
    if (sectors.length > 0) return sectors;
    return candidates.length > 0 ? [candidates[0]] : [];
}

function extractAllNotes(timeSlice) {
    const notes = [];
    if (timeSlice.annotation?.Note?.translatedNote) {
        const tNotes = Array.isArray(timeSlice.annotation.Note.translatedNote) 
            ? timeSlice.annotation.Note.translatedNote 
            : [timeSlice.annotation.Note.translatedNote];
        
        tNotes.forEach(tn => {
            if (tn.LinguisticNote?.note?.['#text']) notes.push(tn.LinguisticNote.note['#text']);
            else if (tn.LinguisticNote?.note) notes.push(tn.LinguisticNote.note);
        });
    }
    return notes;
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
                            if (ts && ['TMA', 'CTR', 'FIR', 'CTA'].includes(ts.type)) {
                                const ident = String(ts.designator || '');
                                // Evitar duplicatas entre pacotes (manter o mais recente)
                                if (!enrichedData.find(e => e.ident === ident)) {
                                    // Extrair limites verticais (múltiplos caminhos possíveis no AIXM 5.1)
                                    const vol = ts.geometryComponent?.AirspaceGeometryComponent?.theAirspaceVolume?.AirspaceVolume;
                                    const layer = ts.class?.AirspaceLayerClass?.associatedLevels?.AirspaceLayer;
                                    const upperObj = vol?.upperLimit || layer?.upperLimit || ts.upperLimit;
                                    const lowerObj = vol?.lowerLimit || layer?.lowerLimit || ts.lowerLimit;

                                    // Extrair classificação do espaço aéreo (A, B, C, D, E, G)
                                    const classification = ts.class?.AirspaceLayerClass?.classification || '';

                                    // Extrair horário de ativação
                                    let schedule = 'H24';
                                    const timesheet = ts.activation?.AirspaceActivation?.timeInterval?.Timesheet;
                                    if (timesheet) {
                                        if (timesheet.day === 'ANY' && timesheet.startTime === '00:00' && timesheet.endTime === '00:00') {
                                            schedule = 'H24';
                                        } else {
                                            schedule = `${timesheet.startTime || '00:00'}–${timesheet.endTime || '00:00'} UTC`;
                                        }
                                    }

                                    // Extrair referência de limite (MSL, STD, SFC)
                                    const upperRef = vol?.upperLimitReference || layer?.upperLimitReference || 'STD';
                                    const lowerRef = vol?.lowerLimitReference || layer?.lowerLimitReference || 'MSL';

                                    enrichedData.push({
                                        ident,
                                        nam: ts.name || '',
                                        type: (ts.type === 'CTA' || ts.type === 'FIR') ? 'TMA' : ts.type,
                                        originalType: ts.type,
                                        upperLimit: upperObj?.['#text'] ?? upperObj?.val ?? upperObj,
                                        uom_upper: upperObj?.['@_uom'] || (upperRef === 'STD' ? 'FL' : 'FT'),
                                        lowerLimit: lowerObj?.['#text'] ?? lowerObj?.val ?? lowerObj,
                                        uom_lower: lowerObj?.['@_uom'] || (lowerRef === 'MSL' ? 'FT' : 'FL'),
                                        upperRef,
                                        lowerRef,
                                        classification,
                                        horario: schedule,
                                        observacoes: toTacticalCase(cleanRtf(extractAllNotes(ts).join(' / '))) || 'SEM OBSERVAÇÕES',
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

        for (const area of enrichedData) {
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
            const aipMatchKeys = findAipMatches(aipFrequencies, area.nam, area.originalType || area.type);

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
            const { data: existing } = await supabase.from('airspace_snapshots').select('id, raw_properties').eq('ident', area.ident).eq('is_current', true).maybeSingle();

            const snapshotData = {
                ident: area.ident,
                nam: area.nam,
                type: area.type,
                upperlimit: parseInt(area.upperLimit) || null,
                uplimituni: area.uom_upper,
                lowerlimit: parseInt(area.lowerLimit) || null,
                lowerlimituni: area.uom_lower,
                is_current: true,
                raw_properties: {
                    ...(existing?.raw_properties || {}),
                    aip_data: {
                        horario: area.horario,
                        classificacao: area.classification || '',
                        upper_ref: area.upperRef || 'STD',
                        lower_ref: area.lowerRef || 'MSL',
                        observacoes: area.observacoes,
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

        console.log('✅ [ROBOT] Super Varredura concluída com sucesso!');
    } catch (error) {
        console.error('❌ [ROBOT] Erro fatal:', error.message);
    }
}

runSync();
