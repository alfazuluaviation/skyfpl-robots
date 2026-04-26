import fs from 'fs';
import axios from 'axios';
import JSZip from 'jszip';
import { XMLParser } from 'fast-xml-parser';
import { createClient } from '@supabase/supabase-js';

// Configurações via Variáveis de Ambiente (GitHub Secrets)
const SUPABASE_URL = process.env.SUPABASE_URL;
const SUPABASE_KEY = process.env.SUPABASE_SERVICE_ROLE_KEY;
const AIXM_URL = 'https://aisweb.decea.mil.br/download/?public=409c79f8-a70d-409f-a5d62c2f1bd33e00.zip&p=Completo';

if (!SUPABASE_URL || !SUPABASE_KEY) {
    console.error('❌ Erro: SUPABASE_URL e SUPABASE_SERVICE_ROLE_KEY são obrigatórios.');
    process.exit(1);
}

const supabase = createClient(SUPABASE_URL, SUPABASE_KEY);

async function runSync() {
    console.log('🧪 [ROBOT] Iniciando Sincronização AIXM...');
    
    try {
        // 1. Descoberta Dinâmica do Link (Igual ao Dashboard)
        console.log('🔍 [ROBOT] Consultando catálogo oficial via Edge Function...');
        const DISCOVERY_URL = `${SUPABASE_URL}/functions/v1/fetch-aisweb-data`;
        const discoveryRes = await axios.post(DISCOVERY_URL, 
            { area: 'pub', type: 'aixm' },
            { headers: { 'Authorization': `Bearer ${SUPABASE_KEY}` } }
        );

        if (!discoveryRes.data?.success) {
            throw new Error('Falha ao descobrir link: ' + (discoveryRes.data?.error || 'Erro desconhecido'));
        }

        // Parsing Robusto (Ajustado para a estrutura real: aisweb -> pub -> item)
        const parser = new XMLParser({ ignoreAttributes: false, removeNSPrefix: true });
        const jsonObj = parser.parse(discoveryRes.data.xml);
        const items = jsonObj.aisweb?.pub?.item || [];
        const itemsArray = Array.isArray(items) ? items : [items];

        // Busca o item "Completo" ou "Snapshot" usando a tag <name>
        const selectedItem = itemsArray.find(item => {
            const name = String(item.name || '').toLowerCase();
            return name.includes('completo') || name.includes('snapshot') || name.includes('full');
        }) || itemsArray[0];

        let dynamicLink = selectedItem?.link || selectedItem?.file || '';
        if (typeof dynamicLink === 'object') dynamicLink = dynamicLink['#text'] || '';
        
        if (!dynamicLink) {
            console.error('📦 XML recebido:', discoveryRes.data.xml);
            throw new Error('Não foi possível encontrar o link de download no catálogo do DECEA.');
        }

        // Limpeza agressiva: remover CDATA e sufixos estranhos como ">Completo"
        dynamicLink = dynamicLink.replace(']]>', '').replace('<![CDATA[', '');
        dynamicLink = dynamicLink.split('">')[0].trim(); // Remove ">Completo"

        console.log(`🛰️ [ROBOT] Link Autorizado: ${dynamicLink}`);

        // 2. Download via CURL (Ignorando o proxy que bloqueou o GitHub)
        console.log('📦 [ROBOT] Iniciando download direto via CURL...');
        
        const { execSync } = await import('child_process');
        const fs = await import('fs');
        const tempPath = './aixm_temp.zip';

        const curlCmd = `curl -L -A "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/121.0.0.0 Safari/537.36" \
            -H "Referer: https://aisweb.decea.mil.br/?i=download" \
            --connect-timeout 60 \
            --retry 3 \
            -o ${tempPath} "${dynamicLink}"`;

        execSync(curlCmd, { stdio: 'inherit' });

        if (!fs.existsSync(tempPath) || fs.statSync(tempPath).size < 1000000) {
            throw new Error('O arquivo baixado pelo CURL é muito pequeno ou não existe.');
        }

        console.log(`✅ [ROBOT] Banco AIXM recebido (${(fs.statSync(tempPath).size / 1024 / 1024).toFixed(2)} MB).`);
        const zipData = fs.readFileSync(tempPath);
        const zip = await JSZip.loadAsync(zipData);
        
        // Limpeza
        fs.unlinkSync(tempPath);
        
        // 2. Extração do XML
        const xmlFileName = Object.keys(zip.files).find(f => f.endsWith('.xml'));
        if (!xmlFileName) throw new Error('XML não encontrado no ZIP');
        
        console.log(`📄 [ROBOT] Extraindo XML: ${xmlFileName}`);
        const xmlText = await zip.files[xmlFileName].async('text');
        
        // 3. Parsing
        console.log('🔍 [ROBOT] Analisando estrutura XML...');
        const parser2 = new XMLParser({
            ignoreAttributes: false,
            attributeNamePrefix: "@_",
            removeNSPrefix: true
        });
        
        const jsonObj2 = parser2.parse(xmlText);
        const members = jsonObj2.AIXMBasicMessage?.hasMember || [];
        console.log(`📂 [ROBOT] Total de membros encontrados: ${members.length}`);

        const enrichedData = [];
        const typeMap = { 'R': 'RESTRITA', 'P': 'PROIBIDA', 'D': 'PERIGOSA' };

        // Helper DMS
        const toDMS = (lat, lon) => {
            const convert = (val, isLat) => {
                const abs = Math.abs(val);
                const deg = Math.floor(abs);
                const min = Math.floor((abs - deg) * 60);
                const sec = Math.round(((abs - deg) * 60 - min) * 60);
                const dir = val < 0 ? (isLat ? 'S' : 'W') : (isLat ? 'N' : 'E');
                const pad = isLat ? 2 : 3;
                return `${deg.toString().padStart(pad, '0')}${min.toString().padStart(2, '0')}${sec.toString().padStart(2, '0')}${dir}`;
            };
            return `${convert(lat, true)} ${convert(lon, false)}`;
        };

        // 4. Processamento ENR 5.1
        members.forEach(member => {
            const airspace = member.Airspace;
            if (!airspace) return;

            const timeSlices = Array.isArray(airspace.timeSlice) ? airspace.timeSlice : [airspace.timeSlice];
            const timeSlice = timeSlices.find(ts => 
                ['BASELINE', 'PERMANENT', 'SNAPSHOT'].includes(ts.AirspaceTimeSlice?.interpretation)
            )?.AirspaceTimeSlice || timeSlices[0]?.AirspaceTimeSlice;
            
        // Máquina de Estados para limpar RTF com perfeição (V42.60)
        const stripRtf = (str) => {
            if (!str || typeof str !== 'string') return '';
            // Remove bullets e caracteres de lista comuns no DECEA
            let clean = str.replace(/·/g, '').trim();
            if (!clean.includes('{\\rtf1')) return clean;
            
            let text = '';
            let inGroup = 0;
            let skipGroup = 0;
            
            for (let i = 0; i < str.length; i++) {
                const char = str[i];
                if (char === '{') {
                    inGroup++;
                    if (str.substring(i, i + 12).match(/\{\\(\*|fonttbl|colortbl|stylesheet|info)/)) {
                        if (skipGroup === 0) skipGroup = inGroup;
                    }
                } else if (char === '}') {
                    if (skipGroup === inGroup) skipGroup = 0;
                    inGroup--;
                } else if (char === '\\') {
                    let j = i + 1;
                    if (str[j] === "'") { 
                        const hex = str.substring(j + 1, j + 3);
                        text += String.fromCharCode(parseInt(hex, 16));
                        i = j + 2; 
                    }
                    else if (str[j] && str[j].match(/[a-zA-Z]/)) {
                        while (str[j] && str[j].match(/[a-zA-Z0-9-]/)) j++;
                        if (str[j] === ' ') j++; 
                        i = j - 1;
                    } else { i = j; } 
                } else {
                    if (skipGroup === 0 && inGroup > 0 && char !== '\r' && char !== '\n') {
                        text += char;
                    }
                }
            }
            return text.replace(/&#[0-9]+;/g, '').replace(/·/g, '').replace(/\s+/g, ' ').trim();
        };

        const getLimit = (node) => {
            if (!node) return null;
            const val = node.val || node['#text'] || node;
            return val !== undefined ? parseFloat(val) : null;
        };

        // Extrator Universal de Notas Poliglotas (Cão Farejador com Inteligência Semântica)
        const extractAllNotes = (obj) => {
            let notes = [];
            const traverse = (o) => {
                if (!o) return;
                if (Array.isArray(o)) {
                    o.forEach(traverse);
                } else if (typeof o === 'object') {
                    if (o.translatedNote) {
                        const tNotes = Array.isArray(o.translatedNote) ? o.translatedNote : [o.translatedNote];
                        let bestText = null;
                        let maxPorScore = -999;

                        tNotes.forEach(tn => {
                            const rawText = tn?.LinguisticNote?.note?.['#text'] || '';
                            const text = stripRtf(rawText);
                            if (!text || text.length < 2) return;
                            
                            const lang = String(tn?.LinguisticNote?.note?.['@_lang'] || '').toUpperCase();
                            const lowerText = text.toLowerCase();
                            let score = 0;
                            
                            const porWords = ['em', 'com', 'sob', 'para', 'de', 'da', 'do', 'mediante', 'coordenação', 'coordenacao', 'ativado', 'ativada', 'pelo', 'pela', 'aos', 'às', 'até', 'sujeito', 'voos', 'rádio', 'livre', 'balões', 'quente'];
                            porWords.forEach(w => { if (new RegExp(`\\b${w}\\b`).test(lowerText)) score += 2; });
                            
                            const engWords = ['under', 'with', 'and', 'by', 'subject', 'activated', 'to', 'from', 'for', 'coordination', 'coordenation', 'flights', 'free', 'hot', 'balloons'];
                            engWords.forEach(w => { if (new RegExp(`\\b${w}\\b`).test(lowerText)) score -= 2; });

                            if (lang === 'POR') score += 1;

                            if (score > maxPorScore) {
                                maxPorScore = score;
                                bestText = text;
                            }
                        });

                        if (bestText && !notes.includes(bestText)) notes.push(bestText);
                    } else if (o.Note && o.Note.text && !o.Note.translatedNote) {
                        const text = stripRtf(o.Note.text);
                        if (text && text.length >= 2 && !notes.includes(text)) notes.push(text);
                    } else if (o.Annotation && o.Annotation.text) {
                        const text = stripRtf(o.Annotation.text);
                        if (text && text.length >= 2 && !notes.includes(text)) notes.push(text);
                    } else if (o.text && typeof o.text === 'string' && !o.translatedNote) {
                        const text = stripRtf(o.text);
                        if (text && text.length >= 2 && !notes.includes(text)) notes.push(text);
                    }
                    Object.values(o).forEach(traverse);
                }
            };
            traverse(obj);
            return notes;
        };

            if (timeSlice && ['R', 'P', 'D'].includes(timeSlice.type)) {
                
                const geometry = timeSlice.geometryComponent?.AirspaceGeometryComponent || timeSlice.geometryComponent;
                const volume = geometry?.theAirspaceVolume?.AirspaceVolume || geometry?.theAirspaceVolume;
                
                const rawUpper = volume?.upperLimit || timeSlice.upperLimit || geometry?.upperLimit;
                const rawLower = volume?.lowerLimit || timeSlice.lowerLimit || geometry?.lowerLimit;
                const upperRef = volume?.upperLimitReference || timeSlice.upperLimitReference || '';
                const lowerRef = volume?.lowerLimitReference || timeSlice.lowerLimitReference || '';

                // Horário / Ativação
                const activationList = Array.isArray(timeSlice.activation) ? timeSlice.activation : [timeSlice.activation];
                const firstActivation = activationList[0]?.AirspaceActivation || activationList[0];
                const timesheet = firstActivation?.timeInterval?.Timesheet || firstActivation?.timeInterval;
                
                let horarioFinal = 'CONSULTAR NOTAM';
                if (timesheet) {
                    const start = timesheet.startEvent || timesheet.startTime;
                    const end = timesheet.endEvent || timesheet.endTime;

                    if (start === 'SR' && end === 'SS') {
                        horarioFinal = 'Do nascer ao pôr do sol';
                    } else if (start === 'SS' && end === 'SR') {
                        horarioFinal = 'Do pôr do sol ao nascer';
                    } else if (start === '00:00' && end === '00:00') {
                        horarioFinal = 'H24';
                    } else if (start && end) {
                        horarioFinal = `${start} - ${end} UTC`;
                    }
                }

                // 3. Notas de Ativação (Enriquecimento)
                const activationNotes = extractAllNotes(firstActivation);
                const activationNoteStr = activationNotes.filter(n => n.length > 2).join(' / ');
                
                // 🛡️ AVIONICS MASTER: Separação Inteligente de Horário e Notas
                let extraObsFromActivation = '';
                if (activationNoteStr) {
                    // Tenta detectar padrão "H24 (Nota...)"
                    const splitMatch = activationNoteStr.match(/^([A-Z0-9\-\s]+)\s*\((.*)\)$/i);
                    if (splitMatch) {
                        horarioFinal = splitMatch[1].trim();
                        extraObsFromActivation = splitMatch[2].trim();
                    } else if (horarioFinal === 'CONSULTAR NOTAM' || horarioFinal === 'H24') {
                        // Se já temos H24 e a nota não segue o padrão de parênteses, 
                        // tratamos a nota como observação extra
                        extraObsFromActivation = activationNoteStr;
                    } else {
                        horarioFinal = activationNoteStr;
                    }
                }

                // 4. Observações Gerais e Atividades
                const allNotes = extractAllNotes(timeSlice, 'POR');
                let observacoesFinal = allNotes
                    .filter(n => n.length > 2 && !activationNotes.includes(n))
                    .join(' / ')
                    .trim();

                if (extraObsFromActivation) {
                    observacoesFinal = (extraObsFromActivation + (observacoesFinal ? ' / ' : '') + observacoesFinal)
                        .replace(/\/ \//g, '/')
                        .replace(/^\/|\/$/g, '')
                        .trim();
                }

                const activityMap = { 
                    'OTHER': 'OUTRAS ATIVIDADES', 'TRAINING': 'TREINAMENTO', 'MILOPS': 'OPERAÇÕES MILITARES',
                    'SHOOTING': 'TIRO REAL', 'AIR_GUN': 'ARTILHARIA AÉREA', 'AEROCLUB': 'AEROCLUBE',
                    'ACROBATICS': 'VOOS ACROBÁTICOS / ACROBACIAS', 'AEROBATICS': 'VOOS ACROBÁTICOS / ACROBACIAS',
                    'EXERCISE': 'COMBATE AÉREO / EXERCÍCIOS', 'GLIDER': 'PLANADOR / VOO A VELA',
                    'PARACHUTE': 'PÁRA-QUEDISMO', 'UAS': 'DRONE / UAS', 'SPORT': 'ATIVIDADE ESPORTIVA',
                    'NAVAL_EXER': 'EXERCÍCIOS NAVAIS'
                };
                
                const activities = activationList.map(a => (a.AirspaceActivation || a)?.activity).filter(Boolean);
                if (activities.length) {
                    const actStr = activities.map(act => activityMap[act] || act).join(' / ');
                    if (!observacoesFinal.includes(actStr)) observacoesFinal += (observacoesFinal ? ' / ' : '') + actStr;
                }

                const mapRef = (ref, uom) => {
                    const r = String(ref?.['#text'] || ref?.val || ref || '').toUpperCase();
                    if (r === 'SFC') return 'GND';
                    if (r === 'MSL') return 'MSL';
                    if (r === 'STD' || uom === 'FL') return 'STD';
                    return 'AGL';
                };

                const parseLimit = (limit) => {
                    const val = limit?.['#text'] || limit?.val || limit;
                    return val !== undefined ? parseFloat(val) : null;
                };

                const safeTimeSlice = JSON.parse(JSON.stringify(timeSlice));
                if (safeTimeSlice.geometryComponent) delete safeTimeSlice.geometryComponent;

                enrichedData.push({
                    ident: String(timeSlice.designator || 'UNKN'),
                    nome: timeSlice.name || null,
                    tipo: timeSlice.type,
                    upperlimit: parseLimit(rawUpper),
                    uom_ulimit: rawUpper?.['@_uom'] || 'FL',
                    lowerlimit: parseLimit(rawLower),
                    uom_llimit: rawLower?.['@_uom'] || 'FL',
                    ref_lower: mapRef(lowerRef, rawLower?.['@_uom']),
                    ref_upper: mapRef(upperRef, rawUpper?.['@_uom']),
                    horario: horarioFinal,
                    observacoes: observacoesFinal || 'OUTRAS ATIVIDADES',
                    full_aixm_node: safeTimeSlice
                });
            }
        });

        console.log(`📊 [ROBOT] ${enrichedData.length} áreas processadas. Sincronizando com o Banco...`);

        let effectiveDate = '2026-04-16';
        try {
            const rawAmdt = selectedItem?.amdt || '';
            const dateMatch = String(rawAmdt).match(/(\d{4}-\d{2}-\d{2})/);
            if (dateMatch) effectiveDate = dateMatch[1];
        } catch (e) {}

        let countUpdated = 0;
        for (const area of enrichedData) {
            // 🛡️ AVIONICS MASTER: Match resiliente (remove espaços para evitar divergência WFS vs AIXM)
            const cleanIdent = area.ident.replace(/\s+/g, '');

            const { data: existingArray } = await supabase
                .from('eac_snapshots')
                .select('raw_properties, id, ident')
                .or(`ident.eq."${area.ident}",ident.eq."${cleanIdent}"`)
                .eq('tipo', area.tipo)
                .eq('is_current', true)
                .limit(1);

            const existing = existingArray?.[0];

            if (existing) {
                // 🛡️ AVIONICS MASTER: Proteção de Dados (Não sobrescreve nota rica por nota genérica)
                const currentObs = existing.raw_properties?.observacoes || existing.raw_properties?.properties?.observacao || '';
                const newObs = area.observacoes || '';
                const isGeneric = (n) => !n || n.toUpperCase().includes('SEM OBSERVAÇÃO') || n.toUpperCase() === 'PROCEDURE' || n.toUpperCase() === 'TECHNICAL';
                
                const finalObsToSave = (isGeneric(newObs) && !isGeneric(currentObs)) ? currentObs : newObs;

                const { error: updateError } = await supabase
                    .from('eac_snapshots')
                    .update({
                        efetivacao: effectiveDate,
                        raw_properties: {
                            ...(existing.raw_properties || {}),
                            properties: {
                                ...(existing.raw_properties?.properties || {}),
                                observacao: finalObsToSave,
                                horario: area.horario || existing.raw_properties?.horario,
                                aip_source: 'AIXM 5.1 (SkyFPL Robot)'
                            },
                            ident: area.ident,
                            nome: area.nome || existing.nome,
                            observacoes: finalObsToSave,
                            horario: area.horario || existing.raw_properties?.horario,
                            aip_source: 'AIXM 5.1 (SkyFPL Robot)'
                        }
                    })
                    .eq('id', existing.id);

                if (!updateError) countUpdated++;
            }
        }
        console.log(`✅ [ROBOT] Sincronização concluída! Atualizadas: ${countUpdated} de ${enrichedData.length}`);

        // ==========================================
        // ROTA 1: MOTOR DE FALLBACK (HEURÍSTICA)
        // Para áreas que o DECEA esqueceu no AIXM
        // ==========================================
        console.log(`\n🔍 [ROBOT] Iniciando Motor de Fallback para áreas Órfãs (PENDENTE)...`);
        const { data: pendingAreas, error: pendingError } = await supabase
            .from('eac_snapshots')
            .select('*')
            .eq('is_current', true)
            .is('raw_properties->>aip_source', null);

        if (pendingError) {
            console.error(`❌ Erro ao buscar áreas pendentes:`, pendingError);
        } else if (pendingAreas && pendingAreas.length > 0) {
            console.log(`💡 Encontradas ${pendingAreas.length} áreas abandonadas pelo DECEA. Acionando Dedução Semântica...`);
            let countFallback = 0;
            
            for (const orphan of pendingAreas) {
                // 🛡️ AVIONICS MASTER: Marcar como ausente preservando a geometria
                const fallbackProps = {
                    ...(orphan.raw_properties || {}),
                    observacoes: 'DADOS AUSENTES NO AIXM',
                    horario: 'NÃO INFORMADO',
                    processed_at: new Date().toISOString(),
                    aip_source: 'WFS GEOJSON (FALLBACK)' 
                };

                const { error: fallbackError } = await supabase
                    .from('eac_snapshots')
                    .update({ raw_properties: fallbackProps })
                    .eq('id', orphan.id);

                if (fallbackError) {
                    console.error(`❌ Erro no fallback do ${orphan.ident}:`, fallbackError);
                } else {
                    countFallback++;
                }
            }
            console.log(`🤖 [ROBOT] Processamento concluído. Áreas marcadas como DADOS AUSENTES: ${countFallback} de ${pendingAreas.length}`);
        } else {
            console.log(`✨ Nenhuma área órfã encontrada. A sincronização está 100% íntegra.`);
        }

    } catch (error) {
        console.error('❌ [ROBOT] Erro fatal no pipeline:', error.message);
        process.exit(1);
    }
}

runSync();
