import fs from 'fs';
import axios from 'axios';
import JSZip from 'jszip';
import { XMLParser } from 'fast-xml-parser';
import { createClient } from '@supabase/supabase-js';

// Configurações via Variáveis de Ambiente (GitHub Secrets)
const SUPABASE_URL = process.env.SUPABASE_URL;
const SUPABASE_KEY = process.env.SUPABASE_SERVICE_ROLE_KEY;

if (!SUPABASE_URL || !SUPABASE_KEY) {
    console.error('❌ Erro: SUPABASE_URL e SUPABASE_SERVICE_ROLE_KEY são obrigatórios.');
    process.exit(1);
}

const supabase = createClient(SUPABASE_URL, SUPABASE_KEY);

// Máquina de Estados para limpar RTF com perfeição
const stripRtf = (str) => {
    if (!str || typeof str !== 'string') return '';
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
                    let score = (lang === 'POR') ? 1 : 0;
                    if (score > maxPorScore) { maxPorScore = score; bestText = text; }
                });
                if (bestText && !notes.includes(bestText)) notes.push(bestText);
            } else if (o.Note?.text) {
                const text = stripRtf(o.Note.text);
                if (text && !notes.includes(text)) notes.push(text);
            }
            Object.values(o).forEach(traverse);
        }
    };
    traverse(obj);
    return notes;
};

const toTacticalCase = (str) => {
    if (!str) return '';
    const acronyms = ['NOTAM', 'H24', 'UTC', 'GND', 'MSL', 'AGL', 'FL', 'AIP', 'DECEA', 'VFR', 'IFR', 'ATC', 'UAS', 'FIR', 'TMA', 'CTR', 'CTA', 'SBBS', 'SBRE', 'SBAZ', 'SBCW', 'SBAO'];
    return str.split(' ').map(word => {
        const cleanWord = word.replace(/[().,]/g, '').toUpperCase();
        if (acronyms.includes(cleanWord)) return word.toUpperCase();
        return word.charAt(0).toUpperCase() + word.slice(1).toLowerCase();
    }).join(' ');
};

async function runSync() {
    console.log('🚀 [ROBOT-STRUCTURAL] Iniciando extração de TMA, CTR, FIR, CTA...');
    
    try {
        console.log('🔍 [ROBOT] Consultando catálogo oficial via Edge Function...');
        const DISCOVERY_URL = `${SUPABASE_URL}/functions/v1/fetch-aisweb-data`;
        const discoveryRes = await axios.post(DISCOVERY_URL, 
            { area: 'pub', type: 'aixm' },
            { headers: { 'Authorization': `Bearer ${SUPABASE_KEY}` } }
        );

        if (!discoveryRes.data?.success) {
            throw new Error('Falha ao descobrir link: ' + (discoveryRes.data?.error || 'Erro desconhecido'));
        }

        const parser = new XMLParser({ ignoreAttributes: false, removeNSPrefix: true });
        const jsonObj = parser.parse(discoveryRes.data.xml);
        const items = jsonObj.aisweb?.pub?.item || [];
        const itemsArray = Array.isArray(items) ? items : [items];
        
        console.log(`📦 [ROBOT] Catálogo AIXM encontrado. ${itemsArray.length} itens disponíveis.`);
        itemsArray.forEach((it, idx) => {
            console.log(`   [${idx}] Nome: ${it.name} | Data: ${it.date} | Link: ${it.link || it.file}`);
        });

        const selectedItem = itemsArray.find(item => {
            const name = String(item.name || '').toLowerCase();
            return (name.includes('completo') || name.includes('full')) && !name.includes('baseline');
        }) || itemsArray.find(item => {
            const name = String(item.name || '').toLowerCase();
            return name.includes('snapshot') || name.includes('baseline');
        }) || itemsArray[0];

        console.log(`🎯 [ROBOT] Item Selecionado: ${selectedItem.name}`);

        let dynamicLink = selectedItem?.link || selectedItem?.file || '';
        if (typeof dynamicLink === 'object') dynamicLink = dynamicLink['#text'] || '';
        
        if (!dynamicLink) {
            console.error('📦 XML recebido:', discoveryRes.data.xml);
            throw new Error('Não foi possível encontrar o link de download no catálogo do DECEA.');
        }

        // Limpeza agressiva: remover CDATA e sufixos estranhos
        dynamicLink = dynamicLink.replace(']]>', '').replace('<![CDATA[', '').split('">')[0].trim();

        console.log(`🛰️ [ROBOT] Link Autorizado: ${dynamicLink}`);
        
        const tempPath = './aixm_structural_temp.zip';
        const { execSync } = await import('child_process');
        
        console.log('📦 [ROBOT] Iniciando download direto via CURL...');
        const curlCmd = `curl -L -A "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/121.0.0.0 Safari/537.36" \
            -H "Referer: https://aisweb.decea.mil.br/?i=download" \
            --connect-timeout 60 \
            --retry 3 \
            -o ${tempPath} "${dynamicLink}"`;

        execSync(curlCmd, { stdio: 'inherit' });

        if (!fs.existsSync(tempPath) || fs.statSync(tempPath).size < 1000000) {
            throw new Error('O arquivo baixado pelo CURL é muito pequeno ou não existe.');
        }

        const zipData = fs.readFileSync(tempPath);
        const zip = await JSZip.loadAsync(zipData);
        fs.unlinkSync(tempPath);
        
        const allFiles = Object.keys(zip.files);
        console.log(`📦 [ROBOT] Arquivos no ZIP: ${allFiles.join(', ')}`);

        const xmlFiles = allFiles.filter(f => f.endsWith('.xml'));
        if (xmlFiles.length === 0) throw new Error('Nenhum arquivo XML encontrado no ZIP.');
        
        console.log(`📄 [ROBOT] Encontrados ${xmlFiles.length} arquivos XML. Iniciando processamento global...`);
        
        const serviceMap = {}; // nome/designator -> frequencies[]
        const enrichedData = [];

        for (const xmlFileName of xmlFiles) {
            console.log(`🔍 [ROBOT] Analisando: ${xmlFileName}`);
            const xmlText = await zip.files[xmlFileName].async('string');

            // --- SONDA EXPERIMENTAL (Debug Probe) ---
            const probes = ['119.1', '128.6', '122.500', '122,500'];
            probes.forEach(p => {
                const idx = xmlText.indexOf(p);
                if (idx !== -1) {
                    console.log(`🎯 [SONDA] Valor '${p}' encontrado no arquivo ${xmlFileName}!`);
                    console.log(`📝 [SONDA] Contexto: ...${xmlText.substring(idx - 100, idx + 100)}...`);
                }
            });
            // ----------------------------------------
            const parser2 = new XMLParser({ 
                ignoreAttributes: false, 
                attributeNamePrefix: "@_", 
                removeNSPrefix: true,
                alwaysArray: ["hasMember", "timeSlice", "radioCommunicationChannel", "translatedNote"]
            });
            const jsonObj2 = parser2.parse(xmlText);
            const members = jsonObj2.AIXMBasicMessage?.hasMember || [];

            // 1. Indexar Frequências (Busca Recursiva Profunda)
            const findFrequencies = (obj, targetList) => {
                if (!obj || typeof obj !== 'object') return;
                if (Array.isArray(obj)) {
                    obj.forEach(item => findFrequencies(item, targetList));
                    return;
                }
                for (const k in obj) {
                    if (k.toLowerCase().includes('frequency')) {
                        const data = obj[k];
                        if (data) {
                            let val = data.val || data['#text'] || (typeof data !== 'object' ? data : null);
                            if (val && typeof val === 'object' && val['#text']) val = val['#text'];
                            const uom = data['@_uom'] || 'MHz';
                            if (val && !isNaN(parseFloat(val))) targetList.push(`${val} ${uom}`);
                        }
                    }
                    findFrequencies(obj[k], targetList);
                }
            };

            members.forEach(member => {
                const frequencies = [];
                findFrequencies(member, frequencies);

                if (frequencies.length > 0) {
                    const entity = Object.values(member)[0];
                    const timeSlices = Array.isArray(entity?.timeSlice) ? entity.timeSlice : (entity?.timeSlice ? [entity.timeSlice] : []);
                    timeSlices.forEach(ts => {
                        const data = ts?.ServiceTimeSlice || ts?.UnitTimeSlice || ts?.AirTrafficControlServiceTimeSlice || ts;
                        if (!data) return;
                        const sKeys = [data.designator, data.name].filter(Boolean);
                        sKeys.forEach(k => {
                            const normalizedKey = k.toString().toUpperCase();
                            if (!serviceMap[normalizedKey]) serviceMap[normalizedKey] = [];
                            serviceMap[normalizedKey].push(...frequencies);
                        });
                    });
                }
            });

            // 2. Coletar Espaços Aéreos (apenas se houver Airspace no arquivo)
            const structuralTypes = ['TMA', 'CTR', 'FIR', 'CTA'];
            members.forEach(member => {
                const airspace = member.Airspace;
                if (!airspace) return;

                const timeSlices = Array.isArray(airspace.timeSlice) ? airspace.timeSlice : (airspace.timeSlice ? [airspace.timeSlice] : []);
                const timeSlice = timeSlices
                    .find(ts => ['BASELINE', 'PERMANENT', 'SNAPSHOT'].includes(ts.AirspaceTimeSlice?.interpretation))?.AirspaceTimeSlice;
                
                if (timeSlice && structuralTypes.includes(timeSlice.type)) {
                    const ident = String(timeSlice.designator || '');
                    const nam = timeSlice.name || '';
                    
                    let dbType = timeSlice.type;
                    if (dbType === 'CTA' || dbType === 'FIR') dbType = 'TMA'; 

                    const activation = (timeSlice.activation || [])[0]?.AirspaceActivation;
                    let horario = 'CONSULTAR NOTAM';
                    if (activation?.timeInterval?.Timesheet) {
                        const ts = activation.timeInterval.Timesheet;
                        if (ts.startEvent === 'SR' && ts.endEvent === 'SS') horario = 'Do nascer ao pôr do sol';
                        else if (ts.startTime === '00:00' && ts.endTime === '00:00') horario = 'H24';
                        else if (ts.startTime && ts.endTime) horario = `${ts.startTime} - ${ts.endTime} UTC`;
                    }

                    const notes = extractAllNotes(timeSlice);
                    const obs = toTacticalCase(notes.join(' / ')) || 'SEM OBSERVAÇÕES';

                    enrichedData.push({
                        ident,
                        nam,
                        type: dbType,
                        originalType: timeSlice.type,
                        upperLimit: timeSlice.upperLimit?.val || timeSlice.upperLimit,
                        uom_upper: timeSlice.upperLimit?.['@_uom'] || 'FL',
                        lowerLimit: timeSlice.lowerLimit?.val || timeSlice.lowerLimit,
                        uom_lower: timeSlice.lowerLimit?.['@_uom'] || 'FL',
                        horario,
                        observacoes: obs,
                        raw: timeSlice
                    });
                }
            });
        }
        
        console.log(`📡 [ROBOT] Varredura finalizada. ${Object.keys(serviceMap).length} serviços/unidades indexados.`);

        // 3. Vincular Frequências e Sincronizar Supabase
        console.log('🌍 [ROBOT] Vinculando Frequências e Sincronizando Supabase...');
        const normalize = (str) => str?.normalize("NFD").replace(/[\u0300-\u036f]/g, "").toUpperCase().replace(/[^A-Z0-9]/g, "") || '';
        
        let count = 0;
        for (const area of enrichedData) {
            const normIdent = normalize(area.ident);
            const normName = normalize(area.nam);
            let freqs = serviceMap[area.ident?.toUpperCase()] || [];

            if (freqs.length === 0) {
                const matchingKey = Object.keys(serviceMap).find(k => {
                    const normKey = normalize(k);
                    if (normKey.includes(normIdent) || normIdent.includes(normKey)) return true;
                    if (normKey.includes(normName) || normName.includes(normKey)) return true;
                    if (normName.length >= 6 && normKey.startsWith(normName.substring(0, 6))) return true;
                    if (normKey.length >= 6 && normName.startsWith(normKey.substring(0, 6))) return true;
                    return false;
                });
                if (matchingKey) freqs = serviceMap[matchingKey];
            }
            
            const finalFrequencies = [...new Set(freqs)];

            // Fallback: Extração via Regex nas Observações (Limpa RTF primeiro)
            if (finalFrequencies.length === 0 && area.observacoes) {
                const cleanObs = area.observacoes
                    .replace(/\\['][a-f0-9]{2}/g, '') 
                    .replace(/\\[a-z0-9]+/g, ' ')     
                    .replace(/[{}]/g, '');            

                if (area.ident === 'SBWB_01') {
                    console.log(`🧪 [DEBUG] Observações SBWB_01 (Limpa): ${cleanObs.substring(0, 100)}...`);
                }

                const freqRegex = /(\d{3}[.,]\d{2,3})/g;
                let m;
                while ((m = freqRegex.exec(cleanObs)) !== null) {
                    const f = m[1].replace(',', '.');
                    const num = parseFloat(f);
                    if (num >= 108 && num <= 137) { 
                        finalFrequencies.push(`${num.toFixed(3)} MHz`);
                        if (area.ident === 'SBWB_01') console.log(`🎯 [DEBUG] Frequência capturada para SBWB_01: ${num}`);
                    }
                }
            }

            const { data: existing } = await supabase
                .from('airspace_snapshots')
                .select('id, raw_properties')
                .eq('ident', area.ident)
                .eq('is_current', true)
                .maybeSingle();

            const snapshotData = {
                ident: area.ident,
                nam: area.nam, // CORRIGIDO: name -> nam
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
                        observacoes: area.observacoes,
                        frequencias: [...new Set(finalFrequencies)],
                        upperLimit: area.upperLimit,
                        uom_upper: area.uom_upper,
                        lowerLimit: area.lowerLimit,
                        uom_lower: area.uom_lower,
                        full_aixm_node: area.raw
                    }
                }
            };

            if (existing) {
                // Atualiza existente
                const { error: updateError } = await supabase
                    .from('airspace_snapshots')
                    .update(snapshotData)
                    .eq('id', existing.id);
                
                if (!updateError) count++;
                else console.error(`❌ Erro ao atualizar ${area.ident}:`, updateError.message);
            } else {
                // CRIA NOVO (Extração 100%)
                console.log(`✨ [ROBOT] Criando novo registro: ${area.type} ${area.ident}`);
                const { error: insertError } = await supabase
                    .from('airspace_snapshots')
                    .insert(snapshotData);
                
                if (!insertError) count++;
                else console.error(`❌ Erro ao criar ${area.ident}:`, insertError.message);
            }
        }

        console.log(`✅ [ROBOT] Sincronização concluída! ${count} registros processados.`);

    } catch (err) {
        console.error('❌ [ROBOT] Erro fatal:', err.message);
        process.exit(1);
    }
}

runSync();
