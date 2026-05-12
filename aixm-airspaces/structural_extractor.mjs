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
        
        const xmlFileName = Object.keys(zip.files).find(f => f.endsWith('.xml'));
        if (!xmlFileName) throw new Error('XML não encontrado no arquivo ZIP.');
        
        console.log(`📄 [ROBOT] Extraindo XML: ${xmlFileName}`);
        const xmlText = await zip.files[xmlFileName].async('text');
        
        console.log('🔍 [ROBOT] Analisando XML...');
        const parser2 = new XMLParser({ ignoreAttributes: false, attributeNamePrefix: "@_", removeNSPrefix: true });
        const jsonObj2 = parser2.parse(xmlText);
        const members = jsonObj2.AIXMBasicMessage?.hasMember || [];

        // 1. Indexar Serviços, Unidades e Frequências (Busca Recursiva Profunda corrigida para Arrays)
        console.log('📻 [ROBOT] Iniciando busca recursiva de frequências...');
        const serviceMap = {}; // nome/designator -> frequencies[]
        
        const findFrequencies = (obj, targetList) => {
            if (!obj || typeof obj !== 'object') return;
            
            // Se for um Array, percorre cada item
            if (Array.isArray(obj)) {
                obj.forEach(item => findFrequencies(item, targetList));
                return;
            }

            // Se encontrar um canal de rádio
            if (obj.transmissionFrequency || obj.RadioCommunicationChannel) {
                const ch = obj.RadioCommunicationChannel || obj;
                const freqData = ch.transmissionFrequency;
                if (freqData) {
                    let val = freqData.val || freqData['#text'] || freqData;
                    if (typeof val === 'object' && val['#text']) val = val['#text'];
                    const uom = freqData['@_uom'] || 'MHz';
                    if (val && typeof val !== 'object') targetList.push(`${val} ${uom}`);
                }
            }

            // Continua a busca em todas as propriedades
            Object.keys(obj).forEach(k => {
                if (k !== 'timeSlice') findFrequencies(obj[k], targetList); // Evita loop infinito em referências circulares se houver
            });
        };

        members.forEach(member => {
            const entity = Object.values(member)[0];
            if (!entity || typeof entity !== 'object') return;

            const frequencies = [];
            findFrequencies(entity, frequencies);

            if (frequencies.length > 0) {
                const timeSlices = Array.isArray(entity.timeSlice) ? entity.timeSlice : [entity.timeSlice];
                timeSlices.forEach(ts => {
                    const data = ts?.ServiceTimeSlice || ts?.UnitTimeSlice || ts?.AirTrafficControlServiceTimeSlice || ts;
                    const sKeys = [data?.designator, data?.name].filter(Boolean);
                    sKeys.forEach(k => {
                        const normalizedKey = k.toString().toUpperCase();
                        if (!serviceMap[normalizedKey]) serviceMap[normalizedKey] = [];
                        serviceMap[normalizedKey].push(...frequencies);
                    });
                });
            }
        });
        
        const totalServices = Object.keys(serviceMap).length;
        console.log(`📡 [ROBOT] Varredura finalizada. ${totalServices} serviços/unidades indexados com frequências.`);

        // 2. Processar Espaços Aéreos
        console.log('🌍 [ROBOT] Processando Espaços Aéreos...');
        const structuralTypes = ['TMA', 'CTR', 'FIR', 'CTA'];
        const enrichedData = [];

        members.forEach(member => {
            const airspace = member.Airspace;
            if (!airspace) return;

            const timeSlice = (Array.isArray(airspace.timeSlice) ? airspace.timeSlice : [airspace.timeSlice])
                .find(ts => ['BASELINE', 'PERMANENT', 'SNAPSHOT'].includes(ts.AirspaceTimeSlice?.interpretation))?.AirspaceTimeSlice;
            
            if (timeSlice && structuralTypes.includes(timeSlice.type)) {
                const ident = String(timeSlice.designator || '');
                const nam = timeSlice.name || '';
                
                // Mapeamento de Tipo para evitar erro de Constraint no Supabase
                // Se o banco não aceita CTA, mapeamos para TMA (que é estruturalmente similar)
                let dbType = timeSlice.type;
                if (dbType === 'CTA') dbType = 'TMA'; 
                
                // Horários
                const activation = (Array.isArray(timeSlice.activation) ? timeSlice.activation : [timeSlice.activation])[0]?.AirspaceActivation;
                let horario = 'CONSULTAR NOTAM';
                if (activation?.timeInterval?.Timesheet) {
                    const ts = activation.timeInterval.Timesheet;
                    if (ts.startEvent === 'SR' && ts.endEvent === 'SS') horario = 'Do nascer ao pôr do sol';
                    else if (ts.startTime === '00:00' && ts.endTime === '00:00') horario = 'H24';
                    else if (ts.startTime && ts.endTime) horario = `${ts.startTime} - ${ts.endTime} UTC`;
                }

                // Observações
                const notes = extractAllNotes(timeSlice);
                const obs = toTacticalCase(notes.join(' / ')) || 'SEM OBSERVAÇÕES';

                // Tentar vincular frequências
                const normalize = (str) => str?.normalize("NFD").replace(/[\u0300-\u036f]/g, "").toUpperCase().replace(/[^A-Z0-9]/g, "") || '';
                const normIdent = normalize(ident);
                const normName = normalize(nam);

                let freqs = serviceMap[ident?.toUpperCase()] || [];
                
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

                enrichedData.push({
                    ident,
                    nam,
                    type: timeSlice.type,
                    upperLimit: timeSlice.upperLimit?.val || timeSlice.upperLimit,
                    uom_upper: timeSlice.upperLimit?.['@_uom'] || 'FL',
                    lowerLimit: timeSlice.lowerLimit?.val || timeSlice.lowerLimit,
                    uom_lower: timeSlice.lowerLimit?.['@_uom'] || 'FL',
                    horario,
                    observacoes: obs,
                    frequencias: [...new Set(freqs)],
                    raw: timeSlice
                });
            }
        });

        console.log(`📊 [ROBOT] ${enrichedData.length} áreas encontradas. Sincronizando Supabase...`);

        let count = 0;
        for (const area of enrichedData) {
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
                        frequencias: area.frequencias,
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
