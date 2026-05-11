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
        console.log('🔍 [ROBOT] Consultando catálogo oficial...');
        const DISCOVERY_URL = `${SUPABASE_URL}/functions/v1/fetch-aisweb-data`;
        const discoveryRes = await axios.post(DISCOVERY_URL, 
            { area: 'pub', type: 'aixm' },
            { headers: { 'Authorization': `Bearer ${SUPABASE_KEY}` } }
        );

        const parser = new XMLParser({ ignoreAttributes: false, removeNSPrefix: true });
        const jsonObj = parser.parse(discoveryRes.data.xml);
        const items = Array.isArray(jsonObj.aisweb?.pub?.item) ? jsonObj.aisweb.pub.item : [jsonObj.aisweb.pub.item];
        const selectedItem = items.find(i => String(i.name).toLowerCase().includes('completo')) || items[0];
        let dynamicLink = (selectedItem?.link || selectedItem?.file || '').replace(']]>', '').replace('<![CDATA[', '').split('">')[0].trim();

        console.log(`📦 [ROBOT] Baixando AIXM: ${dynamicLink}`);
        const tempPath = './aixm_structural_temp.zip';
        const { execSync } = await import('child_process');
        execSync(`curl -L -A "Mozilla/5.0" -H "Referer: https://aisweb.decea.mil.br/" -o ${tempPath} "${dynamicLink}"`, { stdio: 'inherit' });

        const zipData = fs.readFileSync(tempPath);
        const zip = await JSZip.loadAsync(zipData);
        fs.unlinkSync(tempPath);
        
        const xmlFileName = Object.keys(zip.files).find(f => f.endsWith('.xml'));
        const xmlText = await zip.files[xmlFileName].async('text');
        
        console.log('🔍 [ROBOT] Analisando XML...');
        const parser2 = new XMLParser({ ignoreAttributes: false, attributeNamePrefix: "@_", removeNSPrefix: true });
        const jsonObj2 = parser2.parse(xmlText);
        const members = jsonObj2.AIXMBasicMessage?.hasMember || [];

        // 1. Indexar Serviços e Frequências
        console.log('📻 [ROBOT] Indexando Frequências e Serviços...');
        const serviceMap = {}; // designator -> frequencies[]
        
        members.forEach(member => {
            const service = member.Service || member.ApproachControlService || member.AreaControlService || member.AerodromeControlService;
            if (!service) return;

            const timeSlice = (Array.isArray(service.timeSlice) ? service.timeSlice : [service.timeSlice])[0]?.ServiceTimeSlice;
            if (!timeSlice) return;

            const name = timeSlice.name;
            const designator = timeSlice.designator;
            
            // Frequências
            const frequencies = [];
            const channels = Array.isArray(timeSlice.radioCommunicationChannel) ? timeSlice.radioCommunicationChannel : [timeSlice.radioCommunicationChannel];
            
            channels.forEach(ch => {
                const channel = ch?.RadioCommunicationChannel;
                if (!channel) return;
                const freq = channel.transmissionFrequency?.val || channel.transmissionFrequency;
                const uom = channel.transmissionFrequency?.['@_uom'] || 'MHz';
                if (freq) frequencies.push(`${freq} ${uom}`);
            });

            if (designator) {
                if (!serviceMap[designator]) serviceMap[designator] = [];
                serviceMap[designator].push(...frequencies);
            }
        });

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
                const name = timeSlice.name || '';
                
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

                // Tentar vincular frequências pelo designator ou nome
                let freqs = serviceMap[ident] || [];
                if (freqs.length === 0) {
                    // Busca por aproximação de nome se falhar o designator
                    const matchingService = Object.keys(serviceMap).find(k => name.includes(k) || k.includes(ident));
                    if (matchingService) freqs = serviceMap[matchingService];
                }

                enrichedData.push({
                    ident,
                    name,
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

        console.log(`📊 [ROBOT] ${enrichedData.length} áreas estruturais encontradas. Sincronizando Supabase...`);

        let count = 0;
        for (const area of enrichedData) {
            const { data: existing } = await supabase
                .from('airspace_snapshots')
                .select('id, raw_properties')
                .eq('ident', area.ident)
                .eq('type', area.type)
                .eq('is_current', true)
                .limit(1)
                .maybeSingle();

            if (existing) {
                const { error } = await supabase
                    .from('airspace_snapshots')
                    .update({
                        raw_properties: {
                            ...existing.raw_properties,
                            aip_data: {
                                horario: area.horario,
                                observacoes: area.observacoes,
                                frequencias: area.frequencias,
                                upperLimit: area.upperLimit,
                                uom_upper: area.uom_upper,
                                lowerLimit: area.lowerLimit,
                                uom_lower: area.uom_lower,
                                processed_at: new Date().toISOString(),
                                source: 'SkyFPL Structural Robot (AIXM 5.1)'
                            }
                        }
                    })
                    .eq('id', existing.id);
                
                if (!error) count++;
            }
        }

        console.log(`✅ [ROBOT] Sincronização concluída! ${count} registros atualizados.`);

    } catch (err) {
        console.error('❌ [ROBOT] Erro fatal:', err.message);
        process.exit(1);
    }
}

runSync();
