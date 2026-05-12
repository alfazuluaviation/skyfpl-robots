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

        // 1. Indexar Serviços, Unidades e Frequências
        console.log('📻 [ROBOT] Indexando Frequências e Serviços...');
        const serviceMap = {}; // nome/designator -> frequencies[]
        
        members.forEach(member => {
            // Tenta encontrar qualquer tipo de serviço ou unidade
            const entity = member.Service || member.ApproachControlService || member.AreaControlService || 
                           member.AerodromeControlService || member.Unit || member.AirTrafficControlService;
            if (!entity) return;

            const timeSlice = (Array.isArray(entity.timeSlice) ? entity.timeSlice : [entity.timeSlice])[0];
            const data = timeSlice?.ServiceTimeSlice || timeSlice?.UnitTimeSlice || timeSlice?.AirTrafficControlServiceTimeSlice;
            if (!data) return;

            const name = data.name;
            const designator = data.designator;
            
            // Coletar Frequências (podem estar em radioCommunicationChannel ou em associações de unidade)
            const frequencies = [];
            const channels = Array.isArray(data.radioCommunicationChannel) ? data.radioCommunicationChannel : [data.radioCommunicationChannel];
            
            channels.forEach(ch => {
                const channel = ch?.RadioCommunicationChannel;
                if (!channel) return;
                const freq = channel.transmissionFrequency?.val || channel.transmissionFrequency;
                const uom = channel.transmissionFrequency?.['@_uom'] || 'MHz';
                if (freq) frequencies.push(`${freq} ${uom}`);
            });

            // Indexar por Designador (ex: SBWA) e por Nome (ex: AMAZONAS)
            const keys = [designator, name].filter(Boolean);
            keys.forEach(k => {
                const normalizedKey = k.toString().toUpperCase();
                if (!serviceMap[normalizedKey]) serviceMap[normalizedKey] = [];
                serviceMap[normalizedKey].push(...frequencies);
            });
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

                // Tentar vincular frequências pelo designator ou nome (busca inteligente / Fuzzy Match)
                const normalize = (str) => str?.normalize("NFD").replace(/[\u0300-\u036f]/g, "").toUpperCase().replace(/[^A-Z0-9]/g, "") || '';
                const normIdent = normalize(ident);
                const normName = normalize(name);

                let freqs = serviceMap[ident?.toUpperCase()] || [];
                
                if (freqs.length === 0) {
                    const matchingKey = Object.keys(serviceMap).find(k => {
                        const normKey = normalize(k);
                        // 1. Match exato ou contido
                        if (normKey.includes(normIdent) || normIdent.includes(normKey)) return true;
                        if (normKey.includes(normName) || normName.includes(normKey)) return true;
                        // 2. Fuzzy match por prefixo (primeiros 6 caracteres)
                        if (normName.length >= 6 && normKey.startsWith(normName.substring(0, 6))) return true;
                        if (normKey.length >= 6 && normName.startsWith(normKey.substring(0, 6))) return true;
                        return false;
                    });
                    if (matchingKey) freqs = serviceMap[matchingKey];
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
                            ...(existing.raw_properties || {}),
                            aip_data: {
                                horario: area.horario,
                                observacoes: area.observacoes,
                                frequencias: area.frequencias,
                                upperLimit: area.upperLimit,
                                uom_upper: area.uom_upper,
                                lowerLimit: area.lowerLimit,
                                uom_lower: area.uom_lower,
                                full_aixm_node: area.raw, // Salvando o nó bruto para auditoria
                                processed_at: new Date().toISOString(),
                                source: 'SkyFPL Structural Robot (AIXM 5.1)'
                            }
                        }
                    })
                    .eq('id', existing.id);
                
                if (!error) {
                    count++;
                } else {
                    console.warn(`⚠️ [ROBOT] Erro ao atualizar ${area.type} ${area.ident}:`, error.message);
                }
            } else {
                console.log(`ℹ️ [ROBOT] Ignorando ${area.type} ${area.ident}: Não encontrada no banco (is_current=true).`);
            }
        }

        console.log(`✅ [ROBOT] Sincronização concluída! ${count} registros atualizados.`);

    } catch (err) {
        console.error('❌ [ROBOT] Erro fatal:', err.message);
        process.exit(1);
    }
}

runSync();
