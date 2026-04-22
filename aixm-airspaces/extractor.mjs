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
            
            // MODO DETETIVE: Investigar áreas específicas que o usuário reportou como faltantes
            const designator = timeSlices[0]?.AirspaceTimeSlice?.designator || 'UNKN';
            if (designator === 'SBR881' || designator === 'SBR108') {
                console.log(`🔍 [DETETIVE] Encontrada área alvo: ${designator}`);
                console.log(`📄 [DETETIVE] Interpretations: ${timeSlices.map(ts => ts.AirspaceTimeSlice?.interpretation).join(', ')}`);
                console.log(`📄 [DETETIVE] Types: ${timeSlices.map(ts => ts.AirspaceTimeSlice?.type).join(', ')}`);
            }

            const timeSlice = timeSlices.find(ts => 
                ['BASELINE', 'PERMANENT', 'SNAPSHOT'].includes(ts.AirspaceTimeSlice?.interpretation)
            )?.AirspaceTimeSlice || timeSlices[0]?.AirspaceTimeSlice;

            if (timeSlice && ['R', 'P', 'D'].includes(timeSlice.type)) {
                
                const geometry = timeSlice.geometryComponent?.AirspaceGeometryComponent || timeSlice.geometryComponent;
                const volume = geometry?.theAirspaceVolume?.AirspaceVolume || geometry?.theAirspaceVolume;
                
                const rawUpper = volume?.upperLimit || timeSlice.upperLimit;
                const rawLower = volume?.lowerLimit || timeSlice.lowerLimit;
                const lowerRef = volume?.lowerLimitReference || '';

                // Horário H24
                let horarioFinal = 'CONSULTAR NOTAM';
                const activationList = Array.isArray(timeSlice.activation) ? timeSlice.activation : [timeSlice.activation];
                const firstActivation = activationList[0]?.AirspaceActivation || activationList[0];
                const timesheet = firstActivation?.timeInterval?.Timesheet || firstActivation?.timeInterval;
                
                if (timesheet && (timesheet.startTime === '00:00' && timesheet.endTime === '00:00')) {
                    horarioFinal = 'H24';
                } else if (firstActivation?.status === 'ACTIVE') {
                    horarioFinal = 'ATIVO';
                }

                // Observações
                let observacoesFinal = '';
                const circle = volume?.horizontalProjection?.Surface?.patches?.PolygonPatch?.exterior?.Ring?.curveMember?.Curve?.segments?.CircleByCenterPoint;
                if (circle) {
                    const pos = String(circle.pos || '').split(' ');
                    const dmsPos = pos.length === 2 ? toDMS(parseFloat(pos[0]), parseFloat(pos[1])) : pos.join(' ');
                    const radius = circle.radius?.['#text'] || circle.radius?.val || circle.radius || '1.0';
                    observacoesFinal = `ÁREA CIRCULAR COM CENTRO EM ${dmsPos} COM UM RAIO DE ${radius} NM. `;
                }

                const activityMap = { 'OTHER': 'OUTRAS ATIVIDADES / MOTIVOS', 'TRAINING': 'TREINAMENTO MILITAR' };
                const activities = activationList.map(a => (a.AirspaceActivation || a)?.activity).filter(Boolean);
                observacoesFinal += activities.length ? activities.map(act => activityMap[act] || act).join(' / ') : 'OUTRAS ATIVIDADES / MOTIVOS';

                const extraNote = timeSlice.annotation?.Annotation?.text || '';
                if (extraNote && !observacoesFinal.includes(extraNote)) {
                    observacoesFinal += ` / ${extraNote}`;
                }

                enrichedData.push({
                    ident: String(timeSlice.designator || 'UNKN'),
                    tipo: timeSlice.type,
                    nome: timeSlice.name || 'ÁREA SEM NOME',
                    upperlimit: parseFloat(rawUpper?.['#text'] || rawUpper?.val || rawUpper || 0),
                    uom_ulimit: rawUpper?.['@_uom'] || 'FL',
                    lowerlimit: parseFloat(rawLower?.['#text'] || rawLower?.val || rawLower || 0),
                    uom_llimit: rawLower?.['@_uom'] || 'FT',
                    ref_lower: lowerRef === 'SFC' ? 'AGL' : (lowerRef === 'MSL' ? 'MSL' : 'STD'),
                    horario: horarioFinal,
                    observacoes: observacoesFinal.toUpperCase()
                });
            }
        });

        console.log(`📊 [ROBOT] ${enrichedData.length} áreas extraídas. Iniciando sincronização Supabase...`);

        // 5. Sincronização em Massa (Upsert por Identificador)
        for (const area of enrichedData) {
            const { error: updateError } = await supabase
                .from('eac_snapshots')
                .update({
                    // Atualizamos o raw_properties com a "verdade" do AIP
                    raw_properties: {
                        ...area,
                        aip_source: 'AIXM 5.1 ROBOT',
                        processed_at: new Date().toISOString()
                    }
                })
                .eq('ident', area.ident)
                .eq('is_current', true);

            if (updateError) console.warn(`⚠️ Erro ao atualizar ${area.ident}: ${updateError.message}`);
        }

        console.log('✅ [ROBOT] Sincronização concluída com sucesso!');

    } catch (error) {
        console.error('❌ [ROBOT] Erro fatal no pipeline:', error.message);
        process.exit(1);
    }
}

runSync();
