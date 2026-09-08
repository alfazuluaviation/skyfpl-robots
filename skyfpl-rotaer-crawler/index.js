import { createClient } from '@supabase/supabase-js';
import { S3Client, PutObjectCommand, GetObjectCommand, DeleteObjectCommand, HeadObjectCommand } from '@aws-sdk/client-s3';
import * as dotenv from 'dotenv';
import { createRequire } from 'module';
import { readFile } from 'fs/promises';
import fs from 'fs/promises';
import path from 'path';
import { fileURLToPath } from 'url';

// Fix para Node.js 20: WebSocket nativo não suportado pelo @supabase/realtime-js
const require = createRequire(import.meta.url);
const ws = require('ws');

dotenv.config();

const __dirname = path.dirname(fileURLToPath(import.meta.url));

// ── Supabase ──────────────────────────────────────────────────────────────────
const supabase = createClient(
    process.env.SUPABASE_URL || '',
    process.env.SUPABASE_SERVICE_ROLE_KEY || '',
    { realtime: { transport: ws } }  // Fix Node.js 20
);

// ── Cloudflare R2 (via S3 API) ────────────────────────────────────────────────
const s3 = new S3Client({
    region: 'auto',
    endpoint: process.env.R2_ENDPOINT || '',
    credentials: {
        accessKeyId: process.env.R2_ACCESS_KEY_ID || '',
        secretAccessKey: process.env.R2_SECRET_ACCESS_KEY || ''
    }
});
const BUCKET_NAME = 'skyfpl-charts';

// ── Constantes ────────────────────────────────────────────────────────────────
const DELAY_MS = 800;               // 0.8 segundos entre chamadas ao DECEA
const DAYS_BEFORE_CYCLE = 14;       // Janela D-14 Oficial ICAO (Idêntico ao NavData)
const BATCH_SIZE = 50;              // Aeródromos por log de progresso
const CHECKPOINT_EVERY = 250;       // Salva checkpoint a cada N aeródromos processados

// ── Modo de Teste ─────────────────────────────────────────────────────────────
const FORCE_RUN = process.env.FORCE_RUN === 'true'; // Ignora a verificação de data AIRAC
const MAX_AERODROMES = parseInt(process.env.MAX_AERODROMES || '0', 10); // 0 = sem limite

async function sleep(ms) {
    return new Promise(resolve => setTimeout(resolve, ms));
}

// ── INTELIGÊNCIA AIRAC (DOUTRINA NAVDATA) ──────────────────────────────────────
// Lê o mesmo calendar.json que governa todo o ecossistema SkyFPL
function loadAiracCalendar() {
    const calendarPath = path.join(__dirname, 'calendar.json');
    const raw = require('fs').readFileSync(calendarPath, 'utf-8');
    return JSON.parse(raw);
}

function calculateAiracCycle(calendar, targetDate = null) {
    const now = targetDate ? new Date(targetDate) : new Date();
    const allCycles = [];

    for (const year of Object.keys(calendar)) {
        for (const [cycle, dateStr] of Object.entries(calendar[year])) {
            const [d, m, y] = dateStr.split('/');
            const effectiveDt = new Date(Date.UTC(Number(y), Number(m) - 1, Number(d), 0, 0, 0));
            const publicationDt = new Date(effectiveDt.getTime() - (14 * 24 * 60 * 60 * 1000));
            const expirationDt = new Date(effectiveDt.getTime() + (28 * 24 * 60 * 60 * 1000));
            allCycles.push({
                cycle,
                dateStr,
                effectiveDt,
                effective_date: effectiveDt.toISOString().slice(0, 10),
                publication_date: publicationDt.toISOString().slice(0, 10),
                expiration_date: expirationDt.toISOString().slice(0, 10)
            });
        }
    }

    allCycles.sort((a, b) => a.effectiveDt - b.effectiveDt);

    let currentCycle = null;
    let nextCycle = null;

    for (let i = 0; i < allCycles.length; i++) {
        if (allCycles[i].effectiveDt <= now) {
            currentCycle = allCycles[i];
            if (i + 1 < allCycles.length) {
                nextCycle = allCycles[i + 1];
            }
        }
    }

    if (!currentCycle && allCycles.length > 0) {
        currentCycle = allCycles[0];
    }

    let target = currentCycle;
    let daysUntilNext = null;

    if (nextCycle) {
        daysUntilNext = Math.floor((nextCycle.effectiveDt.getTime() - now.getTime()) / (1000 * 60 * 60 * 24));
        if (daysUntilNext >= 0 && daysUntilNext <= DAYS_BEFORE_CYCLE) {
            target = nextCycle;
            console.log(`🎯 Janela D-${daysUntilNext} Detectada! Alvo Selecionado: Ciclo Futuro ${nextCycle.cycle} (Vigência: ${nextCycle.dateStr})`);
        } else {
            console.log(`📌 Operação Normal: Alvo Selecionado: Ciclo Atual ${currentCycle.cycle} (Vigência: ${currentCycle.dateStr})`);
        }
    } else {
        console.log(`📌 Alvo Selecionado: ${target.cycle} (Vigência: ${target.dateStr})`);
    }

    const todayUtc = new Date(Date.UTC(now.getUTCFullYear(), now.getUTCMonth(), now.getUTCDate(), 0, 0, 0));
    const isStaging = target.effectiveDt.getTime() > todayUtc.getTime();

    return {
        cycle: target.cycle,
        dateStr: target.dateStr,
        effectiveDt: target.effectiveDt,
        effective_date: target.effective_date,
        publication_date: target.publication_date,
        expiration_date: target.expiration_date,
        current_cycle: currentCycle,
        next_cycle: nextCycle,
        days_until_next: daysUntilNext,
        is_staging: isStaging
    };
}

async function isCycleAlreadyPublished(cycle) {
    const versionedKey = `rotaer/cycles/${cycle}/rotaer_${cycle}_snapshot.json`;
    try {
        await s3.send(new HeadObjectCommand({ Bucket: BUCKET_NAME, Key: versionedKey }));
        return true;
    } catch (e) {
        return false;
    }
}

// ── SISTEMA DE CHECKPOINT ─────────────────────────────────────────────────────
// Guarda progresso no R2 para retomada automática em caso de interrupção

function getCheckpointKey(cycle) {
    return `rotaer/rotaer_${cycle}_checkpoint.json`;
}

async function loadCheckpointFromR2(cycle) {
    const key = getCheckpointKey(cycle);
    try {
        const response = await s3.send(new GetObjectCommand({ Bucket: BUCKET_NAME, Key: key }));
        const body = await response.Body.transformToString();
        const checkpoint = JSON.parse(body);
        console.log(`♻️  Checkpoint encontrado! Retomando a partir do aeródromo ${checkpoint.last_index + 1} (${checkpoint.last_icao})`);
        console.log(`   Dados já coletados: ${Object.keys(checkpoint.data).length} aeródromos`);
        return checkpoint;
    } catch (e) {
        // NoSuchKey = sem checkpoint anterior, começa do zero
        if (e.name === 'NoSuchKey' || e.$metadata?.httpStatusCode === 404) {
            console.log('🆕 Nenhum checkpoint encontrado. Iniciando do zero.');
            return null;
        }
        // Outro erro — loga mas não bloqueia
        console.warn(`⚠️  Erro ao carregar checkpoint: ${e.message}. Iniciando do zero.`);
        return null;
    }
}

async function saveCheckpointToR2(cycle, lastIndex, lastIcao, data) {
    const key = getCheckpointKey(cycle);
    const checkpoint = JSON.stringify({
        airac_cycle: cycle,
        last_index: lastIndex,
        last_icao: lastIcao,
        saved_at: new Date().toISOString(),
        total_collected: Object.keys(data).length,
        data
    });
    try {
        await s3.send(new PutObjectCommand({
            Bucket: BUCKET_NAME,
            Key: key,
            Body: checkpoint,
            ContentType: 'application/json',
            CacheControl: 'no-cache, no-store'  // Nunca cachear checkpoint
        }));
        console.log(`💾 Checkpoint salvo: ${Object.keys(data).length} aeródromos coletados até ${lastIcao}`);
    } catch (e) {
        // Falha ao salvar checkpoint não deve derrubar o crawl — apenas avisa
        console.warn(`⚠️  Falha ao salvar checkpoint (continuando): ${e.message}`);
    }
}

async function deleteCheckpointFromR2(cycle) {
    const key = getCheckpointKey(cycle);
    try {
        await s3.send(new DeleteObjectCommand({ Bucket: BUCKET_NAME, Key: key }));
        console.log(`🗑️  Checkpoint deletado (processamento completo).`);
    } catch (e) {
        console.warn(`⚠️  Falha ao deletar checkpoint: ${e.message}`);
    }
}

// ── MOTOR PRINCIPAL ───────────────────────────────────────────────────────────
async function startCrawler() {
    console.log('═══════════════════════════════════════════════════════════');
    console.log('🤖  SkyFPL ROTAER Crawler — Iniciando');
    console.log(`🕐  ${new Date().toISOString()}`);
    console.log('═══════════════════════════════════════════════════════════');

    // 1. Determinação do Ciclo AIRAC Oficial (Doutrina NavData D-14)
    const calendar = loadAiracCalendar();
    const airac = calculateAiracCycle(calendar);

    console.log(`\n🛫 Ciclo Vigente : ${airac.current_cycle?.cycle || 'N/A'} (${airac.current_cycle?.dateStr || 'N/A'})`);
    console.log(`🎯 Ciclo Alvo    : ${airac.cycle} (${airac.dateStr}) [${airac.is_staging ? 'STANDBY / STAGING D-14' : 'PRODUÇÃO ATIVA'}]`);

    // 2. ♻️ Verificar e carregar checkpoint ANTES da trava de idempotência
    console.log('\n🔍 Verificando checkpoint de execução anterior (para garantir retomada)...');
    const checkpoint = await loadCheckpointFromR2(airac.cycle);

    if (FORCE_RUN) {
        console.log(`\n🧪 MODO DE TESTE ATIVADO (FORCE_RUN=true)`);
        console.log(`   Verificação de data AIRAC e idempotência ignoradas.`);
        if (MAX_AERODROMES > 0) {
            console.log(`   Limite de processamento: ${MAX_AERODROMES} aeródromo(s).`);
        }
    } else if (checkpoint) {
        console.log(`\n🚨 TRABALHO INCOMPLETO DETECTADO! Retomando processamento pendente do Ciclo ${airac.cycle}.`);
    } else if (await isCycleAlreadyPublished(airac.cycle)) {
        console.log(`\n🛡️  TRAVA DE IDEMPOTÊNCIA ATIVA (PADRÃO NAVDATA):`);
        console.log(`   O Ciclo AIRAC ${airac.cycle} já está consolidado e disponível em rotaer/cycles/${airac.cycle}/rotaer_${airac.cycle}_snapshot.json.`);
        console.log(`   Nenhuma extração necessária hoje. (Para forçar um reprocessamento, utilize FORCE_RUN=true).`);
        process.exit(0);
    } else {
        console.log(`\n🚨 PROCESSAMENTO AUTORIZADO:`);
        console.log(`   Ciclo ${airac.cycle} (Vigência: ${airac.dateStr}) em processamento...\n`);
    }

    // 3. Buscar malha aérea: Prioriza NavData do ciclo em Staging, com fallback para produção
    console.log(`📡 Buscando malha aérea de referência para o Ciclo ${airac.cycle}...`);
    let targets = [];
    let navdataUrl = `https://pub-1b4a512269cb4fc496e8badb21acf51c.r2.dev/navdata/cycles/${airac.cycle}/navdata_${airac.cycle}.json`;

    try {
        let response = await fetch(navdataUrl);
        if (response.ok) {
            console.log(`🛰️  NavData oficial do Ciclo ${airac.cycle} localizado no R2 Staging!`);
        } else {
            navdataUrl = 'https://pub-1b4a512269cb4fc496e8badb21acf51c.r2.dev/latest_navdata.json';
            console.log(`📌 NavData do Ciclo ${airac.cycle} ainda não publicado em staging. Utilizando malha ativa em produção (${navdataUrl}).`);
            response = await fetch(navdataUrl);
        }
        if (!response.ok) throw new Error(`HTTP ${response.status}`);
        const navdata = await response.json();
        
        targets = (navdata.data || [])
            .filter(a => (a.type === 'airport' || a.type === 'heliport') && a.icao)
            .map(a => ({
                icao: a.icao,
                ciad: a.props?.ciad || '',
                name: a.name || a.props?.nome || '',
                type: a.type
            }))
            .sort((a, b) => a.icao.localeCompare(b.icao));
            
        console.log(`✅ ${targets.length} aeródromos válidos filtrados a partir de ${navdataUrl}.`);
    } catch (error) {
        console.error(`❌ Erro ao baixar malha aérea: ${error.message}`);
        process.exit(1);
    }

    // Aplica limite de teste se MAX_AERODROMES estiver definido
    if (MAX_AERODROMES > 0 && targets.length > MAX_AERODROMES) {
        console.log(`🧪 Modo teste: limitando de ${targets.length} para ${MAX_AERODROMES} aeródromo(s)`);
        targets = targets.slice(0, MAX_AERODROMES);
    }

    // 4. Pré-carrega dados do checkpoint ou inicia vazio
    const results = checkpoint ? { ...checkpoint.data } : {};
    const startIndex = checkpoint ? checkpoint.last_index + 1 : 0;
    let successCount = Object.keys(results).length; // já processados
    let failCount = 0;

    const remainingTargets = targets.slice(startIndex);
    const totalTargets = targets.length;

    if (startIndex > 0) {
        console.log(`\n▶️  Retomando do aeródromo ${startIndex + 1}/${totalTargets}`);
        console.log(`   (${successCount} aeródromos já coletados na sessão anterior)\n`);
    } else {
        console.log(`✈️  Total de alvos a processar: ${totalTargets}`);
        console.log(`⏱️  Tempo estimado: ~${Math.round((totalTargets * DELAY_MS) / 1000)} segundos\n`);
    }

    // 4. Loop de raspagem (retoma do ponto certo)
    const startTime = Date.now();

    for (const [relIndex, aero] of remainingTargets.entries()) {
        const absIndex = startIndex + relIndex;  // índice absoluto na lista completa
        const icao = aero.icao;
        const progress = `[${absIndex + 1}/${totalTargets}]`;

        // Log de progresso a cada BATCH_SIZE aeródromos
        if (relIndex % BATCH_SIZE === 0 && relIndex > 0) {
            const elapsed = ((Date.now() - startTime) / 60000).toFixed(1);
            const sessionSuccess = Object.keys(results).length - (checkpoint ? Object.keys(checkpoint.data).length : 0);
            const rate = (sessionSuccess / relIndex * 100).toFixed(0);
            console.log(`\n📊 Progresso: ${progress} | ✅ ${successCount} total | ❌ ${failCount} | ⏱️ ${elapsed} min | Taxa sessão: ${rate}%\n`);
        }

        process.stdout.write(`  ${progress} ${icao.padEnd(6)} → `);

        try {
            const { data, error: fnError } = await supabase.functions.invoke('fetch-rotaer', {
                method: 'POST',
                body: { icaoCode: icao }
            });

            if (fnError) throw new Error(fnError.message || JSON.stringify(fnError));
            if (!data?.success || !data?.data) throw new Error('Resposta sem dados válidos');

            results[icao] = {
                ...data.data,
                _airac_cycle: airac.cycle,
                _crawled_at: new Date().toISOString()
            };
            successCount++;
            process.stdout.write(`✅ OK\n`);

        } catch (e) {
            process.stdout.write(`❌ FALHA (${e.message.substring(0, 60)})\n`);
            failCount++;
        }

        // 💾 Salva checkpoint a cada CHECKPOINT_EVERY aeródromos processados nesta sessão
        if ((relIndex + 1) % CHECKPOINT_EVERY === 0) {
            await saveCheckpointToR2(airac.cycle, absIndex, icao, results);
        }

        // Sleep entre requisições (proteção de rate limit do DECEA)
        if (relIndex < remainingTargets.length - 1) {
            await sleep(DELAY_MS);
        }
    }

    // 5. Relatório final de raspagem
    const totalTime = ((Date.now() - startTime) / 60000).toFixed(1);
    console.log('\n═══════════════════════════════════════════════════════════');
    console.log('📊 RELATÓRIO FINAL DA RASPAGEM');
    console.log('═══════════════════════════════════════════════════════════');
    console.log(`   ✅ Total coletados : ${Object.keys(results).length} aeródromos`);
    console.log(`   ✅ Nesta sessão    : ${successCount - (checkpoint ? Object.keys(checkpoint.data).length : 0)}`);
    console.log(`   ❌ Falhas          : ${failCount}`);
    console.log(`   ⏱️  Tempo desta sessão: ${totalTime} minutos`);
    console.log(`   📦 Ciclo           : ${airac.cycle} (efetivo em ${airac.dateStr})`);

    if (Object.keys(results).length === 0) {
        console.error('\n❌ Nenhum dado foi coletado. Abortando upload para evitar sobrescrever dados bons.');
        process.exit(1);
    }

    // 6. Montar e publicar o Snapshot Offline no Cloudflare R2
    console.log('\n☁️  Publicando snapshot no Cloudflare R2...');
    const snapshot = JSON.stringify({
        _meta: {
            generated_at: new Date().toISOString(),
            airac_cycle: airac.cycle,
            airac_effective_date: airac.dateStr,
            total_success: Object.keys(results).length,
            total_fail: failCount,
            version: '2.0.0'
        },
        data: results
    });

    const R2_KEY = `rotaer/rotaer_${airac.cycle}_snapshot.json`;
    const R2_KEY_VERSIONED = `rotaer/cycles/${airac.cycle}/rotaer_${airac.cycle}_snapshot.json`;
    const R2_KEY_LATEST = 'rotaer/rotaer_snapshot_latest.json';

    try {
        // Upload da versão AIRAC específica
        await s3.send(new PutObjectCommand({
            Bucket: BUCKET_NAME,
            Key: R2_KEY,
            Body: snapshot,
            ContentType: 'application/json',
            CacheControl: 'public, max-age=2419200'
        }));
        console.log(`   ✅ Publicado: ${R2_KEY}`);

        // Upload da versão de Staging/Quarentena
        await s3.send(new PutObjectCommand({
            Bucket: BUCKET_NAME,
            Key: R2_KEY_VERSIONED,
            Body: snapshot,
            ContentType: 'application/json',
            CacheControl: 'public, max-age=2419200'
        }));
        console.log(`   ✅ Publicado (Staging / Quarentena): ${R2_KEY_VERSIONED}`);

        // 🛡️ BLINDAGEM DE PRODUÇÃO (DOUTRINA NAVDATA)
        if (airac.is_staging) {
            console.log(`\n🛡️  TRAVA DE SEGURANÇA ATIVA (PADRÃO NAVDATA):`);
            console.log(`   O Ciclo ${airac.cycle} é FUTURO (Vigência em ${airac.dateStr}).`);
            console.log(`   O snapshot foi gravado com sucesso na QUARENTENA DE STAGING.`);
            console.log(`   🛑 O arquivo 'rotaer_snapshot_latest.json' segue 100% PROTEGIDO em produção.`);
            console.log(`   A promoção ocorrerá na data oficial de vigência via Dashboard / Edge Function.`);
        } else {
            // Se for ciclo já vigente hoje (ex: reprocessamento corretivo), atualiza produção
            await s3.send(new PutObjectCommand({
                Bucket: BUCKET_NAME,
                Key: R2_KEY_LATEST,
                Body: snapshot,
                ContentType: 'application/json',
                CacheControl: 'public, max-age=86400'
            }));
            console.log(`   🚀 Produção Atualizada: ${R2_KEY_LATEST}`);
        }

        // Salva Telemetria Estruturada com Metadados Oficiais AIRAC
        const telemetry = JSON.stringify({
            status: 'completed',
            airac_cycle: airac.cycle,
            airac_effective_date: airac.dateStr,
            publication_date: airac.publication_date,
            expiration_date: airac.expiration_date,
            total_collected: Object.keys(results).length,
            failures: failCount,
            is_staging: airac.is_staging,
            versioned_path: R2_KEY_VERSIONED,
            updated_at: new Date().toISOString()
        }, null, 2);

        await s3.send(new PutObjectCommand({
            Bucket: BUCKET_NAME,
            Key: 'rotaer/telemetry.json',
            Body: telemetry,
            ContentType: 'application/json',
            CacheControl: 'no-cache, no-store, must-revalidate'
        }));
        console.log(`   📡 Telemetria R2 atualizada (is_staging=${airac.is_staging}).`);

        // 🗑️ Deleta checkpoint — processamento 100% completo
        await deleteCheckpointFromR2(airac.cycle);

    } catch (e) {
        console.error(`   ❌ Erro no upload para R2: ${e.message}`);
        process.exit(1);
    }

    console.log('\n🏁 Crawler finalizado com sucesso!');
    console.log(`   URL em Produção : https://cartas.skyfpl.com/${R2_KEY_LATEST}`);
    console.log(`   URL em Staging   : https://cartas.skyfpl.com/${R2_KEY_VERSIONED}`);
    console.log('═══════════════════════════════════════════════════════════\n');

    // Sinaliza sucesso total para o workflow — sem checkpoint pendente
    process.exit(0);
}

startCrawler().catch(err => {
    console.error('\n💥 CRASH FATAL:', err);
    process.exit(1);
});
