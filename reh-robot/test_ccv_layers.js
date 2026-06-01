const https = require('https');
const { PNG } = require('pngjs');

// Test the CCV_REH layers to confirm they have real content
const TESTS = [
    { layer: 'ICA:CCV_REH_WJ2_RIO_DE_JANEIRO', bbox: '-4988622,-2753239,-4648719,-2489670' },
    { layer: 'ICA:CCV_REH_WJ1_CABO_FRIO', bbox: '-4697614,-2428009,-4462143,-2245034' },
    { layer: 'ICA:CCV_REH_XP2_SAO_PAULO_1', bbox: '-5331828,-2805404,-4940562,-2544265' },
    { layer: 'ICA:CCV_REH_XP2_SAO_PAULO_2', bbox: '-5331828,-2805404,-4940562,-2544265' },
    { layer: 'ICA:CCV_REH_XP1_SAO_JOSE_DOS_CAMPOS', bbox: '-5109284,-2625454,-5050174,-2584040' },
    { layer: 'ICA:CCV_REH_XP1_SOROCABA', bbox: '-5265869,-2637143,-5202019,-2567700' },
    { layer: 'ICA:CCV_REH_XP2_CAMPINAS', bbox: '-5285742,-2682018,-5183660,-2621384' },
    { layer: 'ICA:CCV_REH_WH_BELO_HORIZONTE', bbox: '-4975063,-2302337,-4877074,-2244040' },
];

async function checkLayer(layer, bbox) {
    return new Promise((resolve) => {
        const url = `https://geoaisweb.decea.mil.br/geoserver/ICA/wms?SERVICE=WMS&VERSION=1.1.1&REQUEST=GetMap&LAYERS=${layer}&STYLES=&SRS=EPSG:3857&BBOX=${bbox}&WIDTH=512&HEIGHT=512&FORMAT=image/png&TRANSPARENT=TRUE`;
        https.get(url, (res) => {
            let data = [];
            res.on('data', chunk => data.push(chunk));
            res.on('end', () => {
                const buffer = Buffer.concat(data);
                try {
                    const png = PNG.sync.read(buffer);
                    let alpha0 = 0, whitePixels = 0;
                    const total = png.width * png.height;
                    for (let i = 0; i < png.data.length; i += 4) {
                        const a = png.data[i + 3];
                        const r = png.data[i], g = png.data[i + 1], b = png.data[i + 2];
                        if (a === 0) alpha0++;
                        if (r === 255 && g === 255 && b === 255) whitePixels++;
                    }
                    const validPixels = total - alpha0;
                    const status = validPixels > 100 ? '✅ HAS DATA' : '❌ EMPTY';
                    console.log(`${status} [${layer}] valid_pixels=${validPixels} (${(validPixels/total*100).toFixed(1)}%)`);
                } catch(e) {
                    console.log(`❌ INVALID PNG [${layer}] size=${buffer.length}`);
                }
                resolve();
            });
        }).on('error', (e) => {
            console.log(`❌ ERROR [${layer}]: ${e.message}`);
            resolve();
        });
    });
}

async function run() {
    for (const { layer, bbox } of TESTS) {
        await checkLayer(layer, bbox);
    }
}
run();
