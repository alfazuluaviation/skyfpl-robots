const https = require('https');
const { PNG } = require('pngjs');

const layers = [
    'ICA:CV_REH_WJ2_RIO_DE_JANEIRO',
    'ICA:CV_REH_XP_SAO_PAULO',
    'ICA:CV_REH_XR_VITORIA',
    'ICA:CV_REH_BR_COMPLETO',
    'ICA:CCV_REH_WH_BELO_HORIZONTE'
];

const BBOXES = {
    'ICA:CV_REH_WJ2_RIO_DE_JANEIRO': '-4988622,-2753239,-4648719,-2489670', // rough mercator
    'ICA:CV_REH_XP_SAO_PAULO': '-5331828,-2805404,-4940562,-2544265',
    'ICA:CV_REH_XR_VITORIA': '-4526978,-2342205,-4443423,-2249053',
    'ICA:CV_REH_BR_COMPLETO': '-7823871,-3599908,-3896182,33411',
    'ICA:CCV_REH_WH_BELO_HORIZONTE': '-4897974,-2278065,-4897974,-2278065' // just testing
};

async function checkLayer(layer) {
    return new Promise((resolve) => {
        let bbox = BBOXES[layer] || '-7823871,-3599908,-3896182,33411';
        let url = `https://geoaisweb.decea.mil.br/geoserver/ICA/wms?SERVICE=WMS&VERSION=1.1.1&REQUEST=GetMap&LAYERS=${layer}&STYLES=&SRS=EPSG:3857&BBOX=${bbox}&WIDTH=512&HEIGHT=512&FORMAT=image/png&TRANSPARENT=TRUE`;
        
        https.get(url, (res) => {
            let data = [];
            res.on('data', chunk => data.push(chunk));
            res.on('end', () => {
                const buffer = Buffer.concat(data);
                try {
                    const png = PNG.sync.read(buffer);
                    let alpha0 = 0;
                    for (let i = 3; i < png.data.length; i += 4) {
                        if (png.data[i] === 0) alpha0++;
                    }
                    console.log(`[${layer}] Total: ${png.width*png.height}, Transparent: ${alpha0}`);
                } catch(e) {
                    console.log(`[${layer}] Failed to parse PNG. Size: ${buffer.length}`);
                }
                resolve();
            });
        });
    });
}

async function run() {
    for (const layer of layers) {
        await checkLayer(layer);
    }
}
run();
