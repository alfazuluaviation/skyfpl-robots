const https = require('https');
const fs = require('fs');
const { PNG } = require('pngjs');

const url = 'https://geoaisweb.decea.mil.br/geoserver/ICA/wms?SERVICE=WMS&VERSION=1.1.1&REQUEST=GetMap&LAYERS=ICA:CCV_REA_XR_VITORIA&STYLES=&SRS=EPSG:3857&BBOX=-4520336.852230467,-2309000.7208198756,-4481203.411649233,-2269867.280238641&WIDTH=512&HEIGHT=512&FORMAT=image/png&TRANSPARENT=TRUE';

https.get(url, (res) => {
    let data = [];
    res.on('data', chunk => data.push(chunk));
    res.on('end', () => {
        const buffer = Buffer.concat(data);
        fs.writeFileSync('vitoria_tile_ccv.png', buffer);
        console.log('Saved vitoria_tile_ccv.png, size:', buffer.length);
        
        try {
            const png = PNG.sync.read(buffer);
            let alpha0 = 0;
            for (let i = 3; i < png.data.length; i += 4) {
                if (png.data[i] === 0) alpha0++;
            }
            console.log('Total pixels:', png.width * png.height);
            console.log('Transparent pixels:', alpha0);
        } catch(e) {
            console.log("Not a valid PNG");
        }
    });
});
