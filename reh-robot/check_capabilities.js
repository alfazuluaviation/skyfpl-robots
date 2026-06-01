const https = require('https');
const fs = require('fs');

const url = 'https://geoaisweb.decea.mil.br/geoserver/ICA/wms?SERVICE=WMS&VERSION=1.1.1&REQUEST=GetCapabilities';

https.get(url, (res) => {
    let data = [];
    res.on('data', chunk => data.push(chunk));
    res.on('end', () => {
        const xml = Buffer.concat(data).toString();
        const lines = xml.split('\n');
        lines.forEach(line => {
            if (line.includes('REH') || line.includes('VITORIA')) {
                console.log(line.trim());
            }
        });
    });
});
