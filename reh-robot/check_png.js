const fs = require('fs');

// Extremely simple PNG parser to check alpha channel
const data = fs.readFileSync('vitoria_tile.png');

// Check if there is any IDAT chunk with non-zero alpha?
// Actually it's easier to just print the first few bytes.
console.log("Header:", data.slice(0, 16).toString('hex'));

// If DECEA returned a Geoserver Error (XML):
if (data.toString('utf8', 0, 5) === '<?xml') {
    console.log("IT IS XML!");
} else {
    console.log("IT IS PNG");
}
